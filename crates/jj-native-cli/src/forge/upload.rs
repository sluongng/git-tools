//! Bounded topological native closure transfer through authorized upload sessions.
use super::{context::Context, rpc};
use futures::AsyncReadExt as _;
use jj_cli::command_error::{CommandError, user_error};
use jj_native_backend::NativeObjectIo;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use tinytree_client::{NativeClient, forge_protocol as f};
use tinytree_model as m;
const MAX_OBJECTS: usize = 5000;
const MAX_METADATA: usize = 16 * 1024 * 1024;
const MAX_FILE: u64 = 64 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    kind: u8,
    id: [u8; 32],
    size_bytes: u64,
    format_version: u16,
    staging_lease: Value,
    expires_at: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    session_id: [u8; 32],
    offset: u64,
    state: String,
    receipt: Option<Receipt>,
}
fn failure(status: tonic::Status, key: &str) -> CommandError {
    user_error(rpc::CallError {
        status,
        request_id: Some(key.into()),
    })
}
async fn call(
    client: &NativeClient,
    context: &Context,
    key: &str,
    body: Value,
) -> Result<Session, CommandError> {
    let reply = client
        .forge_call(f::ForgeCallRequest {
            operation: f::ForgeOperation::WriteObject as i32,
            account_id: context.account_id.clone(),
            view: context.view.clone(),
            request_id: key.into(),
            payload_json: serde_json::to_vec(&body).map_err(user_error)?,
        })
        .await
        .map_err(|s| failure(s, key))?;
    serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("invalid forge upload session result"))
}
fn receipt(value: &Session, expected: m::Descriptor) -> Result<(), CommandError> {
    let Some(receipt) = &value.receipt else {
        return Err(user_error("committed upload has no retention receipt"));
    };
    if value.state != "committed"
        || value.offset != expected.size_bytes
        || receipt.kind != expected.key.kind as u8
        || receipt.id != expected.key.id.0
        || receipt.size_bytes != expected.size_bytes
        || receipt.format_version != expected.format_version
    {
        return Err(user_error("forge upload receipt identity mismatch"));
    }
    let _ = (&receipt.staging_lease, &receipt.expires_at);
    Ok(())
}
async fn send(
    client: &NativeClient,
    context: &Context,
    version: [u8; 32],
    root_key: &str,
    path: &str,
    descriptor: m::Descriptor,
    input: &mut (dyn futures::AsyncRead + Send + Unpin),
) -> Result<(), CommandError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-forge-upload-v1\0");
    hash.update(root_key.as_bytes());
    hash.update(&[descriptor.key.kind as u8]);
    hash.update(&descriptor.key.id.0);
    hash.update(path.as_bytes());
    let key = format!("upload-{}", hash.finalize().to_hex());
    let session=call(client,context,&key,json!({"action":"begin","version":version,"path":path,"kind":descriptor.key.kind as u8,"id":descriptor.key.id.0,"size_bytes":descriptor.size_bytes})).await.map_err(|e|{
        if let Some(call)=e.error.downcast_ref::<rpc::CallError>(){failure(call.status.clone(),root_key)}else{e}
    })?;
    if session.state == "committed" {
        return receipt(&session, descriptor);
    }
    if session.state != "pending" || session.offset > descriptor.size_bytes {
        return Err(user_error("invalid forge upload offset/state"));
    }
    let id = session.session_id;
    let mut offset = 0u64;
    let mut buffer = vec![0u8; 65536];
    // The retained immutable source is read from byte zero even after a resume;
    // no mutable filesystem path or newer working-copy version supplies bytes.
    while offset < descriptor.size_bytes {
        let wanted = (descriptor.size_bytes - offset).min(buffer.len() as u64) as usize;
        input
            .read_exact(&mut buffer[..wanted])
            .await
            .map_err(user_error)?;
        let end = offset + wanted as u64;
        if end > session.offset {
            let start = session.offset.saturating_sub(offset) as usize;
            let value=call(client,context,&key,json!({"action":"append","session_id":id,"offset":offset+start as u64,"data":&buffer[start..wanted]})).await.map_err(|e|{
                if let Some(call)=e.error.downcast_ref::<rpc::CallError>(){failure(call.status.clone(),root_key)}else{e}
            })?;
            if value.session_id != id || value.offset != end || value.state != "pending" {
                return Err(user_error("forge upload acknowledgement range mismatch"));
            }
        }
        offset = end;
    }
    let mut extra = [0u8; 1];
    if input.read(&mut extra).await.map_err(user_error)? != 0 {
        return Err(user_error("native source exceeds its immutable descriptor"));
    }
    let value = call(
        client,
        context,
        &key,
        json!({"action":"finalize","session_id":id}),
    )
    .await
    .map_err(|e| {
        if let Some(call) = e.error.downcast_ref::<rpc::CallError>() {
            failure(call.status.clone(), root_key)
        } else {
            e
        }
    })?;
    if value.session_id != id {
        return Err(user_error("forge upload final session identity mismatch"));
    }
    receipt(&value, descriptor)
}
enum Task {
    Visit(m::ObjectKey, String),
    Emit(m::ObjectKey, String),
}
async fn baseline(
    io: &Arc<dyn NativeObjectIo>,
    base: [u8; 32],
) -> Result<(BTreeSet<(u8, [u8; 32], String)>, BTreeSet<[u8; 32]>), CommandError> {
    let commit = io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(base),
        })
        .await
        .map_err(user_error)?
        .into_object();
    let m::Object::Commit(commit) = commit else {
        return Err(user_error("forge baseline is not a native commit"));
    };
    let mut tasks = commit
        .root_tree
        .terms
        .into_iter()
        .map(|id| (id, String::new()))
        .collect::<Vec<_>>();
    let mut known = BTreeSet::new();
    let mut copies = BTreeSet::new();
    let mut bytes = 0usize;
    let mut paths = 0usize;
    while let Some((id, path)) = tasks.pop() {
        if !known.insert((m::Kind::Tree as u8, id.0, path.clone())) {
            continue;
        }
        let metadata = io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            })
            .await
            .map_err(user_error)?;
        bytes = bytes
            .checked_add(metadata.bytes().len())
            .ok_or_else(|| user_error("baseline metadata budget overflow"))?;
        let m::Object::Tree(tree) = metadata.into_object() else {
            return Err(user_error("forge baseline tree kind mismatch"));
        };
        for entry in tree.entries {
            let child = if path.is_empty() {
                entry.name
            } else {
                format!("{path}/{}", entry.name)
            };
            paths += child.len();
            match entry.value {
                m::TreeValue::Tree(id) => tasks.push((id, child)),
                m::TreeValue::File { id, copy_id, .. } => {
                    known.insert((m::Kind::File as u8, id.0, child));
                    if let Some(id) = copy_id {
                        copies.insert(id.0);
                    }
                }
                m::TreeValue::Symlink { id, .. } => {
                    known.insert((m::Kind::Symlink as u8, id.0, child));
                }
            }
        }
        if known.len() + tasks.len() > MAX_OBJECTS
            || bytes > MAX_METADATA
            || paths > 2 * 1024 * 1024
        {
            return Err(user_error("forge baseline metadata/path budget exceeded"));
        }
    }
    Ok((known, copies))
}
pub(super) async fn closure(
    context: &Context,
    version: [u8; 32],
    base: [u8; 32],
    head: [u8; 32],
    key: &str,
    io: Arc<dyn NativeObjectIo>,
) -> Result<(), CommandError> {
    let config = context.client_config().map_err(user_error)?;
    let deadline = tokio::time::Instant::now() + config.rpc.operation_timeout();
    let client = NativeClient::connect(config)
        .await
        .map_err(|s| failure(s, key))?
        .with_operation_deadline(deadline);
    let mut tasks = vec![Task::Visit(
        m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head),
        },
        String::new(),
    )];
    let (mut complete, baseline_copies) = baseline(&io, base).await?;
    let mut visiting = BTreeSet::new();
    let mut new_objects = 0usize;
    let mut path_bytes = 0usize;
    let mut bytes = 0usize;
    let mut edges = 0usize;
    while let Some(task) = tasks.pop() {
        if tokio::time::Instant::now() >= deadline {
            return Err(failure(
                tonic::Status::deadline_exceeded(
                    "source closure upload deadline exceeded; resume the exact saved request",
                ),
                key,
            ));
        }
        match task {
            Task::Visit(object, path) => {
                if object.kind == m::Kind::Commit
                    && (object.id.0 == base || object.id.0 == m::CommitId::ROOT.0)
                {
                    continue;
                }
                let identity = (object.kind as u8, object.id.0, path.clone());
                if object.kind == m::Kind::CopyHistory && baseline_copies.contains(&object.id.0) {
                    continue;
                }
                if complete.contains(&identity) {
                    continue;
                }
                new_objects += 1;
                path_bytes += path.len();
                if new_objects >= MAX_OBJECTS
                    || path_bytes > 2 * 1024 * 1024
                    || path.len() > 4096
                    || !visiting.insert(identity)
                {
                    return Err(user_error(
                        "native upload closure exceeds object/path bounds or contains a cycle",
                    ));
                }
                tasks.push(Task::Emit(object, path.clone()));
                if object.kind == m::Kind::File {
                    continue;
                }
                let metadata = io.read_metadata(object).await.map_err(user_error)?;
                bytes = bytes
                    .checked_add(metadata.bytes().len())
                    .ok_or_else(|| user_error("native upload metadata budget overflow"))?;
                if bytes > MAX_METADATA || metadata.bytes().len() > 1024 * 1024 {
                    return Err(user_error("native upload metadata limit exceeded"));
                }
                let value = metadata.into_object();
                let mut children = Vec::new();
                match value {
                    m::Object::Tree(tree) => {
                        for entry in tree.entries {
                            let child = if path.is_empty() {
                                entry.name
                            } else {
                                format!("{path}/{}", entry.name)
                            };
                            match entry.value {
                                m::TreeValue::File { id, copy_id, .. } => {
                                    children.push((
                                        m::ObjectKey {
                                            kind: m::Kind::File,
                                            id: m::ObjectId(id.0),
                                        },
                                        child.clone(),
                                    ));
                                    if let Some(copy) = copy_id {
                                        children.push((
                                            m::ObjectKey {
                                                kind: m::Kind::CopyHistory,
                                                id: m::ObjectId(copy.0),
                                            },
                                            child,
                                        ));
                                    }
                                }
                                m::TreeValue::Tree(id) => children.push((
                                    m::ObjectKey {
                                        kind: m::Kind::Tree,
                                        id: m::ObjectId(id.0),
                                    },
                                    child,
                                )),
                                m::TreeValue::Symlink { id, .. } => children.push((
                                    m::ObjectKey {
                                        kind: m::Kind::Symlink,
                                        id: m::ObjectId(id.0),
                                    },
                                    child,
                                )),
                            }
                        }
                    }
                    m::Object::CopyHistory(copy) => {
                        for parent in copy.parents {
                            children.push((
                                m::ObjectKey {
                                    kind: m::Kind::CopyHistory,
                                    id: m::ObjectId(parent.0),
                                },
                                copy.current_path.clone(),
                            ));
                        }
                    }
                    other => {
                        for child in other.references() {
                            children.push((child, String::new()));
                        }
                    }
                }
                edges += children.len();
                if edges > 50000 {
                    return Err(user_error("native upload edge bound exceeded"));
                }
                for (child, path) in children.into_iter().rev() {
                    tasks.push(Task::Visit(child, path));
                }
            }
            Task::Emit(object, mut path) => {
                let identity = (object.kind as u8, object.id.0, path.clone());
                if object.kind == m::Kind::File {
                    let expected = io
                        .descriptor(object)
                        .await
                        .map_err(user_error)?
                        .ok_or_else(|| user_error("source file descriptor is missing"))?;
                    if expected.size_bytes > MAX_FILE {
                        return Err(user_error("native upload file exceeds 64 MiB"));
                    }
                    let file = io
                        .read_file(m::FileId(object.id.0))
                        .await
                        .map_err(user_error)?;
                    let (descriptor, mut source) = file.into_parts();
                    if descriptor != expected || descriptor.size_bytes > MAX_FILE {
                        return Err(user_error("native upload file exceeds 64 MiB"));
                    }
                    send(
                        &client,
                        context,
                        version,
                        key,
                        &path,
                        descriptor,
                        &mut source,
                    )
                    .await?;
                } else {
                    let metadata = io.read_metadata(object).await.map_err(user_error)?;
                    let descriptor = metadata.descriptor();
                    let data = metadata.bytes().to_vec();
                    if let m::Object::CopyHistory(copy) = metadata.into_object() {
                        path = copy.current_path;
                    }
                    send(
                        &client,
                        context,
                        version,
                        key,
                        &path,
                        descriptor,
                        &mut futures::io::Cursor::new(data),
                    )
                    .await?;
                }
                visiting.remove(&identity);
                complete.insert(identity);
            }
        }
    }
    Ok(())
}

async fn files(
    io: &Arc<dyn NativeObjectIo>,
    head: [u8; 32],
) -> Result<std::collections::BTreeMap<String, (m::FileId, Option<m::CopyId>)>, CommandError> {
    let commit = io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head),
        })
        .await
        .map_err(user_error)?
        .into_object();
    let m::Object::Commit(commit) = commit else {
        return Err(user_error("native lineage head kind mismatch"));
    };
    if commit.root_tree.terms.len() != 1 {
        return Err(user_error("lineage requires a resolved source tree"));
    }
    let mut tasks = vec![(commit.root_tree.terms[0], String::new())];
    let mut result = std::collections::BTreeMap::new();
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    while let Some((id, path)) = tasks.pop() {
        nodes += 1;
        if nodes + result.len() > MAX_OBJECTS {
            return Err(user_error("native lineage tree bound exceeded"));
        }
        let metadata = io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            })
            .await
            .map_err(user_error)?;
        bytes += metadata.bytes().len();
        if bytes > MAX_METADATA {
            return Err(user_error("native lineage metadata bound exceeded"));
        }
        let m::Object::Tree(tree) = metadata.into_object() else {
            return Err(user_error("native lineage tree kind mismatch"));
        };
        for entry in tree.entries {
            let child = if path.is_empty() {
                entry.name
            } else {
                format!("{path}/{}", entry.name)
            };
            match entry.value {
                m::TreeValue::Tree(id) => tasks.push((id, child)),
                m::TreeValue::File { id, copy_id, .. } => {
                    result.insert(child, (id, copy_id));
                }
                _ => {}
            }
        }
    }
    Ok(result)
}
pub(super) async fn lineage(
    io: &Arc<dyn NativeObjectIo>,
    base: [u8; 32],
    head: [u8; 32],
) -> Result<Vec<Value>, CommandError> {
    let before = files(io, base).await?;
    let after = files(io, head).await?;
    let origins = before
        .iter()
        .filter_map(|(path, (file, copy))| copy.map(|id| (id, (path, file))))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut result = Vec::new();
    for (path, (_file, copy)) in &after {
        let Some(copy) = copy else {
            continue;
        };
        if origins.contains_key(copy) {
            continue;
        }
        let value = io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::CopyHistory,
                id: m::ObjectId(copy.0),
            })
            .await
            .map_err(user_error)?
            .into_object();
        let m::Object::CopyHistory(copy) = value else {
            return Err(user_error("native lineage record kind mismatch"));
        };
        if copy.parents.is_empty() {
            continue;
        }
        if copy.current_path != *path || copy.parents.len() != 1 {
            return Err(user_error(
                "native copy lineage requires one exact source parent",
            ));
        }
        let Some((source, _original)) = origins.get(&copy.parents[0]) else {
            return Err(user_error(
                "native copy source is not in the frozen projected baseline",
            ));
        };
        result.push(json!({"operation":if after.contains_key(*source){"copy"}else{"rename"},"source_path":source,"target_path":path}));
    }
    Ok(result)
}
