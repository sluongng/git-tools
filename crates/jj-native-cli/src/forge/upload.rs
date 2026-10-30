//! Bounded topological native closure transfer through authorized upload sessions.
use super::{context::Context, rpc};
use futures::AsyncReadExt as _;
use jj_cli::command_error::{CommandError, user_error};
use jj_native_backend::NativeObjectIo;
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};
use tinytree_client::{NativeClient, forge_protocol as f};
use tinytree_forge_core::upload::{ForgeUploadState, ForgeUploadStatus as Session};
use tinytree_model as m;
const MAX_OBJECTS: usize = 5000;
const MAX_METADATA: usize = 16 * 1024 * 1024;
const MAX_FILE: u64 = 64 * 1024 * 1024;
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
    if value.state != ForgeUploadState::Committed
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
    if session.state == ForgeUploadState::Committed {
        return receipt(&session, descriptor);
    }
    if session.state != ForgeUploadState::Pending
        || session.offset > descriptor.size_bytes
        || session.receipt.is_some()
    {
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
            if value.session_id != id
                || value.offset != end
                || value.state != ForgeUploadState::Pending
            {
                return Err(user_error("forge upload acknowledgement range mismatch"));
            }
        }
        offset = end;
    }
    let mut extra = [0u8; 1];
    if input.read(&mut extra).await.map_err(user_error)? != 0 {
        return Err(user_error("native source exceeds its immutable descriptor"));
    }
    // A persisted verification quantum may finish receiving bytes before its
    // scoped proof is ready. Retry only this exact Finalize/session/key; retain
    // any narrower whole-transfer deadline already carried by the SDK client.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    let client = client.clone().with_operation_deadline(deadline);
    let finalize = json!({"action":"finalize","session_id":id});
    for round in 0..512 {
        if round != 0 {
            tokio::time::timeout_at(
                deadline,
                tokio::time::sleep(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(|_| {
                failure(
                    tonic::Status::deadline_exceeded(
                        "upload verification deadline; resume the original request",
                    ),
                    root_key,
                )
            })?;
        }
        let value = call(&client, context, &key, finalize.clone())
            .await
            .map_err(|e| {
                if let Some(call) = e.error.downcast_ref::<rpc::CallError>() {
                    failure(call.status.clone(), root_key)
                } else {
                    e
                }
            })?;
        if value.session_id != id || value.offset != descriptor.size_bytes {
            return Err(user_error(
                "forge upload final session identity/offset mismatch",
            ));
        }
        match &value.state {
            ForgeUploadState::Committed => return receipt(&value, descriptor),
            ForgeUploadState::Pending if value.receipt.is_none() => {}
            _ => {
                return Err(user_error(
                    "invalid forge upload final verification state/receipt",
                ));
            }
        }
    }
    Err(failure(
        tonic::Status::resource_exhausted(
            "upload verification round limit; resume the original request",
        ),
        root_key,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone, Copy, Debug)]
    enum Mode {
        Pending,
        ResumedFull,
        OtherSession,
        ShortOffset,
        PendingReceipt,
        OtherReceipt,
        UnknownState,
        NarrowDeadline,
    }
    struct Upload {
        id: [u8; 32],
        descriptor: m::Descriptor,
        bytes: Vec<u8>,
        finalized: Vec<Value>,
        append_calls: usize,
    }

    // Real canonical local source + SDK HTTP transport. This supporting uploader
    // contract test is not Cloudflare authorization/storage or mounted-kernel proof.
    #[tokio::test]
    async fn finalize_continuations_keep_one_exact_intent_and_reject_unbound_progress() {
        for mode in [
            Mode::Pending,
            Mode::ResumedFull,
            Mode::OtherSession,
            Mode::ShortOffset,
            Mode::PendingReceipt,
            Mode::OtherReceipt,
            Mode::UnknownState,
            Mode::NarrowDeadline,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let io = Arc::new(
                jj_native_backend::DirectObjectIo::init(directory.path(), Default::default())
                    .unwrap(),
            );
            let tree = io
                .write_metadata(m::Object::Tree(m::Tree::default()))
                .await
                .unwrap();
            let person = m::Signature {
                name: "Fixture".into(),
                email: "fixture@example.invalid".into(),
                timestamp: m::Timestamp {
                    millis_since_epoch: 0,
                    tz_offset_minutes: 0,
                },
            };
            let commit = |parent: m::CommitId, tree: m::TreeId, change: u8| {
                m::Object::Commit(m::Commit {
                    parents: vec![parent],
                    predecessors: vec![],
                    root_tree: m::TreeMerge {
                        terms: vec![tree],
                        labels: vec![String::new()],
                    },
                    change_id: m::ChangeId([change; 16]),
                    description: "Upload fixture".into(),
                    author: person.clone(),
                    committer: person.clone(),
                    secure_signature: None,
                })
            };
            let base = io
                .write_metadata(commit(m::CommitId::ROOT, m::TreeId(tree.key.id.0), 1))
                .await
                .unwrap();
            let original = b"immutable authored bytes\n".to_vec();
            let file = io
                .write_file(
                    &mut futures::io::Cursor::new(original.clone()),
                    original.len() as u64,
                )
                .await
                .unwrap();
            let file_descriptor = m::describe_file(&original);
            let tree = io
                .write_metadata(m::Object::Tree(m::Tree {
                    entries: vec![m::TreeEntry {
                        name: "file.txt".into(),
                        value: m::TreeValue::File {
                            id: file,
                            executable: false,
                            size_bytes: original.len() as u64,
                            copy_id: None,
                        },
                    }],
                }))
                .await
                .unwrap();
            let head = io
                .write_metadata(commit(
                    m::CommitId(base.key.id.0),
                    m::TreeId(tree.key.id.0),
                    2,
                ))
                .await
                .unwrap();
            let mut canonical = BTreeMap::new();
            canonical.insert(
                (1, file_descriptor.key.id.0),
                (file_descriptor, original.clone()),
            );
            for descriptor in [tree, head] {
                let metadata = io.read_metadata(descriptor.key).await.unwrap();
                canonical.insert(
                    (descriptor.key.kind as u8, descriptor.key.id.0),
                    (descriptor, metadata.bytes().to_vec()),
                );
            }
            let canonical = Arc::new(canonical);
            let uploads = Arc::new(Mutex::new(BTreeMap::<String, Upload>::new()));
            let observed = uploads.clone();
            let expected = canonical.clone();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let (stop, mut stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = tokio::select! {result=listener.accept()=>result.unwrap(),_=&mut stopped=>break};
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        if socket.read_exact(&mut byte).await.is_err() {
                            break;
                        }
                        header.push(byte[0]);
                        assert!(header.len() < 16384);
                    }
                    if !header.ends_with(b"\r\n\r\n") {
                        continue;
                    }
                    let header = String::from_utf8(header).unwrap().to_ascii_lowercase();
                    assert!(header.starts_with("post /api/v1/forge/call http/1.1\r\n"));
                    let length: usize = header
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    assert!(length < 2 * 1024 * 1024);
                    let mut bytes = vec![0; length];
                    if socket.read_exact(&mut bytes).await.is_err() {
                        continue;
                    }
                    let input: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(input["operation"], 10);
                    assert_eq!(input["account_id"], "example");
                    assert_eq!(input["view"], "public");
                    let payload: Value =
                        serde_json::from_str(input["payload_json"].as_str().unwrap()).unwrap();
                    let key = input["request_id"].as_str().unwrap().to_owned();
                    let (result, drop_response) = {
                        let mut uploads = observed.lock().unwrap();
                        match payload["action"].as_str().unwrap() {
                            "begin" => {
                                let kind = payload["kind"].as_u64().unwrap() as u8;
                                let id: [u8; 32] =
                                    serde_json::from_value(payload["id"].clone()).unwrap();
                                let (descriptor, bytes) = expected.get(&(kind, id)).unwrap();
                                assert_eq!(payload["size_bytes"], descriptor.size_bytes);
                                assert_eq!(payload["version"], json!(vec![1u8; 32]));
                                let session = *blake3::hash(key.as_bytes()).as_bytes();
                                let upload = uploads.entry(key.clone()).or_insert_with(|| Upload {
                                    id: session,
                                    descriptor: *descriptor,
                                    bytes: if matches!(mode, Mode::ResumedFull) {
                                        bytes.clone()
                                    } else {
                                        vec![]
                                    },
                                    finalized: vec![],
                                    append_calls: 0,
                                });
                                (
                                    json!({"session_id":upload.id,"offset":upload.bytes.len(),"state":"pending","receipt":null}),
                                    false,
                                )
                            }
                            "append" => {
                                let upload = uploads.get_mut(&key).unwrap();
                                assert_eq!(payload["session_id"], json!(upload.id));
                                assert_eq!(payload["offset"], upload.bytes.len());
                                let data: Vec<u8> =
                                    serde_json::from_value(payload["data"].clone()).unwrap();
                                upload.bytes.extend_from_slice(&data);
                                upload.append_calls += 1;
                                (
                                    json!({"session_id":upload.id,"offset":upload.bytes.len(),"state":"pending","receipt":null}),
                                    false,
                                )
                            }
                            "finalize" => {
                                let upload = uploads.get_mut(&key).unwrap();
                                assert_eq!(payload["session_id"], json!(upload.id));
                                if let Some(first) = upload.finalized.first() {
                                    assert_eq!(
                                        &input, first,
                                        "all continuation/retry envelopes remain identical"
                                    );
                                }
                                upload.finalized.push(input.clone());
                                m::verify(&upload.descriptor, &upload.bytes, Default::default())
                                    .unwrap();
                                assert_eq!(
                                    &upload.bytes,
                                    &expected[&(
                                        upload.descriptor.key.kind as u8,
                                        upload.descriptor.key.id.0
                                    )]
                                        .1
                                );
                                let pending = matches!(mode, Mode::NarrowDeadline)
                                    || upload.finalized.len() == 1;
                                let mut receipt = json!({"kind":upload.descriptor.key.kind as u8,"id":upload.descriptor.key.id.0,"size_bytes":upload.descriptor.size_bytes,"format_version":upload.descriptor.format_version,"staging_lease":"fixture-retained","expires_at":86400});
                                let mut result = json!({"session_id":upload.id,"offset":upload.bytes.len(),"state":if pending{"pending"}else{"committed"},"receipt":if pending{Value::Null}else{receipt.clone()}});
                                match mode {
                                    Mode::OtherSession => {
                                        result["session_id"] = json!(vec![9u8; 32])
                                    }
                                    Mode::ShortOffset => {
                                        result["offset"] = json!(upload.bytes.len() - 1)
                                    }
                                    Mode::PendingReceipt => result["receipt"] = receipt,
                                    Mode::OtherReceipt => {
                                        receipt["id"] = json!(vec![9u8; 32]);
                                        result["state"] = json!("committed");
                                        result["receipt"] = receipt;
                                    }
                                    Mode::UnknownState => result["state"] = json!("working"),
                                    _ => {}
                                }
                                // Commit one proof quantum before a response is
                                // lost; the SDK must retry the identical intent.
                                let lost =
                                    matches!(mode, Mode::Pending) && upload.finalized.len() == 2;
                                (result, lost)
                            }
                            other => panic!("unexpected upload action {other}"),
                        }
                    };
                    if drop_response {
                        continue;
                    }
                    let reply = json!({"version":1,"operation":10,"account_id":input["account_id"],"view":input["view"],"request_id":input["request_id"],"state":"complete","result_json":result.to_string()});
                    let bytes = serde_json::to_vec(&reply).unwrap();
                    let _=socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",bytes.len()).as_bytes()).await;
                    let _ = socket.write_all(&bytes).await;
                }
            });
            let context = Context {
                schema_version: 1,
                transport: tinytree_client::Transport::ForgeHttpsV1,
                server: endpoint,
                account_id: "example".into(),
                view: "public".into(),
                token_env: None,
                ca_certificate: None,
                tls_domain: None,
            };
            let result = if matches!(mode, Mode::NarrowDeadline) {
                // Exercise the physical sender with an inherited shorter public
                // SDK deadline; the60s continuation cap must not renew it.
                let client = NativeClient::connect(context.client_config().unwrap())
                    .await
                    .unwrap()
                    .with_operation_deadline(
                        tokio::time::Instant::now() + std::time::Duration::from_millis(80),
                    );
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    send(
                        &client,
                        &context,
                        [1; 32],
                        "saved-root-intent",
                        "file.txt",
                        file_descriptor,
                        &mut futures::io::Cursor::new(original),
                    ),
                )
                .await
                .expect("shorter SDK deadline must survive the continuation loop")
            } else {
                closure(
                    &context,
                    [1; 32],
                    base.key.id.0,
                    head.key.id.0,
                    "saved-root-intent",
                    io,
                )
                .await
            };
            if matches!(mode, Mode::Pending | Mode::ResumedFull) {
                assert!(result.is_ok(), "{mode:?}: {result:?}");
                let uploads = uploads.lock().unwrap();
                assert_eq!(uploads.len(), 3);
                for upload in uploads.values() {
                    assert!(upload.finalized.len() >= 2);
                    if matches!(mode, Mode::ResumedFull) {
                        assert_eq!(upload.append_calls, 0);
                    }
                }
            } else if matches!(mode, Mode::NarrowDeadline) {
                let error = result.unwrap_err();
                let call = error.error.downcast_ref::<rpc::CallError>().unwrap();
                assert_eq!(call.status.code(), tonic::Code::DeadlineExceeded);
                assert_eq!(call.request_id.as_deref(), Some("saved-root-intent"));
            } else {
                assert!(result.is_err(), "{mode:?} must fail closed");
                let uploads = uploads.lock().unwrap();
                assert_eq!(uploads.len(), 1);
                assert_eq!(
                    uploads.values().next().unwrap().finalized.len(),
                    1,
                    "invalid progress is not retryable"
                );
            }
            stop.send(()).unwrap();
            server.await.unwrap();
        }
    }
}
