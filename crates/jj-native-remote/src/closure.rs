//! Complete canonical traversal. Summary RPCs never establish admission.
use crate::{Config, Inner};
use anyhow::{Context, Result, bail, ensure};
use jj_native_backend::NativeObjectIo;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tinytree_client::MetadataOutcome;
use tinytree_model as m;
// Payload-free contexts keep publication diagnostics independent of error text.
#[derive(Debug)]
pub(crate) enum ClosureFailure {
    IntrinsicRoot,
    Cycle,
    ObjectBudget,
    LocalIncomplete,
    RemoteResponseMissing,
    ObjectIdentity,
    FileLength,
    SymlinkLengthConflict,
    EdgeOverflow,
    EdgeBudget,
    QueueBudget,
    SymlinkMissing,
    SymlinkKind,
    SymlinkLength,
    MetadataBytesOverflow,
    MetadataBytesBudget,
    RemoteUnavailable,
    CanonicalMetadata,
    DuplicateResponse,
    TreeContextMissing,
    TreeContextKind,
    PathEdgeOverflow,
    PathEdgeBudget,
    PathLengthOverflow,
    PathLengthBudget,
    CopyContextMissing,
    CopyContextKind,
    CopyContextPath,
    PathContextsBudget,
    PathContextBytesOverflow,
    PathContextBytesBudget,
    LocalDescriptor,
    LocalMetadataRead,
    RemoteConnect,
    RemoteRead,
    InstallDescriptor,
    InstallWrite,
    InstallIdentity,
}
impl ClosureFailure {
    pub(crate) fn category(&self) -> &'static str {
        match self {
            Self::IntrinsicRoot => "closure_intrinsic_root",
            Self::Cycle => "closure_cycle",
            Self::ObjectBudget => "closure_object_budget",
            Self::LocalIncomplete => "closure_local_incomplete",
            Self::RemoteResponseMissing => "closure_remote_response_missing",
            Self::ObjectIdentity => "closure_object_identity",
            Self::FileLength => "closure_file_length",
            Self::SymlinkLengthConflict => "closure_symlink_length_conflict",
            Self::EdgeOverflow => "closure_edge_overflow",
            Self::EdgeBudget => "closure_edge_budget",
            Self::QueueBudget => "closure_queue_budget",
            Self::SymlinkMissing => "closure_symlink_missing",
            Self::SymlinkKind => "closure_symlink_kind",
            Self::SymlinkLength => "closure_symlink_length",
            Self::MetadataBytesOverflow => "closure_metadata_bytes_overflow",
            Self::MetadataBytesBudget => "closure_metadata_bytes_budget",
            Self::RemoteUnavailable => "closure_remote_unavailable",
            Self::CanonicalMetadata => "closure_canonical_metadata",
            Self::DuplicateResponse => "closure_duplicate_response",
            Self::TreeContextMissing => "closure_tree_context_missing",
            Self::TreeContextKind => "closure_tree_context_kind",
            Self::PathEdgeOverflow => "closure_path_edge_overflow",
            Self::PathEdgeBudget => "closure_path_edge_budget",
            Self::PathLengthOverflow => "closure_path_length_overflow",
            Self::PathLengthBudget => "closure_path_length_budget",
            Self::CopyContextMissing => "closure_copy_context_missing",
            Self::CopyContextKind => "closure_copy_context_kind",
            Self::CopyContextPath => "closure_copy_context_path",
            Self::PathContextsBudget => "closure_path_contexts_budget",
            Self::PathContextBytesOverflow => "closure_path_context_bytes_overflow",
            Self::PathContextBytesBudget => "closure_path_context_bytes_budget",
            Self::LocalDescriptor => "closure_local_descriptor",
            Self::LocalMetadataRead => "closure_local_metadata_read",
            Self::RemoteConnect => "closure_remote_connect",
            Self::RemoteRead => "closure_remote_read",
            Self::InstallDescriptor => "closure_install_descriptor",
            Self::InstallWrite => "closure_install_write",
            Self::InstallIdentity => "closure_install_identity",
        }
    }
}
impl std::fmt::Display for ClosureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IntrinsicRoot => "intrinsic root requires no remote admission",
            Self::Cycle => "native metadata closure contains a cycle",
            Self::ObjectBudget => "native metadata closure object budget exceeded",
            Self::LocalIncomplete => "native local metadata closure is incomplete",
            Self::RemoteResponseMissing => "native metadata response omitted key",
            Self::ObjectIdentity => "native closure returned another object",
            Self::FileLength => "conflicting native tree file lengths",
            Self::SymlinkLengthConflict => "conflicting native symlink lengths",
            Self::EdgeOverflow => "native closure edge overflow",
            Self::EdgeBudget => "native metadata closure edge budget exceeded",
            Self::QueueBudget => "native metadata traversal queue exceeded budget",
            Self::SymlinkMissing => "native symlink closure is incomplete",
            Self::SymlinkKind => "native symlink closure type mismatch",
            Self::SymlinkLength => "native symlink target length mismatch",
            Self::MetadataBytesOverflow => "native closure byte overflow",
            Self::MetadataBytesBudget => "native closure metadata byte budget exceeded",
            Self::RemoteUnavailable => "native metadata closure is unavailable",
            Self::CanonicalMetadata => "invalid native canonical metadata",
            Self::DuplicateResponse => "native metadata frontier contains a duplicate",
            Self::TreeContextMissing => "native tree context closure is incomplete",
            Self::TreeContextKind => "native tree context kind mismatch",
            Self::PathEdgeOverflow => "native path edge overflow",
            Self::PathEdgeBudget => "native path edge budget exceeded",
            Self::PathLengthOverflow => "native path length overflow",
            Self::PathLengthBudget => "native path length budget exceeded",
            Self::CopyContextMissing => "native copy context closure is incomplete",
            Self::CopyContextKind => "native copy context kind mismatch",
            Self::CopyContextPath => "native copy history current path mismatch",
            Self::PathContextsBudget => "native tree path context budget exceeded",
            Self::PathContextBytesOverflow => "native path context bytes overflow",
            Self::PathContextBytesBudget => "native path context byte budget exceeded",
            Self::LocalDescriptor => "native local metadata descriptor failed",
            Self::LocalMetadataRead => "native local metadata read failed",
            Self::RemoteConnect => "native metadata remote connection failed",
            Self::RemoteRead => "native metadata remote read failed",
            Self::InstallDescriptor => "native metadata installation descriptor failed",
            Self::InstallWrite => "native metadata installation write failed",
            Self::InstallIdentity => "native metadata installation changed identity",
        })
    }
}
impl std::error::Error for ClosureFailure {}

pub(crate) struct Node {
    pub descriptor: m::Descriptor,
    pub bytes: Vec<u8>,
    pub object: m::Object,
}
pub(crate) struct Closure {
    pub nodes: HashMap<m::ObjectKey, Node>,
    pub order: Vec<m::ObjectKey>,
    pub files: Vec<(m::ObjectId, u64, m::ObjectId)>,
}
impl Closure {
    pub async fn collect(inner: &Arc<Inner>, head: m::CommitId, remote: bool) -> Result<Self> {
        let root = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head.0),
        };
        ensure!(head != m::CommitId::ROOT, ClosureFailure::IntrinsicRoot);
        let mut nodes = HashMap::new();
        let mut prefetched = HashMap::new();
        let mut pending = vec![(root, false)];
        let mut visiting = HashSet::new();
        let mut done = HashSet::new();
        let mut order = Vec::new();
        let mut files = HashMap::<m::ObjectId, (u64, m::ObjectId)>::new();
        let mut expectations = HashMap::<m::ObjectKey, u64>::new();
        let mut tree_roots = HashSet::new();
        let mut bytes = 0usize;
        let mut graph_edges = 0usize;
        while let Some((key, exit)) = pending.pop() {
            crate::io::check_deadline()?;
            if key.kind == m::Kind::Commit && key.id.0 == [0; 32] {
                continue;
            }
            if key.kind == m::Kind::File {
                continue;
            }
            if exit {
                visiting.remove(&key);
                done.insert(key);
                order.push(key);
                continue;
            }
            if done.contains(&key) {
                continue;
            }
            ensure!(!visiting.contains(&key), ClosureFailure::Cycle);
            ensure!(
                nodes.len().saturating_add(prefetched.len()) < inner.config.closure.max_objects
                    || prefetched.contains_key(&key),
                ClosureFailure::ObjectBudget
            );
            let node =
                if let Some(node) = prefetched.remove(&key) {
                    node
                } else if inner
                    .local
                    .descriptor(key)
                    .await
                    .map_err(crate::backend)
                    .context(ClosureFailure::LocalDescriptor)?
                    .is_some()
                {
                    let object = inner
                        .local
                        .read_metadata(key)
                        .await
                        .map_err(crate::backend)
                        .context(ClosureFailure::LocalMetadataRead)?;
                    crate::io::check_deadline()?;
                    let descriptor = object.descriptor();
                    let contents = object.bytes().to_vec();
                    account_bytes(inner, &mut bytes, contents.len())?;
                    Node {
                        descriptor,
                        bytes: contents,
                        object: object.into_object(),
                    }
                } else {
                    ensure!(remote, ClosureFailure::LocalIncomplete);
                    // The DFS stack already contains a frontier of known children.
                    // Fetch missing siblings together, but leave DFS visitation and
                    // parent-first validation unchanged. Prefetch never follows new
                    // edges or installs objects into authoritative local storage.
                    let room = inner
                        .config
                        .closure
                        .max_objects
                        .checked_sub(nodes.len().saturating_add(prefetched.len()))
                        .context(ClosureFailure::ObjectBudget)?;
                    // A typed 32-byte key uses at most 40 encoded bytes including
                    // its outer field. Reserve repository string framing as well.
                    let request_limit =
                        inner.config.remote.limits.max_request_bytes.saturating_sub(
                            inner.config.remote.repository_id.len().saturating_add(8),
                        ) / 40;
                    let batch_limit = inner
                        .config
                        .remote
                        .limits
                        .max_batch_items
                        .min(room)
                        .min(request_limit);
                    ensure!(batch_limit != 0, ClosureFailure::ObjectBudget);
                    let mut keys = vec![key];
                    let mut selected = HashSet::from([key]);
                    for &(candidate, exiting) in pending.iter().rev() {
                        if keys.len() >= batch_limit {
                            break;
                        }
                        if exiting
                            || candidate.kind == m::Kind::File
                            || (candidate.kind == m::Kind::Commit && candidate.id.0 == [0; 32])
                            || nodes.contains_key(&candidate)
                            || prefetched.contains_key(&candidate)
                            || visiting.contains(&candidate)
                            || !selected.insert(candidate)
                        {
                            continue;
                        }
                        crate::io::check_deadline()?;
                        if inner
                            .local
                            .descriptor(candidate)
                            .await
                            .map_err(crate::backend)
                            .context(ClosureFailure::LocalDescriptor)?
                            .is_none()
                        {
                            keys.push(candidate);
                        }
                    }
                    prefetch(inner, keys, &mut prefetched, &mut bytes).await?;
                    prefetched
                        .remove(&key)
                        .context(ClosureFailure::RemoteResponseMissing)?
                };
            ensure!(node.descriptor.key == key, ClosureFailure::ObjectIdentity);
            if let m::Object::Commit(commit) = &node.object {
                for tree in &commit.root_tree.terms {
                    tree_roots.insert(*tree);
                }
            }
            if let m::Object::Tree(tree) = &node.object {
                for entry in &tree.entries {
                    match entry.value {
                        m::TreeValue::File { id, size_bytes, .. } => {
                            let id = m::ObjectId(id.0);
                            if let Some((size, _)) = files.get(&id) {
                                ensure!(*size == size_bytes, ClosureFailure::FileLength);
                            } else {
                                files.insert(id, (size_bytes, key.id));
                            }
                        }
                        m::TreeValue::Symlink { id, target_bytes } => {
                            let child = m::ObjectKey {
                                kind: m::Kind::Symlink,
                                id: m::ObjectId(id.0),
                            };
                            if let Some(expected) = expectations.insert(child, target_bytes) {
                                ensure!(
                                    expected == target_bytes,
                                    ClosureFailure::SymlinkLengthConflict
                                );
                            }
                        }
                        m::TreeValue::Tree(_) => {}
                    }
                }
            }
            visiting.insert(key);
            pending.push((key, true));
            let refs = node.object.references();
            graph_edges = graph_edges
                .checked_add(refs.len())
                .context(ClosureFailure::EdgeOverflow)?;
            ensure!(
                graph_edges <= inner.config.closure.max_edges,
                ClosureFailure::EdgeBudget
            );
            for child in refs.into_iter().rev() {
                if child.kind != m::Kind::File
                    && !(child.kind == m::Kind::Commit && child.id.0 == [0; 32])
                    && !done.contains(&child)
                {
                    pending.push((child, false));
                }
            }
            ensure!(
                pending.len() <= inner.config.closure.max_edges,
                ClosureFailure::QueueBudget
            );
            nodes.insert(key, node);
        }
        for (key, expected) in expectations {
            let node = nodes.get(&key).context(ClosureFailure::SymlinkMissing)?;
            let m::Object::Symlink(target) = &node.object else {
                bail!(ClosureFailure::SymlinkKind)
            };
            ensure!(
                target.len() as u64 == expected,
                ClosureFailure::SymlinkLength
            );
        }
        validate_contexts(&nodes, tree_roots, &inner.config)?;
        let files = files
            .into_iter()
            .map(|(id, (size, tree))| (id, size, tree))
            .collect();
        Ok(Self {
            nodes,
            order,
            files,
        })
    }
    pub fn descriptors(&self) -> Vec<m::Descriptor> {
        self.order
            .iter()
            .map(|key| self.nodes[key].descriptor)
            .collect()
    }
}
fn account_bytes(inner: &Inner, bytes: &mut usize, additional: usize) -> Result<()> {
    *bytes = bytes
        .checked_add(additional)
        .context(ClosureFailure::MetadataBytesOverflow)?;
    ensure!(
        *bytes <= inner.config.closure.max_metadata_bytes,
        ClosureFailure::MetadataBytesBudget
    );
    Ok(())
}

async fn prefetch(
    inner: &Arc<Inner>,
    keys: Vec<m::ObjectKey>,
    prefetched: &mut HashMap<m::ObjectKey, Node>,
    bytes: &mut usize,
) -> Result<()> {
    let client = inner
        .connector
        .get()
        .await
        .context(ClosureFailure::RemoteConnect)?;
    let mut batches = vec![keys];
    while let Some(mut keys) = batches.pop() {
        crate::io::check_deadline()?;
        let outcomes = match crate::io::network(client.read_objects(keys.clone())).await {
            Ok(outcomes) => outcomes,
            // Metadata sizes are unknown until read. A batch may exceed the
            // aggregate response/request limit although every individual object
            // fits. Split only bounded multi-item requests; a failing singleton
            // remains an admission failure, as it was before batching.
            Err(error) if error.code() == tonic::Code::ResourceExhausted && keys.len() > 1 => {
                let tail = keys.split_off(keys.len() / 2);
                batches.push(tail);
                batches.push(keys);
                continue;
            }
            Err(error) => return Err(anyhow::Error::new(error).context(ClosureFailure::RemoteRead)),
        };
        ensure!(
            outcomes.len() == keys.len(),
            ClosureFailure::RemoteResponseMissing
        );
        for (key, outcome) in keys.into_iter().zip(outcomes) {
            let MetadataOutcome::Object(object) = outcome else {
                bail!(ClosureFailure::RemoteUnavailable)
            };
            let descriptor = object.descriptor();
            ensure!(descriptor.key == key, ClosureFailure::ObjectIdentity);
            let contents = object.canonical_bytes().to_vec();
            account_bytes(inner, bytes, contents.len())?;
            let decoded = m::decode(&contents, inner.model_limits())
                .context(ClosureFailure::CanonicalMetadata)?;
            ensure!(
                prefetched
                    .insert(
                        key,
                        Node {
                            descriptor,
                            bytes: contents,
                            object: decoded
                        }
                    )
                    .is_none(),
                ClosureFailure::DuplicateResponse
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_contexts(
    nodes: &HashMap<m::ObjectKey, Node>,
    roots: HashSet<m::TreeId>,
    config: &Config,
) -> Result<()> {
    // One path-work allowance spans all roots and all distinct (tree, prefix) contexts.
    let mut path_edges = 0usize;
    let mut pending = Vec::new();
    let mut seen = HashSet::new();
    let mut context_bytes = 0usize;
    for tree in roots {
        enqueue(
            tree,
            String::new(),
            &mut seen,
            &mut pending,
            &mut context_bytes,
            config,
        )?;
    }
    while let Some((tree, prefix)) = pending.pop() {
        crate::io::check_deadline()?;
        let key = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(tree.0),
        };
        let node = nodes
            .get(&key)
            .context(ClosureFailure::TreeContextMissing)?;
        let m::Object::Tree(tree) = &node.object else {
            bail!(ClosureFailure::TreeContextKind)
        };
        for entry in &tree.entries {
            crate::io::check_deadline()?;
            path_edges = path_edges
                .checked_add(1)
                .context(ClosureFailure::PathEdgeOverflow)?;
            ensure!(
                path_edges <= config.closure.max_edges,
                ClosureFailure::PathEdgeBudget
            );
            let length = prefix
                .len()
                .checked_add(usize::from(!prefix.is_empty()))
                .and_then(|n| n.checked_add(entry.name.len()))
                .context(ClosureFailure::PathLengthOverflow)?;
            ensure!(
                length <= config.closure.max_path_bytes,
                ClosureFailure::PathLengthBudget
            );
            match entry.value {
                m::TreeValue::Tree(child) => {
                    let path = if prefix.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{prefix}/{}", entry.name)
                    };
                    enqueue(
                        child,
                        path,
                        &mut seen,
                        &mut pending,
                        &mut context_bytes,
                        config,
                    )?;
                }
                m::TreeValue::File {
                    copy_id: Some(copy),
                    ..
                } => {
                    path_edges = path_edges
                        .checked_add(1)
                        .context(ClosureFailure::PathEdgeOverflow)?;
                    ensure!(
                        path_edges <= config.closure.max_edges,
                        ClosureFailure::PathEdgeBudget
                    );
                    let path = if prefix.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{prefix}/{}", entry.name)
                    };
                    let node = nodes
                        .get(&m::ObjectKey {
                            kind: m::Kind::CopyHistory,
                            id: m::ObjectId(copy.0),
                        })
                        .context(ClosureFailure::CopyContextMissing)?;
                    let m::Object::CopyHistory(history) = &node.object else {
                        bail!(ClosureFailure::CopyContextKind)
                    };
                    ensure!(
                        history.current_path == path,
                        ClosureFailure::CopyContextPath
                    );
                }
                _ => {}
            }
        }
    }
    Ok(())
}
fn enqueue(
    tree: m::TreeId,
    path: String,
    seen: &mut HashSet<(m::TreeId, String)>,
    pending: &mut Vec<(m::TreeId, String)>,
    bytes: &mut usize,
    config: &Config,
) -> Result<()> {
    if seen.contains(&(tree, path.clone())) {
        return Ok(());
    }
    ensure!(
        seen.len() < config.closure.max_path_contexts,
        ClosureFailure::PathContextsBudget
    );
    *bytes = bytes
        .checked_add(path.len())
        .context(ClosureFailure::PathContextBytesOverflow)?;
    ensure!(
        *bytes <= config.closure.max_path_context_bytes,
        ClosureFailure::PathContextBytesBudget
    );
    seen.insert((tree, path.clone()));
    pending.push((tree, path));
    Ok(())
}
