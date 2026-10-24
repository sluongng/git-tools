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
        ensure!(
            head != m::CommitId::ROOT,
            "intrinsic root requires no remote admission"
        );
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
        let mut edges = 0usize;
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
            ensure!(
                !visiting.contains(&key),
                "native metadata closure contains a cycle"
            );
            ensure!(
                nodes.len().saturating_add(prefetched.len()) < inner.config.closure.max_objects
                    || prefetched.contains_key(&key),
                "native metadata closure object budget exceeded"
            );
            let node =
                if let Some(node) = prefetched.remove(&key) {
                    node
                } else if inner
                    .local
                    .descriptor(key)
                    .await
                    .map_err(crate::backend)?
                    .is_some()
                {
                    let object = inner
                        .local
                        .read_metadata(key)
                        .await
                        .map_err(crate::backend)?;
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
                    ensure!(remote, "native local metadata closure is incomplete");
                    // The DFS stack already contains a frontier of known children.
                    // Fetch missing siblings together, but leave DFS visitation and
                    // parent-first validation unchanged. Prefetch never follows new
                    // edges or installs objects into authoritative local storage.
                    let room = inner
                        .config
                        .closure
                        .max_objects
                        .checked_sub(nodes.len().saturating_add(prefetched.len()))
                        .context("native metadata closure object budget exceeded")?;
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
                    ensure!(
                        batch_limit != 0,
                        "native metadata closure object budget exceeded"
                    );
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
                            .map_err(crate::backend)?
                            .is_none()
                        {
                            keys.push(candidate);
                        }
                    }
                    prefetch(inner, keys, &mut prefetched, &mut bytes).await?;
                    prefetched
                        .remove(&key)
                        .context("native metadata response omitted key")?
                };
            ensure!(
                node.descriptor.key == key,
                "native closure returned another object"
            );
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
                                ensure!(
                                    *size == size_bytes,
                                    "conflicting native tree file lengths"
                                );
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
                                    "conflicting native symlink lengths"
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
            edges = edges
                .checked_add(refs.len())
                .context("native closure edge overflow")?;
            ensure!(
                edges <= inner.config.closure.max_edges,
                "native metadata closure edge budget exceeded"
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
                "native metadata traversal queue exceeded budget"
            );
            nodes.insert(key, node);
        }
        for (key, expected) in expectations {
            let node = nodes
                .get(&key)
                .context("native symlink closure is incomplete")?;
            let m::Object::Symlink(target) = &node.object else {
                bail!("native symlink closure type mismatch")
            };
            ensure!(
                target.len() as u64 == expected,
                "native symlink target length mismatch"
            );
        }
        validate_contexts(&nodes, tree_roots, &inner.config, &mut edges)?;
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
        .context("native closure byte overflow")?;
    ensure!(
        *bytes <= inner.config.closure.max_metadata_bytes,
        "native closure metadata byte budget exceeded"
    );
    Ok(())
}

async fn prefetch(
    inner: &Arc<Inner>,
    keys: Vec<m::ObjectKey>,
    prefetched: &mut HashMap<m::ObjectKey, Node>,
    bytes: &mut usize,
) -> Result<()> {
    let client = inner.connector.get().await?;
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
            Err(error) => return Err(error.into()),
        };
        ensure!(
            outcomes.len() == keys.len(),
            "native metadata response omitted key"
        );
        for (key, outcome) in keys.into_iter().zip(outcomes) {
            let MetadataOutcome::Object(object) = outcome else {
                bail!("native metadata closure is unavailable")
            };
            let descriptor = object.descriptor();
            ensure!(
                descriptor.key == key,
                "native closure returned another object"
            );
            let contents = object.canonical_bytes().to_vec();
            account_bytes(inner, bytes, contents.len())?;
            let decoded = m::decode(&contents, inner.model_limits())
                .context("invalid native canonical metadata")?;
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
                "native metadata frontier contains a duplicate"
            );
        }
    }
    Ok(())
}

pub(crate) fn validate_contexts(
    nodes: &HashMap<m::ObjectKey, Node>,
    roots: HashSet<m::TreeId>,
    config: &Config,
    edges: &mut usize,
) -> Result<()> {
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
            .context("native tree context closure is incomplete")?;
        let m::Object::Tree(tree) = &node.object else {
            bail!("native tree context kind mismatch")
        };
        for entry in &tree.entries {
            crate::io::check_deadline()?;
            *edges = edges.checked_add(1).context("native path edge overflow")?;
            ensure!(
                *edges <= config.closure.max_edges,
                "native path edge budget exceeded"
            );
            let length = prefix
                .len()
                .checked_add(usize::from(!prefix.is_empty()))
                .and_then(|n| n.checked_add(entry.name.len()))
                .context("native path length overflow")?;
            ensure!(
                length <= config.closure.max_path_bytes,
                "native path length budget exceeded"
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
                    *edges = edges.checked_add(1).context("native path edge overflow")?;
                    ensure!(
                        *edges <= config.closure.max_edges,
                        "native path edge budget exceeded"
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
                        .context("native copy context closure is incomplete")?;
                    let m::Object::CopyHistory(history) = &node.object else {
                        bail!("native copy context kind mismatch")
                    };
                    ensure!(
                        history.current_path == path,
                        "native copy history current path mismatch"
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
        "native tree path context budget exceeded"
    );
    *bytes = bytes
        .checked_add(path.len())
        .context("native path context bytes overflow")?;
    ensure!(
        *bytes <= config.closure.max_path_context_bytes,
        "native path context byte budget exceeded"
    );
    seen.insert((tree, path.clone()));
    pending.push((tree, path));
    Ok(())
}
