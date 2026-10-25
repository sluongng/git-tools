//! Capture-only staging. Retained cutoffs are sealed provider proofs, not CAS
//! presence. A completed frontier is still not publication or closure authority.
use super::{NativeTransferLimits, Owner, add, insert_file, limit};
use crate::{
    RemoteObjectIo, RemoteRepositoryControl, backend, capture_boundaries::CaptureBoundaries,
    closure, io,
};
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo;
use std::collections::{HashMap, HashSet};
use tinytree_model as m;
use tonic::Status;

struct Frontier<'a> {
    control: &'a RemoteRepositoryControl,
    boundaries: &'a CaptureBoundaries,
    limits: &'a NativeTransferLimits,
    owner: Owner,
    nodes: HashMap<m::ObjectKey, closure::Node>,
    order: Vec<m::ObjectKey>,
    files: HashMap<m::ObjectId, (u64, m::ObjectId)>,
    total_file_bytes: u64,
    metadata_bytes: usize,
    edges: usize,
    expectations: HashMap<m::ObjectKey, u64>,
    tree_roots: HashSet<m::TreeId>,
}
impl Frontier<'_> {
    fn slots(&self, extra: usize) -> Result<()> {
        self.nodes
            .len()
            .checked_add(self.files.len())
            .and_then(|n| n.checked_add(self.boundaries.count()))
            .and_then(|n| n.checked_add(extra))
            .filter(|n| *n <= self.limits.closure.max_objects)
            .ok_or_else(limit)?;
        Ok(())
    }
    fn memory(&self, extra: usize) -> Result<()> {
        let structures = self
            .limits
            .closure
            .max_objects
            .checked_mul(1024)
            .and_then(|n| {
                self.limits
                    .closure
                    .max_edges
                    .checked_mul(256)
                    .and_then(|v| n.checked_add(v))
            })
            .and_then(|n| {
                self.limits
                    .closure
                    .max_path_context_bytes
                    .checked_mul(4)
                    .and_then(|v| n.checked_add(v))
            })
            .ok_or_else(limit)?;
        let memory = self
            .metadata_bytes
            .checked_add(extra)
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_add(self.boundaries.resident_bytes()))
            .and_then(|n| n.checked_add(structures))
            .ok_or_else(limit)?;
        if u64::try_from(memory).map_err(|_| limit())? > self.limits.max_memory_bytes {
            return Err(limit());
        }
        Ok(())
    }
    fn metadata(&mut self, key: m::ObjectKey, descriptor: m::Descriptor) -> Result<()> {
        ensure!(
            descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
            "native capture metadata descriptor mismatch"
        );
        let size = usize::try_from(descriptor.size_bytes).map_err(|_| limit())?;
        if size > self.control.inner.config.remote.limits.max_metadata_bytes {
            return Err(limit());
        }
        self.memory(size)?;
        add(
            &mut self.metadata_bytes,
            size,
            self.limits.closure.max_metadata_bytes,
        )
    }
    async fn read(&mut self, key: m::ObjectKey) -> Result<closure::Node> {
        io::check_deadline()?;
        self.slots(1)?;
        let local = self.control.inner.local.clone();
        if let Some(descriptor) = local
            .descriptor_with_owner(key, self.owner.clone())
            .await
            .map_err(backend)?
        {
            self.metadata(key, descriptor)?;
            let value = local
                .read_metadata_with_owner(key, self.owner.clone())
                .await
                .map_err(backend)?;
            ensure!(
                value.descriptor() == descriptor,
                "native capture source descriptor changed"
            );
            return Ok(closure::Node {
                descriptor,
                bytes: value.bytes().to_vec(),
                object: value.into_object(),
            });
        }
        // Never use the generic provider descriptor/hydrator for a COMMIT or a
        // copy-history root. Those routes can install an entire ancestry.
        if !matches!(key.kind, m::Kind::Tree | m::Kind::Symlink) {
            return Err(Status::unimplemented(
                "uncovered native capture history is not locally available",
            )
            .into());
        }
        let cap = self.control.inner.config.remote.limits.max_metadata_bytes;
        self.memory(cap)?;
        if self
            .metadata_bytes
            .checked_add(cap)
            .is_none_or(|n| n > self.limits.closure.max_metadata_bytes)
        {
            return Err(limit());
        }
        // This exact-object read preserves existing held/query/cloud authority.
        // TREE/SYMLINK reads are canonical single objects, not recursive hydration.
        let ledger = self.control.inner.ledger.clone();
        io::blocking_owned(self.owner.clone(), move || {
            let authority = ledger
                .metadata_authority(key)?
                .context("native capture exact metadata lacks retained authority")?;
            if let crate::ledger::Authority::Root(root) = &authority {
                if root.binding != ledger.binding.scope || !root.retained() {
                    return Err(Status::failed_precondition(
                        "native capture source authority requires explicit recovery",
                    )
                    .into());
                }
            }
            ledger.validate_authority(&authority)
        })
        .await?;
        let provider = RemoteObjectIo {
            inner: self.control.inner.clone(),
        };
        let value = provider
            .read_metadata_with_owner(key, self.owner.clone())
            .await
            .map_err(backend)?;
        let descriptor = value.descriptor();
        self.metadata(key, descriptor)?;
        Ok(closure::Node {
            descriptor,
            bytes: value.bytes().to_vec(),
            object: value.into_object(),
        })
    }
    fn references(&mut self, object: &m::Object) -> Result<Vec<m::ObjectKey>> {
        let count = match object {
            m::Object::Commit(c) => c
                .parents
                .len()
                .checked_add(c.predecessors.len())
                .and_then(|n| n.checked_add(c.root_tree.terms.len())),
            m::Object::Tree(t) => t.entries.iter().try_fold(0usize, |n, e| {
                n.checked_add(
                    1 + usize::from(matches!(
                        e.value,
                        m::TreeValue::File {
                            copy_id: Some(_),
                            ..
                        }
                    )),
                )
            }),
            m::Object::CopyHistory(c) => Some(c.parents.len()),
            m::Object::Symlink(_) => Some(0),
        }
        .ok_or_else(limit)?;
        add(&mut self.edges, count, self.limits.closure.max_edges)?;
        Ok(object.references())
    }
    fn file(&mut self, key: m::ObjectKey, size: u64, tree: m::ObjectId) -> Result<()> {
        if self.boundaries.covers(&key) {
            return Ok(());
        }
        self.slots(usize::from(!self.files.contains_key(&key.id)))?;
        insert_file(
            &mut self.files,
            &mut self.total_file_bytes,
            key.id,
            size,
            tree,
            self.nodes.len() + self.boundaries.count(),
            self.limits,
        )
    }
    fn tree_entries(&mut self, key: m::ObjectKey, object: &m::Object) -> Result<()> {
        if let m::Object::Commit(c) = object {
            self.tree_roots.extend(c.root_tree.terms.iter().copied());
        }
        if let m::Object::Tree(tree) = object {
            for entry in &tree.entries {
                match entry.value {
                    m::TreeValue::File { id, size_bytes, .. } => self.file(
                        m::ObjectKey {
                            kind: m::Kind::File,
                            id: m::ObjectId(id.0),
                        },
                        size_bytes,
                        key.id,
                    )?,
                    m::TreeValue::Symlink { id, target_bytes } => {
                        let key = m::ObjectKey {
                            kind: m::Kind::Symlink,
                            id: m::ObjectId(id.0),
                        };
                        if !self.boundaries.covers(&key) {
                            if !self.expectations.contains_key(&key)
                                && self.expectations.len() >= self.limits.closure.max_objects
                            {
                                return Err(limit());
                            }
                            if let Some(old) = self.expectations.insert(key, target_bytes) {
                                ensure!(
                                    old == target_bytes,
                                    "native capture conflicting symlink lengths"
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
    fn contexts(&mut self) -> Result<()> {
        let mut pending = Vec::new();
        let mut seen = HashSet::new();
        let mut bytes = 0usize;
        for root in &self.tree_roots {
            if self.boundaries.covers(&m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(root.0),
            }) {
                continue;
            }
            if pending.len() >= self.limits.closure.max_path_contexts {
                return Err(limit());
            }
            pending.push((*root, String::new()));
        }
        while let Some((id, prefix)) = pending.pop() {
            io::check_deadline()?;
            let key = m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            };
            if self.boundaries.covers(&key) {
                continue;
            }
            if seen.contains(&(id, prefix.clone())) {
                continue;
            }
            if seen.len() >= self.limits.closure.max_path_contexts {
                return Err(limit());
            }
            add(
                &mut bytes,
                prefix.len(),
                self.limits.closure.max_path_context_bytes,
            )?;
            seen.insert((id, prefix.clone()));
            let m::Object::Tree(tree) = &self
                .nodes
                .get(&key)
                .context("native capture tree context missing")?
                .object
            else {
                anyhow::bail!("native capture tree context kind mismatch");
            };
            for entry in &tree.entries {
                add(&mut self.edges, 1, self.limits.closure.max_edges)?;
                let length = prefix
                    .len()
                    .checked_add(usize::from(!prefix.is_empty()))
                    .and_then(|n| n.checked_add(entry.name.len()))
                    .ok_or_else(limit)?;
                if length > self.limits.closure.max_path_bytes {
                    return Err(limit());
                }
                match entry.value {
                    m::TreeValue::Tree(child) => {
                        if pending.len() >= self.limits.closure.max_path_contexts {
                            return Err(limit());
                        }
                        // Charge queued strings before allocation, not just after
                        // dequeue; repeated DAG contexts cannot hide in the queue.
                        add(
                            &mut bytes,
                            length,
                            self.limits.closure.max_path_context_bytes,
                        )?;
                        let path = if prefix.is_empty() {
                            entry.name.clone()
                        } else {
                            format!("{prefix}/{}", entry.name)
                        };
                        pending.push((child, path));
                    }
                    m::TreeValue::File {
                        copy_id: Some(copy),
                        ..
                    } => {
                        let key = m::ObjectKey {
                            kind: m::Kind::CopyHistory,
                            id: m::ObjectId(copy.0),
                        };
                        if self.boundaries.covers(&key) {
                            continue;
                        }
                        add(&mut self.edges, 1, self.limits.closure.max_edges)?;
                        let m::Object::CopyHistory(history) = &self
                            .nodes
                            .get(&key)
                            .context("native capture copy context missing")?
                            .object
                        else {
                            anyhow::bail!("native capture copy context kind mismatch");
                        };
                        add(
                            &mut bytes,
                            length,
                            self.limits.closure.max_path_context_bytes,
                        )?;
                        let path = if prefix.is_empty() {
                            entry.name.clone()
                        } else {
                            format!("{prefix}/{}", entry.name)
                        };
                        ensure!(
                            history.current_path == path,
                            "native capture copy path mismatch"
                        );
                    }
                    _ => {}
                }
            }
        }
        // Complete cutoff-subtree path validation remains the final server
        // publication's job; staging cannot turn a reused subtree into a proof.
        Ok(())
    }
}

pub(super) async fn collect(
    control: &RemoteRepositoryControl,
    roots: Vec<m::ObjectKey>,
    boundaries: &CaptureBoundaries,
    limits: &NativeTransferLimits,
    owner: Owner,
) -> Result<closure::Closure> {
    let mut frontier = Frontier {
        control,
        boundaries,
        limits,
        owner,
        nodes: HashMap::new(),
        order: Vec::new(),
        files: HashMap::new(),
        total_file_bytes: 0,
        metadata_bytes: 0,
        edges: boundaries.edge_count(),
        expectations: HashMap::new(),
        tree_roots: HashSet::new(),
    };
    frontier.slots(0)?;
    frontier.memory(0)?;
    add(&mut frontier.edges, roots.len(), limits.closure.max_edges)?;
    let mut commits = Vec::new();
    let mut others = Vec::new();
    for key in roots {
        if boundaries.covers(&key) || (key.kind == m::Kind::Commit && key.id.0 == [0; 32]) {
            continue;
        }
        if key.kind == m::Kind::Commit {
            commits.push((key, false));
        } else if key.kind == m::Kind::File {
            let d = control
                .inner
                .local
                .descriptor_with_owner(key, frontier.owner.clone())
                .await
                .map_err(backend)?
                .ok_or_else(|| {
                    Status::unimplemented(
                        "uncovered native capture FILE root is not locally available",
                    )
                })?;
            ensure!(
                d.key == key && d.format_version == m::FORMAT_VERSION,
                "native capture FILE root mismatch"
            );
            frontier.file(key, d.size_bytes, key.id)?;
        } else {
            if key.kind == m::Kind::Tree {
                frontier.tree_roots.insert(m::TreeId(key.id.0));
            }
            others.push((key, false));
        }
    }
    let mut done = HashSet::new();
    let mut visiting = HashSet::new();
    // Finish every COMMIT chain before independent TREE roots. Parent cutoffs
    // already know the authenticated root trees even when the checkout is dirty.
    for commit_phase in [true, false] {
        let mut pending = if commit_phase {
            std::mem::take(&mut commits)
        } else {
            std::mem::take(&mut others)
        };
        while let Some((key, exit)) = pending.pop() {
            io::check_deadline()?;
            if boundaries.covers(&key) || (key.kind == m::Kind::Commit && key.id.0 == [0; 32]) {
                continue;
            }
            if exit {
                visiting.remove(&key);
                done.insert(key);
                frontier.order.push(key);
                continue;
            }
            if done.contains(&key) {
                continue;
            }
            ensure!(!visiting.contains(&key), "native capture metadata cycle");
            let node = frontier.read(key).await?;
            let refs = frontier.references(&node.object)?;
            frontier.tree_entries(key, &node.object)?;
            frontier.slots(1)?;
            let queued = pending
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_add(refs.len()))
                .and_then(|n| n.checked_add(if commit_phase { others.len() } else { 0 }))
                .ok_or_else(limit)?;
            if queued > limits.closure.max_edges {
                return Err(limit());
            }
            visiting.insert(key);
            pending.push((key, true));
            for child in refs.into_iter().rev() {
                if child.kind == m::Kind::File || boundaries.covers(&child) || done.contains(&child)
                {
                    continue;
                }
                if child.kind == m::Kind::Commit && child.id.0 == [0; 32] {
                    continue;
                }
                if commit_phase && child.kind != m::Kind::Commit {
                    others.push((child, false));
                } else {
                    pending.push((child, false));
                }
            }
            if pending
                .len()
                .checked_add(if commit_phase { others.len() } else { 0 })
                .is_none_or(|n| n > limits.closure.max_edges)
            {
                return Err(limit());
            }
            frontier.nodes.insert(key, node);
        }
    }
    for (key, expected) in &frontier.expectations {
        let m::Object::Symlink(target) = &frontier
            .nodes
            .get(key)
            .context("native capture symlink missing")?
            .object
        else {
            anyhow::bail!("native capture symlink kind mismatch");
        };
        ensure!(
            target.len() as u64 == *expected,
            "native capture symlink size mismatch"
        );
    }
    frontier.contexts()?;
    Ok(closure::Closure {
        nodes: frontier.nodes,
        order: frontier.order,
        files: frontier
            .files
            .into_iter()
            .map(|(id, (size, tree))| (id, size, tree))
            .collect(),
    })
}
