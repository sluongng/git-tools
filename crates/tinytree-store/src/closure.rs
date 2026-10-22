//! Bounded immutable-graph validation and contextual copy provenance.
use super::*;
use tinytree_model::{Object, TreeId, TreeValue};

struct Budget {
    edges: usize,
    maximum: usize,
}
impl Budget {
    fn edge(&mut self) -> Result<()> {
        self.edges = self
            .edges
            .checked_add(1)
            .ok_or(Error::Limit("closure edges"))?;
        if self.edges > self.maximum {
            return Err(Error::Limit("closure edges"));
        }
        Ok(())
    }
}

fn enqueue(
    key: ObjectKey,
    discovered: &mut HashSet<ObjectKey>,
    pending: &mut Vec<ObjectKey>,
    budget: &mut Budget,
    maximum: usize,
) -> Result<()> {
    budget.edge()?;
    // Deduplicate BEFORE queueing, not after popping: a repeated-edge chain must
    // not build a pending stack proportional to every duplicate edge.
    if discovered.insert(key) {
        if discovered.len() > maximum {
            return Err(Error::Limit("object closure"));
        }
        pending.push(key);
    }
    Ok(())
}

impl Inner {
    pub(super) fn closure(
        &self,
        repo: &str,
        roots: impl IntoIterator<Item = ObjectKey>,
        strict: bool,
        verify_file_bytes: bool,
    ) -> Result<HashSet<ObjectKey>> {
        let mut budget = Budget {
            edges: 0,
            maximum: self.config.max_closure_edges,
        };
        let mut discovered = HashSet::new();
        let mut pending = Vec::new();
        for root in roots {
            enqueue(
                root,
                &mut discovered,
                &mut pending,
                &mut budget,
                self.config.max_closure_objects,
            )?;
        }
        let mut tree_roots = HashSet::new();
        while let Some(key) = pending.pop() {
            let Some(descriptor) = self.descriptor(repo, key)? else {
                if strict {
                    return Err(Error::MissingObject(key));
                } else {
                    continue;
                }
            };
            // File content cannot introduce edges. GC validates its presence and
            // length, while reads/publication verify bytes outside the gate.
            if key.kind == Kind::File {
                if verify_file_bytes {
                    let mut file = File::open(self.path(repo, key))?;
                    verify_stream(&mut file, descriptor, self.config.model_limits)?;
                }
                continue;
            }
            let object = self.metadata(repo, descriptor)?;
            match &object {
                Object::Tree(tree) => {
                    for entry in &tree.entries {
                        let expected = match entry.value {
                            TreeValue::File { id, size_bytes, .. } => Some((
                                ObjectKey {
                                    kind: Kind::File,
                                    id: ObjectId(id.0),
                                },
                                size_bytes,
                                false,
                            )),
                            TreeValue::Symlink { id, target_bytes } => Some((
                                ObjectKey {
                                    kind: Kind::Symlink,
                                    id: ObjectId(id.0),
                                },
                                target_bytes,
                                true,
                            )),
                            TreeValue::Tree(_) => None,
                        };
                        if let Some((child, expected_size, symlink)) = expected {
                            match self.descriptor(repo, child)? {
                                Some(actual) => {
                                    let size = if symlink {
                                        match self.metadata(repo, actual)? {
                                            Object::Symlink(target) => target.len() as u64,
                                            _ => return Err(Error::Corrupt("symlink type")),
                                        }
                                    } else {
                                        actual.size_bytes
                                    };
                                    if expected_size != size {
                                        return Err(Error::Invalid("tree child byte length"));
                                    }
                                }
                                None if strict => return Err(Error::MissingObject(child)),
                                None => {}
                            }
                        }
                    }
                }
                Object::Commit(commit) if strict => {
                    // Every commit, including ancestors/legacy predecessors, and
                    // EVERY positive and negative merge term supplies a root.
                    for id in &commit.root_tree.terms {
                        tree_roots.insert(*id);
                        if tree_roots.len() > self.config.max_path_contexts {
                            return Err(Error::Limit("tree path contexts"));
                        }
                    }
                }
                _ => {}
            }
            for child in object.references() {
                enqueue(
                    child,
                    &mut discovered,
                    &mut pending,
                    &mut budget,
                    self.config.max_closure_objects,
                )?;
            }
        }
        if strict {
            self.copy_paths(repo, tree_roots, &mut budget)?;
        }
        Ok(discovered)
    }

    fn metadata(&self, repo: &str, descriptor: Descriptor) -> Result<Object> {
        if descriptor.key.kind == Kind::File {
            return Err(Error::Invalid("file is not metadata"));
        }
        let mut file = File::open(self.path(repo, descriptor.key))?;
        verify_stream(&mut file, descriptor, self.config.model_limits)?;
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(tinytree_model::decode(&bytes, self.config.model_limits)?)
    }

    fn copy_paths(&self, repo: &str, roots: HashSet<TreeId>, budget: &mut Budget) -> Result<()> {
        let mut seen: HashSet<(TreeId, Arc<str>)> = HashSet::new();
        let mut pending = Vec::new();
        let mut context_bytes = 0usize;
        for id in roots {
            self.enqueue_context(
                id,
                Arc::from(""),
                &mut seen,
                &mut pending,
                &mut context_bytes,
            )?;
        }
        while let Some((id, prefix)) = pending.pop() {
            let key = ObjectKey {
                kind: Kind::Tree,
                id: ObjectId(id.0),
            };
            let descriptor = self
                .descriptor(repo, key)?
                .ok_or(Error::MissingObject(key))?;
            let Object::Tree(tree) = self.metadata(repo, descriptor)? else {
                return Err(Error::Corrupt("tree context type"));
            };
            for entry in tree.entries {
                budget.edge()?;
                let length = prefix
                    .len()
                    .checked_add(usize::from(!prefix.is_empty()))
                    .and_then(|n| n.checked_add(entry.name.len()))
                    .ok_or(Error::Limit("tree path length"))?;
                if length > self.config.model_limits.max_path_bytes {
                    return Err(Error::Limit("tree path length"));
                }
                let needs_path = matches!(
                    entry.value,
                    TreeValue::Tree(_)
                        | TreeValue::File {
                            copy_id: Some(_),
                            ..
                        }
                );
                if !needs_path {
                    continue;
                }
                let path = if prefix.is_empty() {
                    entry.name
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                match entry.value {
                    TreeValue::Tree(child) => self.enqueue_context(
                        child,
                        Arc::from(path),
                        &mut seen,
                        &mut pending,
                        &mut context_bytes,
                    )?,
                    TreeValue::File {
                        copy_id: Some(copy),
                        ..
                    } => {
                        budget.edge()?;
                        let key = ObjectKey {
                            kind: Kind::CopyHistory,
                            id: ObjectId(copy.0),
                        };
                        let descriptor = self
                            .descriptor(repo, key)?
                            .ok_or(Error::MissingObject(key))?;
                        let Object::CopyHistory(history) = self.metadata(repo, descriptor)? else {
                            return Err(Error::Corrupt("copy history type"));
                        };
                        if history.current_path != path {
                            return Err(Error::Invalid("copy history current path"));
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn enqueue_context(
        &self,
        id: TreeId,
        path: Arc<str>,
        seen: &mut HashSet<(TreeId, Arc<str>)>,
        pending: &mut Vec<(TreeId, Arc<str>)>,
        bytes: &mut usize,
    ) -> Result<()> {
        let context = (id, path);
        if !seen.contains(&context) {
            if seen.len() >= self.config.max_path_contexts {
                return Err(Error::Limit("tree path contexts"));
            }
            *bytes = bytes
                .checked_add(context.1.len())
                .ok_or(Error::Limit("tree path context bytes"))?;
            if *bytes > self.config.max_path_context_bytes {
                return Err(Error::Limit("tree path context bytes"));
            }
            seen.insert(context.clone());
            pending.push(context);
        }
        Ok(())
    }
}
