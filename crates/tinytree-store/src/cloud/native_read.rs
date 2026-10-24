//! Exact retained-version roots and real canonical edges, not generic CAS grants.
use super::*;
use tinytree_cloud::IndexRootId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudVersionSelector {
    pub workspace: [u8; 32],
    pub generation: u64,
    pub manifest: tinytree_cloud::ManifestId,
    pub selected: BlobDigest,
}
#[derive(Clone, Copy, Debug)]
pub struct CloudNativeReadLimits {
    pub max_path_steps: usize,
    pub max_metadata_bytes: u64,
    pub max_edges: usize,
    pub deadline: Option<std::time::Instant>,
}
impl Default for CloudNativeReadLimits {
    fn default() -> Self {
        Self {
            max_path_steps: 256,
            max_metadata_bytes: 64 * 1024 * 1024,
            max_edges: 1_000_000,
            deadline: None,
        }
    }
}
impl CloudNativeReadLimits {
    fn check_deadline(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(Error::Limit("cloud native read deadline"));
        }
        Ok(())
    }
}
pub struct CloudNativeReadLease {
    // Payload/physical read drops before the retained-version guard.
    lease: ReadLease,
    _version: CloudVersionReadGuard,
}
impl CloudNativeReadLease {
    pub fn descriptor(&self) -> Descriptor {
        self.lease.descriptor()
    }
}
impl Read for CloudNativeReadLease {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.lease.read(bytes)
    }
}
impl Store {
    /// Reacquire and validate the retained version, then prove exactly the
    /// supplied real-edge path. Every returned byte remains protected by the
    /// same version pin; no reference/hold or durable server grant is created.
    pub fn read_cloud_native(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: CloudVersionSelector,
        requested: ObjectKey,
        root: ObjectKey,
        path: &[ObjectKey],
        limits: CloudNativeReadLimits,
    ) -> Result<CloudNativeReadLease> {
        if selector.generation == 0
            || limits.max_path_steps == 0
            || limits.max_metadata_bytes == 0
            || limits.max_edges == 0
        {
            return Err(Error::Invalid("cloud native read bounds"));
        }
        if path.len()
            > limits
                .max_path_steps
                .min(self.inner.config.max_closure_objects)
        {
            return Err(Error::Limit("cloud native proof path"));
        }
        limits.check_deadline()?;
        let version = self.read_cloud_version(
            repo,
            owner,
            selector.workspace,
            selector.generation,
            self.inner.config.cloud.manifest_limits,
        )?;
        if version.version().manifest != selector.manifest
            || version.manifest_bytes().len() as u64 != selector.selected.size_bytes
            || blake3::hash(version.manifest_bytes()).as_bytes() != &selector.selected.blake3
        {
            return Err(Error::Invalid("cloud native version selection"));
        }
        if !version.native_roots().contains(&root) {
            return Err(Error::Invalid("cloud native root is outside version"));
        }
        self.read_cloud_native_path(repo, requested, root, path, limits, version)
    }
    /// Prove membership in only the selected immutable snapshots during the
    /// existing version validation pass, then follow the same real-edge path.
    pub fn read_cloud_snapshot_native(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: CloudVersionSelector,
        requested: ObjectKey,
        selected: &[IndexRootId],
        terminal: CommitId,
        path: &[ObjectKey],
        limits: CloudNativeReadLimits,
    ) -> Result<CloudNativeReadLease> {
        if selector.generation == 0
            || limits.max_path_steps == 0
            || limits.max_metadata_bytes == 0
            || limits.max_edges == 0
        {
            return Err(Error::Invalid("cloud native read bounds"));
        }
        if path.len()
            > limits
                .max_path_steps
                .min(self.inner.config.max_closure_objects)
            || selected.len()
                > limits
                    .max_path_steps
                    .min(self.inner.config.cloud.index_artifacts.max_snapshots)
        {
            return Err(Error::Limit("cloud native proof selection or path"));
        }
        limits.check_deadline()?;
        let version = self.read_cloud_snapshot_commit_version(
            repo,
            owner,
            selector,
            selected,
            terminal,
            limits.deadline,
        )?;
        limits.check_deadline()?;
        self.read_cloud_native_path(
            repo,
            requested,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(terminal.0),
            },
            path,
            limits,
            version,
        )
    }
    fn read_cloud_native_path(
        &self,
        repo: &str,
        requested: ObjectKey,
        root: ObjectKey,
        path: &[ObjectKey],
        limits: CloudNativeReadLimits,
        version: CloudVersionReadGuard,
    ) -> Result<CloudNativeReadLease> {
        let mut current = root;
        let mut metadata = 0u64;
        let mut edges = 0usize;
        for next in path {
            limits.check_deadline()?;
            if current.kind == Kind::File
                || (current.kind == Kind::Commit && current.id.0 == [0; 32])
            {
                return Err(Error::Invalid("cloud native proof has no outgoing edge"));
            }
            let descriptor = self
                .descriptor(repo, current)?
                .ok_or(Error::MissingObject(current))?;
            metadata = metadata
                .checked_add(descriptor.size_bytes)
                .filter(|n| {
                    *n <= limits
                        .max_metadata_bytes
                        .min(self.inner.config.cloud.max_validation_payload_bytes)
                })
                .ok_or(Error::Limit("cloud native proof metadata bytes"))?;
            let (_, bytes) = self.read_metadata(repo, current)?;
            let object = tinytree_model::decode(&bytes, self.inner.config.model_limits)?;
            // Enumerate borrowed canonical fields: no unbounded reference vector
            // is allocated before enforcing aggregate edge work.
            let mut found = false;
            let mut visit = |kind, id| -> Result<()> {
                edges = edges
                    .checked_add(1)
                    .filter(|n| *n <= limits.max_edges.min(self.inner.config.max_closure_edges))
                    .ok_or(Error::Limit("cloud native proof edges"))?;
                if edges % 256 == 0 {
                    limits.check_deadline()?;
                }
                if (ObjectKey {
                    kind,
                    id: ObjectId(id),
                }) == *next
                {
                    found = true;
                }
                Ok(())
            };
            match &object {
                tinytree_model::Object::Commit(value) => {
                    for id in value.parents.iter().chain(&value.predecessors) {
                        if *id != tinytree_model::CommitId::ROOT {
                            visit(Kind::Commit, id.0)?;
                        }
                    }
                    for id in &value.root_tree.terms {
                        visit(Kind::Tree, id.0)?;
                    }
                }
                tinytree_model::Object::Tree(value) => {
                    for entry in &value.entries {
                        match entry.value {
                            tinytree_model::TreeValue::File { id, copy_id, .. } => {
                                visit(Kind::File, id.0)?;
                                if let Some(id) = copy_id {
                                    visit(Kind::CopyHistory, id.0)?;
                                }
                            }
                            tinytree_model::TreeValue::Symlink { id, .. } => {
                                visit(Kind::Symlink, id.0)?
                            }
                            tinytree_model::TreeValue::Tree(id) => visit(Kind::Tree, id.0)?,
                        }
                    }
                }
                tinytree_model::Object::CopyHistory(value) => {
                    for id in &value.parents {
                        visit(Kind::CopyHistory, id.0)?;
                    }
                }
                tinytree_model::Object::Symlink(_) => {}
            }
            if !found {
                return Err(Error::Invalid("cloud native proof edge is not canonical"));
            }
            current = *next;
        }
        if current != requested || (current.kind == Kind::Commit && current.id.0 == [0; 32]) {
            return Err(Error::Invalid("cloud native proof endpoint"));
        }
        limits.check_deadline()?;
        if requested.kind != Kind::File {
            let descriptor = self
                .descriptor(repo, requested)?
                .ok_or(Error::MissingObject(requested))?;
            metadata
                .checked_add(descriptor.size_bytes)
                .filter(|n| {
                    *n <= limits
                        .max_metadata_bytes
                        .min(self.inner.config.cloud.max_validation_payload_bytes)
                })
                .ok_or(Error::Limit("cloud native proof metadata bytes"))?;
        }
        let lease = self.read(repo, requested)?;
        Ok(CloudNativeReadLease {
            lease,
            _version: version,
        })
    }
}
