//! Whole-directory verification for a separately installed lazy namespace.
use super::*;
use output_core::lazy_source::{
    LazySourceRoot, SourceDirectoryEntry, SourceDirectoryEntryKind, SourceDirectoryProvider,
    SourceLoadLimits, SourcePin, VerifiedSourceDirectory,
};

/// One immutable source baseline. This identity is not a server retention
/// receipt: the configured object provider owns remote authorization and pins.
#[derive(Clone)]
pub struct LazyWorkspace {
    provider: Arc<DirectoryProvider>,
}
struct DirectoryProvider {
    compiler: Arc<Compiler>,
    root: m::TreeId,
    pin: SourcePin,
    metadata: Mutex<MetadataUse>,
    source_limits: output_core::SourceLimits,
}
#[derive(Default)]
struct MetadataUse {
    sizes: HashMap<m::ObjectKey, u64>,
    bytes: u64,
}

impl Compiler {
    /// Prepare an immutable lazy baseline without enumerating any directories.
    pub fn lazy_tree(self: &Arc<Self>, root: m::TreeId, generation: u64) -> Result<LazyWorkspace> {
        self.lazy_tree_for_source(
            root,
            generation,
            output_core::SourceLimits {
                max_entries: self.limits.max_entries,
                max_metadata_bytes: self.limits.max_metadata_bytes as usize,
                max_path_bytes: self.limits.max_path_bytes,
                max_depth: self.limits.max_depth,
                ..Default::default()
            },
        )
    }

    /// Use the destination namespace limits before authorizing file locators.
    pub fn lazy_tree_for_source(
        self: &Arc<Self>,
        root: m::TreeId,
        generation: u64,
        source_limits: output_core::SourceLimits,
    ) -> Result<LazyWorkspace> {
        ensure!(generation != 0, "invalid lazy source generation");
        Ok(LazyWorkspace {
            provider: Arc::new(DirectoryProvider {
                compiler: self.clone(),
                root,
                pin: SourcePin {
                    root: root.0,
                    generation,
                },
                metadata: Mutex::new(MetadataUse::default()),
                source_limits,
            }),
        })
    }

    /// Reauthorize file locators from an already validated, owner-private source
    /// namespace after restart. This does not fetch file or tree contents.
    pub fn restore_lazy_artifacts(&self, artifacts: &[Artifact]) -> Result<()> {
        ensure!(
            artifacts.len() <= self.limits.max_authorized_files,
            "native restored file count exceeded"
        );
        let mut descriptors = HashMap::new();
        for artifact in artifacts {
            ensure!(
                artifact.backend == self.scope.backend
                    && artifact.instance == self.scope.instance
                    && artifact.digest_function == 9
                    && artifact.size <= self.limits.max_file_bytes,
                "native restored artifact scope mismatch"
            );
            let hash = blake3::Hash::from_hex(&artifact.hash)?;
            let descriptor = m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(*hash.as_bytes()),
                },
                size_bytes: artifact.size,
                format_version: m::FORMAT_VERSION,
            };
            ensure!(
                descriptors
                    .insert(descriptor.key, descriptor)
                    .is_none_or(|old| old == descriptor),
                "conflicting restored native file lengths"
            );
        }
        self.source.register(&descriptors)
    }
}

impl LazyWorkspace {
    pub fn root_tree(&self) -> m::TreeId {
        self.provider.root
    }
    pub fn pin(&self) -> &SourcePin {
        &self.provider.pin
    }
    pub fn source(&self) -> Arc<NativeBlobSource> {
        self.provider.compiler.source()
    }
    pub fn source_root(&self, identity: &str) -> LazySourceRoot {
        let limits = &self.provider.compiler.limits;
        LazySourceRoot {
            identity: identity.to_owned(),
            root_tree: self.provider.root.0,
            pin: self.provider.pin.clone(),
            provider: self.provider.clone(),
            load_limits: SourceLoadLimits {
                max_directory_entries: limits.max_entries,
                max_directory_bytes: limits.max_metadata_object_bytes.max(1024),
                timeout_ms: limits
                    .operation_timeout_seconds
                    .saturating_mul(1000)
                    .min(300_000),
                ..Default::default()
            },
        }
    }
}

impl DirectoryProvider {
    fn check_pin(&self, pin: &SourcePin) -> Result<()> {
        ensure!(*pin == self.pin, "native lazy source pin mismatch");
        Ok(())
    }
    fn reserve_metadata(&self, key: m::ObjectKey, size: u64) -> Result<()> {
        let mut metadata = self.metadata.lock().unwrap();
        if let Some(old) = metadata.sizes.get(&key) {
            ensure!(*old == size, "native metadata descriptor changed");
            return Ok(());
        }
        ensure!(
            metadata.sizes.len() < self.compiler.limits.max_metadata_objects,
            "native lazy metadata object count exceeded"
        );
        let total = metadata.bytes.checked_add(size);
        ensure!(
            total.is_some_and(|n| n <= self.compiler.limits.max_metadata_bytes),
            "native lazy metadata byte count exceeded"
        );
        metadata.bytes = total.unwrap();
        metadata.sizes.insert(key, size);
        Ok(())
    }
    async fn metadata(
        &self,
        key: m::ObjectKey,
        budget: &mut Budget,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<m::Object> {
        // A descriptor lookup can itself hydrate a missing object. Use the
        // owner-bearing read directly so no detached preflight loses its lease.
        let verified = self
            .compiler
            .provider
            .read_metadata_with_owner(key, owner)
            .await?;
        let descriptor = verified.descriptor();
        ensure!(
            descriptor.key == key
                && descriptor.format_version == m::FORMAT_VERSION
                && descriptor.size_bytes <= self.compiler.limits.max_metadata_object_bytes as u64,
            "invalid native lazy metadata descriptor"
        );
        budget.objects = budget
            .objects
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("native lazy metadata count overflow"))?;
        budget.bytes = budget
            .bytes
            .checked_add(descriptor.size_bytes)
            .ok_or_else(|| anyhow::anyhow!("native lazy metadata byte overflow"))?;
        ensure!(
            budget.objects <= self.compiler.limits.max_metadata_objects
                && budget.bytes <= self.compiler.limits.max_metadata_bytes,
            "native lazy metadata budget exceeded"
        );
        m::verify(
            &descriptor,
            verified.bytes(),
            m::Limits {
                max_object_bytes: self.compiler.limits.max_metadata_object_bytes,
                max_items: self.compiler.limits.max_entries,
                max_field_bytes: self
                    .compiler
                    .limits
                    .max_symlink_bytes
                    .max(self.compiler.limits.max_path_bytes),
                max_path_bytes: self.compiler.limits.max_path_bytes,
            },
        )?;
        Ok(verified.into_object())
    }
}

#[async_trait]
impl SourceDirectoryProvider for DirectoryProvider {
    async fn read_directory(
        &self,
        pin: SourcePin,
        tree_id: [u8; 32],
        base_path: String,
        limits: SourceLoadLimits,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<VerifiedSourceDirectory> {
        self.check_pin(&pin)?;
        ensure!(
            base_path.len() <= self.compiler.limits.max_path_bytes
                && (base_path.is_empty()
                    || base_path.split('/').all(|name| {
                        !name.is_empty()
                            && name != "."
                            && name != ".."
                            && !name.contains(['\\', '\0'])
                    })),
            "invalid native lazy base path"
        );
        let depth = if base_path.is_empty() {
            0
        } else {
            base_path.split('/').count()
        };
        ensure!(
            depth <= self.compiler.limits.max_depth,
            "native lazy source depth exceeded"
        );
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.compiler.limits.operation_timeout_seconds);
        tokio::time::timeout_at(deadline, async {
            let _permit = self.compiler.compilation.acquire().await?;
            let key = m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(tree_id),
            };
            let mut budget = Budget::default();
            let object = self.metadata(key, &mut budget, owner.clone()).await?;
            let tree_bytes = budget.bytes;
            ensure!(
                budget.bytes <= limits.max_directory_bytes as u64,
                "native lazy directory bytes exceeded"
            );
            let m::Object::Tree(tree) = object else {
                anyhow::bail!("native lazy directory type mismatch")
            };
            ensure!(
                tree.entries.len() <= limits.max_directory_entries,
                "native lazy directory entry count exceeded"
            );
            let mut entries = Vec::with_capacity(tree.entries.len());
            let mut descriptors = HashMap::new();
            for entry in tree.entries {
                ensure!(
                    !entry.name.is_empty()
                        && entry.name != "."
                        && entry.name != ".."
                        && !entry.name.contains(['/', '\\', '\0'])
                        && entry.name.len() <= self.compiler.limits.max_path_bytes,
                    "invalid native lazy source component"
                );
                let path = if base_path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{base_path}/{}", entry.name)
                };
                ensure!(
                    path.len() <= self.compiler.limits.max_path_bytes,
                    "native lazy source path exceeded"
                );
                let (mode, kind) = match entry.value {
                    m::TreeValue::Tree(id) => {
                        (0o755, SourceDirectoryEntryKind::Directory { tree_id: id.0 })
                    }
                    m::TreeValue::File {
                        id,
                        executable,
                        size_bytes,
                        copy_id,
                    } => {
                        ensure!(
                            size_bytes <= self.compiler.limits.max_file_bytes,
                            "native lazy file exceeds limit"
                        );
                        if let Some(copy_id) = copy_id {
                            let copy_key = m::ObjectKey {
                                kind: m::Kind::CopyHistory,
                                id: m::ObjectId(copy_id.0),
                            };
                            let before = budget.bytes;
                            let copy = self.metadata(copy_key, &mut budget, owner.clone()).await?;
                            let m::Object::CopyHistory(copy) = copy else {
                                anyhow::bail!("native lazy copy type mismatch")
                            };
                            ensure!(
                                copy.current_path == path,
                                "native lazy copy provenance path mismatch"
                            );
                            self.reserve_metadata(copy_key, budget.bytes - before)?;
                        }
                        let descriptor = m::Descriptor {
                            key: m::ObjectKey {
                                kind: m::Kind::File,
                                id: m::ObjectId(id.0),
                            },
                            size_bytes,
                            format_version: m::FORMAT_VERSION,
                        };
                        ensure!(
                            descriptors
                                .insert(descriptor.key, descriptor)
                                .is_none_or(|old| old == descriptor),
                            "conflicting native lazy file lengths"
                        );
                        (
                            if executable { 0o755 } else { 0o644 },
                            SourceDirectoryEntryKind::File {
                                artifact: self.compiler.scope.artifact(descriptor),
                                copy_id: copy_id.map(|id| id.0),
                            },
                        )
                    }
                    m::TreeValue::Symlink { id, target_bytes } => {
                        ensure!(
                            target_bytes <= self.compiler.limits.max_symlink_bytes as u64,
                            "native lazy symlink exceeds limit"
                        );
                        (
                            0o777,
                            SourceDirectoryEntryKind::Symlink {
                                object_id: id.0,
                                target_size: target_bytes,
                            },
                        )
                    }
                };
                entries.push(SourceDirectoryEntry {
                    name: entry.name,
                    mode,
                    kind,
                });
            }
            let directory = VerifiedSourceDirectory { tree_id, entries };
            directory.validate_for_projection(
                tree_id,
                &base_path,
                &self.source_limits,
                &limits,
                self.compiler.scope.backend(),
            )?;
            self.reserve_metadata(key, tree_bytes)?;
            self.compiler.source.register(&descriptors)?;
            Ok(directory)
        })
        .await
        .map_err(|_| output_core::CacheTimeoutError)?
    }

    async fn read_symlink(
        &self,
        pin: SourcePin,
        object_id: [u8; 32],
        expected_size: u64,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<String> {
        self.check_pin(&pin)?;
        ensure!(
            expected_size <= self.compiler.limits.max_symlink_bytes as u64,
            "native lazy symlink exceeds limit"
        );
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.compiler.limits.operation_timeout_seconds);
        tokio::time::timeout_at(deadline, async {
            let _permit = self.compiler.compilation.acquire().await?;
            let key = m::ObjectKey {
                kind: m::Kind::Symlink,
                id: m::ObjectId(object_id),
            };
            let mut budget = Budget::default();
            let object = self.metadata(key, &mut budget, owner.clone()).await?;
            let m::Object::Symlink(target) = object else {
                anyhow::bail!("native lazy symlink type mismatch")
            };
            ensure!(
                target.len() as u64 == expected_size
                    && !target.is_empty()
                    && !target.contains('\0'),
                "native lazy symlink length or target mismatch"
            );
            self.reserve_metadata(key, budget.bytes)?;
            Ok(target)
        })
        .await
        .map_err(|_| output_core::CacheTimeoutError)?
    }
}
