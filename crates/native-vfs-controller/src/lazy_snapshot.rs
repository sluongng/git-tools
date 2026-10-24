//! Structural snapshots: a delta is applied to canonical directory links, never
//! interpreted as a recursively complete namespace inventory.
use super::*;
use futures::{AsyncRead, AsyncReadExt};
use jj_lib::{repo_path::RepoPathBuf, working_copy::UntrackedReason};
use output_core::{NodeKind, SourceBaselineUpdate, SourceDeltaEntry, SourceNamespaceChange};
use std::{
    collections::{BTreeSet, HashMap},
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

struct Directory {
    entries: BTreeMap<String, m::TreeValue>,
    original: Option<m::TreeId>,
}
struct Graph<'a> {
    controller: &'a Controller,
    snapshot: Arc<SourceDeltaSnapshot>,
    scope: &'a ActionScope,
    directories: BTreeMap<String, Directory>,
    metadata: usize,
    entries: usize,
    objects: HashMap<m::ObjectKey, m::Object>,
    uploads: Vec<(u64, u64, m::FileId)>,
    targets: BTreeMap<String, String>,
    baseline_values: BTreeMap<String, m::TreeValue>,
}
fn split(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}
fn key(kind: m::Kind, bytes: [u8; 32]) -> m::ObjectKey {
    m::ObjectKey {
        kind,
        id: m::ObjectId(bytes),
    }
}
fn file_id(hash: &str) -> Result<m::FileId> {
    Ok(m::FileId(*blake3::Hash::from_hex(hash)?.as_bytes()))
}
impl<'a> Graph<'a> {
    fn owner(&self) -> Arc<dyn std::any::Any + Send + Sync> {
        Arc::new((
            self.controller.inner.clone(),
            self.snapshot.clone(),
            self.scope.owner(),
        ))
    }
    async fn read(&mut self, key: m::ObjectKey) -> Result<m::Object> {
        self.scope.check()?;
        if let Some(object) = self.objects.get(&key) {
            return Ok(object.clone());
        }
        let result = self
            .controller
            .inner
            .provider
            .read_metadata_with_owner(key, self.owner())
            .await;
        self.scope.check()?;
        let verified = result?;
        ensure!(
            verified.descriptor().key == key,
            "native lazy metadata identity differs"
        );
        self.metadata = self
            .metadata
            .checked_add(verified.bytes().len())
            .context("lazy snapshot byte overflow")?;
        ensure!(
            self.metadata
                <= self
                    .controller
                    .inner
                    .config
                    .source_limits
                    .max_metadata_bytes,
            "lazy snapshot metadata byte limit exceeded"
        );
        Ok(verified.into_object())
    }
    async fn load(&mut self, path: &str, id: m::TreeId) -> Result<()> {
        if self.directories.contains_key(path) {
            return Ok(());
        }
        let m::Object::Tree(tree) = self.read(key(m::Kind::Tree, id.0)).await? else {
            bail!("native lazy directory type mismatch")
        };
        self.entries = self
            .entries
            .checked_add(tree.entries.len())
            .context("lazy snapshot entry overflow")?;
        ensure!(
            self.entries <= self.controller.inner.config.source_limits.max_entries,
            "lazy snapshot entry limit exceeded"
        );
        self.directories.insert(
            path.into(),
            Directory {
                entries: tree
                    .entries
                    .into_iter()
                    .map(|entry| (entry.name, entry.value))
                    .collect(),
                original: Some(id),
            },
        );
        Ok(())
    }
    async fn directory(&mut self, path: &str) -> Result<()> {
        self.load("", m::TreeId(self.snapshot.baseline_tree()))
            .await?;
        let mut current = String::new();
        for name in path.split('/').filter(|name| !name.is_empty()) {
            let value = self.directories[&current]
                .entries
                .get(name)
                .context("lazy snapshot parent is missing")?;
            let m::TreeValue::Tree(id) = value else {
                bail!("lazy snapshot ancestor is not a directory")
            };
            let id = *id;
            current = if current.is_empty() {
                name.into()
            } else {
                format!("{current}/{name}")
            };
            self.load(&current, id).await?;
        }
        Ok(())
    }
    async fn lookup(&mut self, path: &str) -> Result<Option<m::TreeValue>> {
        let (parent, name) = split(path);
        self.directory(parent).await?;
        Ok(self.directories[parent].entries.get(name).cloned())
    }
    fn retain_object(&mut self, object: m::Object) -> Result<m::Descriptor> {
        let encoded = m::encode(&object, m::Limits::default())?;
        self.metadata = self
            .metadata
            .checked_add(encoded.bytes.len())
            .context("lazy snapshot byte overflow")?;
        ensure!(
            self.metadata
                <= self
                    .controller
                    .inner
                    .config
                    .source_limits
                    .max_metadata_bytes,
            "lazy snapshot write byte budget exceeded"
        );
        self.objects.insert(encoded.descriptor.key, object);
        Ok(encoded.descriptor)
    }
    async fn renamed(&mut self, value: m::TreeValue, from: &str, to: &str) -> Result<m::TreeValue> {
        if let m::TreeValue::File {
            id,
            executable,
            size_bytes,
            copy_id: Some(copy_id),
        } = value
        {
            let m::Object::CopyHistory(copy) =
                self.read(key(m::Kind::CopyHistory, copy_id.0)).await?
            else {
                bail!("native copy type mismatch")
            };
            ensure!(
                copy.current_path == from,
                "native rename copy provenance differs"
            );
            let descriptor = self.retain_object(m::Object::CopyHistory(m::CopyHistory {
                current_path: to.into(),
                parents: vec![copy_id],
                salt: Vec::new(),
            }))?;
            Ok(m::TreeValue::File {
                id,
                executable,
                size_bytes,
                copy_id: Some(m::CopyId(descriptor.key.id.0)),
            })
        } else {
            Ok(value)
        }
    }
    /// Tracking is path-based, not inode-based: unlink/recreate of a tracked
    /// leaf must keep both its tracking decision and existing copy lineage.
    async fn classify_baseline(&mut self) -> Result<()> {
        let paths: Vec<_> = self
            .snapshot
            .entries()
            .iter()
            .filter(|entry| entry.node.kind != NodeKind::Directory)
            .map(|entry| entry.path.clone())
            .collect();
        if paths.is_empty() {
            return Ok(());
        }
        self.load("", m::TreeId(self.snapshot.baseline_tree()))
            .await?;
        for path in paths {
            let (parent, name) = split(&path);
            let mut current = String::new();
            let mut exists = true;
            for component in parent.split('/').filter(|component| !component.is_empty()) {
                let id = match self.directories[&current].entries.get(component) {
                    Some(m::TreeValue::Tree(id)) => *id,
                    _ => {
                        exists = false;
                        break;
                    }
                };
                current = if current.is_empty() {
                    component.to_owned()
                } else {
                    format!("{current}/{component}")
                };
                self.load(&current, id).await?;
            }
            if exists {
                if let Some(value) = self.directories[&current]
                    .entries
                    .get(name)
                    .filter(|value| !matches!(value, m::TreeValue::Tree(_)))
                    .cloned()
                {
                    self.baseline_values.insert(path, value);
                }
            }
        }
        Ok(())
    }
    /// Excluded local directories survive ACK without their consumed mkdir
    /// journal. Reconstruct their skeleton from final dirty directory entries.
    async fn local_directories(&mut self) -> Result<()> {
        let mut paths: Vec<_> = self
            .snapshot
            .entries()
            .iter()
            .filter(|entry| entry.node.kind == NodeKind::Directory && entry.origin.is_none())
            .map(|entry| entry.path.clone())
            .collect();
        paths.sort_by_key(|path| path.split('/').count());
        for path in paths {
            let (parent, name) = split(&path);
            self.directory(parent).await?;
            if self.directories[parent].entries.contains_key(name) {
                continue;
            }
            let empty = m::TreeId(
                m::encode(&m::Object::Tree(m::Tree::default()), m::Limits::default())?
                    .descriptor
                    .key
                    .id
                    .0,
            );
            self.directories
                .get_mut(parent)
                .unwrap()
                .entries
                .insert(name.into(), m::TreeValue::Tree(empty));
            self.directories.insert(
                path,
                Directory {
                    entries: BTreeMap::new(),
                    original: None,
                },
            );
        }
        Ok(())
    }
    async fn journal(&mut self) -> Result<()> {
        for change in self.snapshot.changes().to_vec() {
            self.scope.check()?;
            match change {
                SourceNamespaceChange::MakeDirectory { path } => {
                    let (parent, name) = split(&path);
                    self.directory(parent).await?;
                    // A new directory remains a temporary structural node. Empty
                    // new directories are pruned, matching jj tracking semantics.
                    let empty = m::TreeId(
                        m::encode(&m::Object::Tree(m::Tree::default()), m::Limits::default())?
                            .descriptor
                            .key
                            .id
                            .0,
                    );
                    self.directories
                        .get_mut(parent)
                        .unwrap()
                        .entries
                        .insert(name.into(), m::TreeValue::Tree(empty));
                    self.directories.insert(
                        path,
                        Directory {
                            entries: BTreeMap::new(),
                            original: None,
                        },
                    );
                }
                SourceNamespaceChange::Remove { path, origin, .. } => {
                    let (parent, name) = split(&path);
                    self.directory(parent).await?;
                    let removed = self
                        .directories
                        .get_mut(parent)
                        .unwrap()
                        .entries
                        .remove(name);
                    ensure!(
                        origin.is_none() || removed.is_some(),
                        "native deletion lacks baseline entry"
                    );
                    self.directories
                        .retain(|p, _| p != &path && !p.starts_with(&format!("{path}/")));
                }
                SourceNamespaceChange::Move { from, to, origin } => {
                    let (from_parent, from_name) = split(&from);
                    self.directory(from_parent).await?;
                    let value = self
                        .directories
                        .get_mut(from_parent)
                        .unwrap()
                        .entries
                        .remove(from_name);
                    if value.is_none() {
                        ensure!(origin.is_none(), "native rename source missing");
                        continue;
                    }
                    let value = value.unwrap();
                    if matches!(value, m::TreeValue::Tree(_)) && origin.is_some() {
                        return Err(LazyMutationUnsupported::DirectoryRename.into());
                    }
                    let value = self.renamed(value, &from, &to).await?;
                    let (parent, name) = split(&to);
                    self.directory(parent).await?;
                    self.directories
                        .get_mut(parent)
                        .unwrap()
                        .entries
                        .insert(name.into(), value);
                    // A local structural directory can contain an explicitly
                    // moved native file. Rewrite its known descendants too;
                    // unopened native subtrees are rejected above.
                    let descendants: Vec<_> = self
                        .directories
                        .iter()
                        .filter(|(path, _)| *path == &from || path.starts_with(&format!("{from}/")))
                        .flat_map(|(path, dir)| {
                            dir.entries
                                .iter()
                                .filter(|(_, value)| {
                                    matches!(
                                        value,
                                        m::TreeValue::File {
                                            copy_id: Some(_),
                                            ..
                                        }
                                    )
                                })
                                .map(move |(name, value)| {
                                    (path.clone(), name.clone(), value.clone())
                                })
                        })
                        .collect();
                    for (path, name, value) in descendants {
                        let previous = format!("{path}/{name}");
                        let next = format!("{to}{}", &previous[from.len()..]);
                        let value = self.renamed(value, &previous, &next).await?;
                        self.directories
                            .get_mut(&path)
                            .unwrap()
                            .entries
                            .insert(name, value);
                    }
                    let moved: Vec<_> = self
                        .directories
                        .keys()
                        .filter(|p| *p == &from || p.starts_with(&format!("{from}/")))
                        .cloned()
                        .collect();
                    for path in moved {
                        let directory = self.directories.remove(&path).unwrap();
                        self.directories
                            .insert(format!("{to}{}", &path[from.len()..]), directory);
                    }
                }
            }
        }
        Ok(())
    }
    fn hash_local(&self, entry: &SourceDeltaEntry) -> Result<m::FileId> {
        let mut hash = blake3::Hasher::new();
        let mut offset = 0;
        while offset < entry.node.size {
            self.scope.check()?;
            let bytes = self.snapshot.read_local(
                entry.node.id,
                offset,
                (entry.node.size - offset).min(self.controller.inner.config.io_buffer_bytes as u64)
                    as usize,
            )?;
            ensure!(!bytes.is_empty(), "lazy snapshot file truncated");
            hash.update(&bytes);
            offset += bytes.len() as u64;
        }
        ensure!(
            self.snapshot
                .read_local(entry.node.id, offset, 1)?
                .is_empty(),
            "lazy snapshot file grew"
        );
        Ok(m::FileId(*hash.finalize().as_bytes()))
    }
    async fn file_bytes(&mut self, path: &str, remaining: &mut usize) -> Result<Option<Vec<u8>>> {
        let delta = self
            .snapshot
            .entries()
            .iter()
            .find(|entry| entry.path == path)
            .cloned();
        let value = if let Some(entry) = delta {
            if entry.node.kind != NodeKind::File {
                return Ok(None);
            }
            if entry.node.remote.is_none() {
                let size = usize::try_from(entry.node.size)?;
                ensure!(size <= *remaining, "ignore file byte budget exceeded");
                *remaining -= size;
                let bytes = self.snapshot.read_local(entry.node.id, 0, size)?;
                ensure!(bytes.len() == size, "local ignore file truncated");
                return Ok(Some(bytes));
            }
            let artifact = entry.node.remote.as_ref().unwrap();
            ensure!(
                artifact.backend == self.controller.inner.host.backend()
                    && artifact.digest_function == 9
                    && artifact.size == entry.node.size,
                "ignore file artifact differs"
            );
            Some(m::TreeValue::File {
                id: file_id(&artifact.hash)?,
                executable: false,
                size_bytes: artifact.size,
                copy_id: None,
            })
        } else {
            self.lookup(path).await?
        };
        let Some(m::TreeValue::File { id, size_bytes, .. }) = value else {
            return Ok(None);
        };
        let size = usize::try_from(size_bytes)?;
        ensure!(size <= *remaining, "ignore file byte budget exceeded");
        *remaining -= size;
        let result = if let Some(owner) = self.scope.owner() {
            self.controller
                .inner
                .provider
                .read_file_with_owner(id, owner)
                .await
        } else {
            self.controller.inner.provider.read_file(id).await
        };
        self.scope.check()?;
        let (descriptor, reader) = result?.into_parts();
        ensure!(
            descriptor.key == key(m::Kind::File, id.0) && descriptor.size_bytes == size_bytes,
            "ignore file descriptor differs"
        );
        let mut bytes = Vec::new();
        reader.take(size_bytes + 1).read_to_end(&mut bytes).await?;
        self.scope.check()?;
        ensure!(
            bytes.len() == size && blake3::hash(&bytes).as_bytes() == &id.0,
            "ignore file payload differs"
        );
        Ok(Some(bytes))
    }
    async fn select(&mut self, policy: &dyn SnapshotPolicy) -> Result<SnapshotStats> {
        let entries: Vec<_> = self
            .snapshot
            .entries()
            .iter()
            .filter(|e| e.node.kind != NodeKind::Directory)
            .cloned()
            .collect();
        let mut candidates = Vec::new();
        let mut directories = BTreeSet::new();
        for entry in &entries {
            let tracked = entry.origin.is_some() || self.baseline_values.contains_key(&entry.path);
            candidates.push(PolicyCandidate {
                path: entry.path.clone(),
                kind: if entry.node.kind == NodeKind::File {
                    PolicyKind::File
                } else {
                    PolicyKind::Symlink
                },
                size: entry.node.size,
                tracked,
            });
            if !tracked {
                let mut parent = split(&entry.path).0;
                loop {
                    directories.insert(parent.to_owned());
                    if parent.is_empty() {
                        break;
                    }
                    parent = split(parent).0;
                }
            }
        }
        let mut ignore_files = Vec::new();
        let mut remaining = self.controller.inner.config.max_ignore_bytes;
        for directory in directories {
            let path = if directory.is_empty() {
                ".gitignore".into()
            } else {
                format!("{directory}/.gitignore")
            };
            if let Some(contents) = self.file_bytes(&path, &mut remaining).await? {
                ignore_files.push(PolicyIgnore {
                    directory,
                    contents,
                });
            }
        }
        let inventory = PolicyInventory {
            candidates,
            ignore_files,
        };
        ensure!(
            serde_json::to_vec(&inventory)?.len()
                <= self
                    .controller
                    .inner
                    .config
                    .source_limits
                    .max_metadata_bytes,
            "lazy policy inventory byte budget exceeded"
        );
        let decisions = tokio::time::timeout_at(self.scope.deadline(), policy.evaluate(&inventory))
            .await
            .map_err(|_| ActionScopeError::Deadline)??;
        self.scope.check()?;
        ensure!(
            decisions.len() == entries.len(),
            "lazy snapshot policy returned incomplete decisions"
        );
        let mut stats = SnapshotStats::default();
        let mut local_bytes = 0u64;
        for ((entry, candidate), decision) in
            entries.iter().zip(&inventory.candidates).zip(decisions)
        {
            ensure!(
                !candidate.tracked || decision == PolicyDecision::Track,
                "snapshot policy omitted tracked path"
            );
            let path = RepoPathBuf::from_internal_string(entry.path.clone())?;
            match decision {
                PolicyDecision::Ignored => continue,
                PolicyDecision::NotAutoTracked => {
                    stats
                        .untracked_paths
                        .insert(path, UntrackedReason::FileNotAutoTracked);
                    continue;
                }
                PolicyDecision::TooLarge { max_size } => {
                    ensure!(entry.node.size > max_size, "invalid policy size rejection");
                    stats.untracked_paths.insert(
                        path,
                        UntrackedReason::FileTooLarge {
                            size: entry.node.size,
                            max_size,
                        },
                    );
                    continue;
                }
                PolicyDecision::Track => {}
            }
            let (parent, name) = split(&entry.path);
            self.directory(parent).await?;
            let value = if entry.node.kind == NodeKind::Symlink {
                let target = entry
                    .target
                    .clone()
                    .context("lazy symlink target missing")?;
                ensure!(
                    !target.is_empty()
                        && !target.contains('\0')
                        && target.len()
                            <= self.controller.inner.config.source_limits.max_path_bytes,
                    "invalid lazy symlink target"
                );
                let descriptor = self.retain_object(m::Object::Symlink(target.clone()))?;
                self.targets.insert(entry.path.clone(), target.clone());
                m::TreeValue::Symlink {
                    id: m::SymlinkId(descriptor.key.id.0),
                    target_bytes: target.len() as u64,
                }
            } else {
                ensure!(
                    entry.node.size <= self.controller.inner.config.max_file_bytes,
                    "lazy file byte limit exceeded"
                );
                let id = if let Some(artifact) = &entry.node.remote {
                    ensure!(
                        artifact.backend == self.controller.inner.host.backend()
                            && artifact.digest_function == 9
                            && artifact.size == entry.node.size,
                        "lazy file artifact differs"
                    );
                    file_id(&artifact.hash)?
                } else {
                    local_bytes = local_bytes
                        .checked_add(entry.node.size)
                        .context("lazy upload bytes overflow")?;
                    ensure!(
                        local_bytes <= self.controller.inner.config.max_snapshot_bytes,
                        "lazy snapshot byte limit exceeded"
                    );
                    let id = self.hash_local(entry)?;
                    let unchanged = matches!(self.directories[parent].entries.get(name), Some(m::TreeValue::File { id: old, size_bytes, .. }) if *old == id && *size_bytes == entry.node.size);
                    if !unchanged {
                        self.uploads.push((entry.node.id, entry.node.size, id));
                    }
                    id
                };
                // Journal replay already rewrites copy lineage for renamed files.
                let copy_id = match self.directories[parent].entries.get(name) {
                    Some(m::TreeValue::File { copy_id, .. }) => *copy_id,
                    _ => match self.baseline_values.get(&entry.path) {
                        Some(m::TreeValue::File { copy_id, .. }) => *copy_id,
                        _ => None,
                    },
                };
                m::TreeValue::File {
                    id,
                    executable: entry.node.mode & 0o111 != 0,
                    size_bytes: entry.node.size,
                    copy_id,
                }
            };
            self.directories
                .get_mut(parent)
                .unwrap()
                .entries
                .insert(name.into(), value);
        }
        Ok(stats)
    }
    async fn write(mut self) -> Result<(m::TreeId, Vec<SourceBaselineUpdate>)> {
        for (inode, size, expected) in self.uploads.clone() {
            self.scope.check()?;
            let mut reader = Reader {
                snapshot: self.snapshot.clone(),
                owner: self.controller.inner.clone(),
                inode,
                size,
                offset: 0,
                buffer: self.controller.inner.config.io_buffer_bytes,
            };
            let result = if let Some(owner) = self.scope.owner() {
                self.controller
                    .inner
                    .provider
                    .write_file_with_owner(&mut reader, size, owner)
                    .await
            } else {
                self.controller
                    .inner
                    .provider
                    .write_file(&mut reader, size)
                    .await
            };
            self.scope.check()?;
            ensure!(
                result? == expected && reader.offset == size,
                "lazy upload identity differs"
            );
        }
        // CopyHistory parents referenced here are existing canonical objects;
        // generated rename records are written in journal insertion dependency order.
        let mut pending = self.objects.clone();
        while !pending.is_empty() {
            let ready: Vec<_> = pending
                .iter()
                .filter(|(_, object)| {
                    object
                        .references()
                        .iter()
                        .all(|key| !pending.contains_key(key))
                })
                .map(|(key, _)| *key)
                .collect();
            ensure!(!ready.is_empty(), "generated lazy metadata cycle");
            for key in ready {
                let object = pending.remove(&key).unwrap();
                self.write_object(object, key).await?;
            }
        }
        let mut paths: Vec<_> = self.directories.keys().cloned().collect();
        paths.sort_by_key(|p| std::cmp::Reverse(p.split('/').filter(|p| !p.is_empty()).count()));
        let mut rebuilt = BTreeMap::new();
        for path in paths {
            self.scope.check()?;
            let directory = self.directories.remove(&path).unwrap();
            if !path.is_empty() && directory.original.is_none() && directory.entries.is_empty() {
                let (parent, name) = split(&path);
                if let Some(parent) = self.directories.get_mut(parent) {
                    parent.entries.remove(name);
                }
                continue;
            }
            let object = m::Object::Tree(m::Tree {
                entries: directory
                    .entries
                    .into_iter()
                    .map(|(name, value)| m::TreeEntry { name, value })
                    .collect(),
            });
            let encoded = m::encode(&object, m::Limits::default())?;
            let id = m::TreeId(encoded.descriptor.key.id.0);
            if directory.original != Some(id) {
                self.write_object(object, encoded.descriptor.key).await?;
            }
            rebuilt.insert(path.clone(), id);
            if !path.is_empty() {
                let (parent, name) = split(&path);
                if let Some(parent) = self.directories.get_mut(parent) {
                    parent.entries.insert(name.into(), m::TreeValue::Tree(id));
                }
            }
        }
        let root = rebuilt
            .get("")
            .copied()
            .unwrap_or(m::TreeId(self.snapshot.baseline_tree()));
        let generation = self
            .snapshot
            .version()
            .generation
            .checked_add(1)
            .context("lazy generation exhausted")?;
        let lazy = self.controller.inner.host.compiler().lazy_tree_for_source(
            root,
            generation,
            self.controller.inner.config.source_limits.clone(),
        )?;
        let source = lazy.source_root(self.controller.inner.host.tree().identity());
        let mut updates = Vec::new();
        for (path, id) in rebuilt {
            self.scope.check()?;
            let directory = source
                .provider
                .read_directory(
                    source.pin.clone(),
                    id.0,
                    path.clone(),
                    source.load_limits.clone(),
                    self.owner(),
                )
                .await?;
            self.scope.check()?;
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            };
            let symlink_targets = self
                .targets
                .iter()
                .filter_map(|(p, t)| {
                    p.strip_prefix(&prefix)
                        .filter(|name| !name.contains('/'))
                        .map(|name| (name.to_owned(), t.clone()))
                })
                .collect();
            updates.push(SourceBaselineUpdate {
                path,
                directory,
                symlink_targets,
            });
        }
        Ok((root, updates))
    }
    async fn write_object(&self, object: m::Object, expected: m::ObjectKey) -> Result<()> {
        self.scope.check()?;
        let result = if let Some(owner) = self.scope.owner() {
            self.controller
                .inner
                .provider
                .write_metadata_with_owner(object, owner)
                .await
        } else {
            self.controller.inner.provider.write_metadata(object).await
        };
        self.scope.check()?;
        ensure!(result?.key == expected, "lazy canonical write differs");
        Ok(())
    }
}

pub(super) async fn snapshot(
    mutation: &mut Mutation,
    id: &str,
    policy: &dyn SnapshotPolicy,
    fingerprint: [u8; 32],
    scope: ActionScope,
    snapshot: SourceDeltaSnapshot,
) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
    let snapshot = Arc::new(snapshot);
    let mut graph = Graph {
        controller: &mutation.controller,
        snapshot: snapshot.clone(),
        scope: &scope,
        directories: BTreeMap::new(),
        metadata: 0,
        entries: 0,
        objects: HashMap::new(),
        uploads: Vec::new(),
        targets: BTreeMap::new(),
        baseline_values: BTreeMap::new(),
    };
    graph.classify_baseline().await.map_err(rejected)?;
    graph.journal().await.map_err(rejected)?;
    graph.local_directories().await.map_err(rejected)?;
    let stats = graph.select(policy).await.map_err(rejected)?;
    scope.check().map_err(rejected)?;
    mutation
        .controller
        .prepare(
            id,
            &mutation.current,
            Action::Snapshot {
                policy: fingerprint,
            },
            snapshot.version().clone(),
        )
        .map_err(rejected)?;
    scope.check().map_err(ambiguous)?;
    let (root, updates) = match graph.write().await {
        Ok(result) => result,
        Err(error) if error.downcast_ref::<ActionScopeError>().is_some() => {
            return Err(ambiguous(error));
        }
        Err(error) => return Err(mutation.controller.rejected(id, error)),
    };
    scope.check().map_err(ambiguous)?;
    mutation.controller.target(id, root.0).map_err(ambiguous)?;
    let snapshot = Arc::try_unwrap(snapshot)
        .map_err(|_| ambiguous(anyhow::anyhow!("lazy snapshot still has physical readers")))?;
    let expected = snapshot.version().clone();
    let generation = expected
        .generation
        .checked_add(1)
        .context("source generation exhausted")
        .map_err(ambiguous)?;
    let lazy = mutation
        .controller
        .inner
        .host
        .compiler()
        .lazy_tree_for_source(
            root,
            generation,
            mutation.controller.inner.config.source_limits.clone(),
        )
        .map_err(ambiguous)?;
    scope.check().map_err(ambiguous)?;
    let source = snapshot
        .acknowledge_root(
            lazy.source_root(mutation.controller.inner.host.tree().identity()),
            updates,
            expected,
            id,
            &mutation.controller.inner.config.source_limits,
        )
        .map_err(ambiguous)?;
    let receipt = mutation
        .controller
        .complete(id, source)
        .map_err(ambiguous)?;
    mutation.current = receipt.after.clone();
    scope.check().map_err(ambiguous)?;
    Ok((receipt, stats))
}

async fn checkout_counts(
    controller: &Controller,
    snapshot: Arc<SourceDeltaSnapshot>,
    target: m::TreeId,
    scope: &ActionScope,
) -> Result<CheckoutStats> {
    let mut graph = Graph {
        controller,
        snapshot: snapshot.clone(),
        scope,
        directories: BTreeMap::new(),
        metadata: 0,
        entries: 0,
        objects: HashMap::new(),
        uploads: Vec::new(),
        targets: BTreeMap::new(),
        baseline_values: BTreeMap::new(),
    };
    let mut pending = vec![(
        Some(m::TreeValue::Tree(m::TreeId(snapshot.baseline_tree()))),
        Some(m::TreeValue::Tree(target)),
    )];
    let mut stats = CheckoutStats::default();
    while let Some((before, after)) = pending.pop() {
        scope.check()?;
        if before == after {
            continue;
        }
        if matches!(before, Some(m::TreeValue::Tree(_)))
            || matches!(after, Some(m::TreeValue::Tree(_)))
        {
            let mut sides = Vec::new();
            for value in [before, after] {
                let map = if let Some(m::TreeValue::Tree(id)) = &value {
                    let m::Object::Tree(tree) = graph.read(key(m::Kind::Tree, id.0)).await? else {
                        bail!("checkout directory type mismatch")
                    };
                    graph.entries = graph
                        .entries
                        .checked_add(tree.entries.len())
                        .context("checkout count overflow")?;
                    ensure!(
                        graph.entries <= controller.inner.config.source_limits.max_entries,
                        "checkout metadata entry budget exceeded"
                    );
                    tree.entries
                        .into_iter()
                        .map(|entry| (entry.name, entry.value))
                        .collect::<BTreeMap<_, _>>()
                } else {
                    BTreeMap::new()
                };
                sides.push((
                    value.filter(|value| !matches!(value, m::TreeValue::Tree(_))),
                    map,
                ));
            }
            let (right, after) = sides.pop().unwrap();
            let (left, before) = sides.pop().unwrap();
            // A file replacing a directory is a new leaf at this exact path;
            // the directory's old descendants are removals, and conversely.
            if left.is_some() {
                stats.removed_files = stats
                    .removed_files
                    .checked_add(1)
                    .context("checkout file count overflow")?;
            }
            if right.is_some() {
                stats.added_files = stats
                    .added_files
                    .checked_add(1)
                    .context("checkout file count overflow")?;
            }
            let names: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
            ensure!(
                pending.len().saturating_add(names.len())
                    <= controller.inner.config.source_limits.max_entries,
                "checkout pending entry budget exceeded"
            );
            for name in names {
                pending.push((before.get(&name).cloned(), after.get(&name).cloned()));
            }
        } else {
            match (before, after) {
                (None, Some(_)) => {
                    stats.added_files = stats
                        .added_files
                        .checked_add(1)
                        .context("checkout file count overflow")?
                }
                (Some(_), None) => {
                    stats.removed_files = stats
                        .removed_files
                        .checked_add(1)
                        .context("checkout file count overflow")?
                }
                (Some(before), Some(after)) => {
                    // Copy lineage is not a filesystem content/mode change.
                    let equal = match (&before, &after) {
                        (
                            m::TreeValue::File {
                                id: a,
                                executable: ae,
                                size_bytes: asz,
                                ..
                            },
                            m::TreeValue::File {
                                id: b,
                                executable: be,
                                size_bytes: bsz,
                                ..
                            },
                        ) => a == b && ae == be && asz == bsz,
                        _ => before == after,
                    };
                    if !equal {
                        stats.updated_files = stats
                            .updated_files
                            .checked_add(1)
                            .context("checkout file count overflow")?;
                    }
                }
                (None, None) => {}
            }
        }
    }
    Ok(stats)
}

pub(super) async fn checkout(
    mutation: &mut Mutation,
    id: &str,
    tree: &NativeTree,
    scope: ActionScope,
) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure> {
    let Capture::Lazy(snapshot) = mutation
        .controller
        .inner
        .host
        .capture(
            &mutation.controller.inner.config.source_limits,
            scope.deadline(),
        )
        .await
        .map_err(rejected)?
    else {
        return Err(rejected(anyhow::anyhow!(
            "lazy checkout capture changed type"
        )));
    };
    ensure_clean(&snapshot).map_err(rejected)?;
    let snapshot = Arc::new(snapshot);
    let stats = checkout_counts(
        &mutation.controller,
        snapshot.clone(),
        resolved_merge(tree).map_err(rejected)?.terms[0],
        &scope,
    )
    .await
    .map_err(rejected)?;
    let expected = snapshot.version().clone();
    scope.check().map_err(rejected)?;
    mutation
        .controller
        .prepare(
            id,
            &mutation.current,
            Action::CheckOut { tree: tree.clone() },
            expected.clone(),
        )
        .map_err(rejected)?;
    drop(snapshot);
    scope.check().map_err(ambiguous)?;
    let source = mutation
        .controller
        .inner
        .host
        .publish(
            tree,
            expected,
            id,
            &mutation.controller.inner.config.source_limits,
            Some(&scope),
        )
        .await
        .map_err(ambiguous)?;
    let receipt = mutation
        .controller
        .complete(id, source)
        .map_err(ambiguous)?;
    mutation.current = receipt.after.clone();
    scope.check().map_err(ambiguous)?;
    Ok((receipt, stats))
}
fn ensure_clean(snapshot: &SourceDeltaSnapshot) -> Result<()> {
    ensure!(
        snapshot.changes().is_empty() && snapshot.entries().is_empty(),
        "native checkout would overwrite dirty or untracked paths"
    );
    Ok(())
}

pub(super) async fn reset(
    mutation: &mut Mutation,
    id: &str,
    tree: &NativeTree,
    recover: bool,
    scope: ActionScope,
) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
    let Capture::Lazy(snapshot) = mutation
        .controller
        .inner
        .host
        .capture(
            &mutation.controller.inner.config.source_limits,
            scope.deadline(),
        )
        .await
        .map_err(rejected)?
    else {
        return Err(rejected(anyhow::anyhow!("lazy reset capture changed type")));
    };
    let merge = resolved_merge(tree).map_err(rejected)?;
    if !snapshot.changes().is_empty()
        || !snapshot.entries().is_empty()
        || merge.terms[0].0 != snapshot.baseline_tree()
    {
        return Err(rejected(
            LazyMutationUnsupported::ResetWithDifferentContents,
        ));
    }
    let expected = snapshot.version().clone();
    scope.check().map_err(rejected)?;
    let generation = expected
        .generation
        .checked_add(1)
        .context("source generation exhausted")
        .map_err(rejected)?;
    let lazy = mutation
        .controller
        .inner
        .host
        .compiler()
        .lazy_tree_for_source(
            merge.terms[0],
            generation,
            mutation.controller.inner.config.source_limits.clone(),
        )
        .map_err(rejected)?;
    mutation
        .controller
        .prepare(
            id,
            &mutation.current,
            Action::Reset {
                tree: tree.clone(),
                recover,
            },
            expected.clone(),
        )
        .map_err(rejected)?;
    scope.check().map_err(ambiguous)?;
    let source = snapshot
        .acknowledge_root(
            lazy.source_root(mutation.controller.inner.host.tree().identity()),
            Vec::new(),
            expected,
            id,
            &mutation.controller.inner.config.source_limits,
        )
        .map_err(ambiguous)?;
    let receipt = mutation
        .controller
        .complete(id, source)
        .map_err(ambiguous)?;
    mutation.current = receipt.after.clone();
    scope.check().map_err(ambiguous)?;
    Ok(receipt)
}
struct Reader {
    snapshot: Arc<SourceDeltaSnapshot>,
    owner: Arc<Inner>,
    inode: u64,
    size: u64,
    offset: u64,
    buffer: usize,
}
impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        bytes: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let _owner = &self.owner;
        if self.offset == self.size || bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let count = bytes
            .len()
            .min(self.buffer)
            .min((self.size - self.offset) as usize);
        match self.snapshot.read_local(self.inode, self.offset, count) {
            Ok(data) if data.is_empty() => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "lazy file truncated",
            ))),
            Ok(data) => {
                bytes[..data.len()].copy_from_slice(&data);
                self.offset += data.len() as u64;
                Poll::Ready(Ok(data.len()))
            }
            Err(error) => Poll::Ready(Err(std::io::Error::other(error.to_string()))),
        }
    }
}
