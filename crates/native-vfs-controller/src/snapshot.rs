use super::*;
use futures::{AsyncRead, AsyncReadExt};
use jj_lib::{repo_path::RepoPathBuf, working_copy::UntrackedReason};
use native_workspace::CompiledWorkspace;
use output_core::{NodeKind, SourceContent, SourceSnapshotEntry};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

pub(super) struct Plan {
    pub entries: Vec<Selected>,
    pub stats: SnapshotStats,
}
pub(super) struct Selected {
    path: String,
    value: Value,
}
enum Value {
    File {
        id: m::FileId,
        size: u64,
        executable: bool,
        copy_id: Option<m::CopyId>,
        upload: Option<u64>,
    },
    Symlink {
        target: String,
        write: bool,
    },
    EmptyDirectory(m::TreeId),
}
fn file_id(hash: &str) -> Result<m::FileId> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid native file hash"
    );
    let mut bytes = [0; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16)?;
    }
    Ok(m::FileId(bytes))
}
fn local_hash(
    snapshot: &SourceSnapshot,
    entry: &SourceSnapshotEntry,
    buffer: usize,
    scope: &ActionScope,
) -> Result<m::FileId> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0;
    while offset < entry.node.size {
        scope.check()?;
        let bytes = snapshot.read_local(
            entry.node.id,
            offset,
            (entry.node.size - offset).min(buffer as u64) as usize,
        )?;
        ensure!(!bytes.is_empty(), "local source file was truncated");
        hasher.update(&bytes);
        offset += bytes.len() as u64;
    }
    ensure!(
        snapshot.read_local(entry.node.id, offset, 1)?.is_empty(),
        "local source file grew"
    );
    Ok(m::FileId(*hasher.finalize().as_bytes()))
}

pub(super) async fn plan(
    controller: &Controller,
    snapshot: Arc<SourceSnapshot>,
    base: &CompiledWorkspace,
    policy: &dyn SnapshotPolicy,
    scope: &ActionScope,
) -> Result<Plan> {
    scope.check()?;
    let config = &controller.inner.config;
    let manifest = base.manifest(controller.inner.host.tree().identity());
    let previous: BTreeMap<_, _> = manifest
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let current: BTreeMap<_, _> = snapshot
        .entries()
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    ensure!(
        current.len() <= config.source_limits.max_entries,
        "native source snapshot entry count exceeded"
    );
    let mut known_files = HashMap::new();
    for file in base.file_provenance().values() {
        let origin = known_files
            .entry((file.id, file.size_bytes))
            .or_insert((file.copy_id, false));
        origin.1 |= origin.0 != file.copy_id;
    }
    let known_symlinks: HashSet<_> = manifest
        .entries
        .iter()
        .filter_map(|entry| match &entry.content {
            SourceContent::Symlink(target) => Some(target.as_str()),
            _ => None,
        })
        .collect();
    let missing_copy_source = base
        .file_provenance()
        .iter()
        .any(|(path, file)| file.copy_id.is_some() && !current.contains_key(path.as_str()));
    let mut candidates = Vec::new();
    let mut ignore_directories = BTreeSet::new();
    let mut budget = 0usize;
    for entry in snapshot
        .entries()
        .iter()
        .filter(|entry| entry.node.kind != NodeKind::Directory)
    {
        let tracked = previous
            .get(entry.path.as_str())
            .is_some_and(|old| !matches!(old.content, SourceContent::Directory));
        budget = budget
            .checked_add(entry.path.len().saturating_mul(6))
            .and_then(|n| n.checked_add(256))
            .context("policy inventory overflow")?;
        ensure!(
            budget <= config.source_limits.max_metadata_bytes,
            "policy inventory metadata budget exceeded"
        );
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
            ignore_directories.insert(String::new());
            let mut parent = entry.path.rsplit_once('/').map(|(parent, _)| parent);
            while let Some(path) = parent {
                ignore_directories.insert(path.to_owned());
                parent = path.rsplit_once('/').map(|(parent, _)| parent);
            }
        }
    }
    let mut ignore_files = Vec::new();
    let mut remaining_ignore = config.max_ignore_bytes;
    for directory in ignore_directories {
        scope.check()?;
        let path = if directory.is_empty() {
            ".gitignore".to_owned()
        } else {
            format!("{directory}/.gitignore")
        };
        if let Some(entry) = current
            .get(path.as_str())
            .filter(|entry| entry.node.kind == NodeKind::File)
        {
            let bytes =
                ignore_bytes(controller, &snapshot, entry, &mut remaining_ignore, scope).await?;
            scope.check()?;
            budget = budget
                .checked_add(bytes.len().saturating_mul(6))
                .and_then(|n| n.checked_add(directory.len().saturating_mul(6)))
                .context("policy inventory overflow")?;
            ensure!(
                budget <= config.source_limits.max_metadata_bytes,
                "policy inventory metadata budget exceeded"
            );
            ignore_files.push(PolicyIgnore {
                directory,
                contents: bytes,
            });
        }
    }
    let inventory = PolicyInventory {
        candidates,
        ignore_files,
    };
    // Policy is pure client selection; no native byte mutation is cancelled by
    // bounding this callback. Physical provider reads above are awaited first.
    let decisions = tokio::time::timeout_at(scope.deadline(), policy.evaluate(&inventory))
        .await
        .map_err(|_| ActionScopeError::Deadline)??;
    scope.check()?;
    ensure!(
        decisions.len() == inventory.candidates.len(),
        "snapshot policy returned an incomplete decision set"
    );
    let mut stats = SnapshotStats::default();
    let mut selected = Vec::new();
    let mut local_bytes = 0u64;
    for ((entry, candidate), decision) in snapshot
        .entries()
        .iter()
        .filter(|entry| entry.node.kind != NodeKind::Directory)
        .zip(&inventory.candidates)
        .zip(decisions)
    {
        scope.check()?;
        ensure!(
            !candidate.tracked || decision == PolicyDecision::Track,
            "snapshot policy omitted a tracked path"
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
                ensure!(
                    entry.node.size > max_size,
                    "snapshot policy returned an invalid file-size rejection"
                );
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
        let value = match entry.node.kind {
            NodeKind::Directory => unreachable!(),
            NodeKind::Symlink => {
                let target = entry
                    .target
                    .clone()
                    .context("native snapshot symlink target missing")?;
                ensure!(
                    !target.is_empty()
                        && target.len() <= config.source_limits.max_path_bytes
                        && !target.contains('\0'),
                    "native snapshot symlink target invalid"
                );
                let write = !known_symlinks.contains(target.as_str());
                Value::Symlink { target, write }
            }
            NodeKind::File => {
                ensure!(
                    entry.node.size <= config.max_file_bytes,
                    "native snapshot file size exceeded"
                );
                let executable = entry.node.mode & 0o111 != 0;
                let (id, upload) = if let Some(artifact) = &entry.node.remote {
                    ensure!(
                        artifact.backend == controller.inner.host.backend()
                            && artifact.digest_function == 9
                            && artifact.size == entry.node.size,
                        "native source artifact identity or length mismatch"
                    );
                    (file_id(&artifact.hash)?, None)
                } else {
                    local_bytes = local_bytes
                        .checked_add(entry.node.size)
                        .context("native snapshot byte overflow")?;
                    ensure!(
                        local_bytes <= config.max_snapshot_bytes,
                        "native snapshot local byte budget exceeded"
                    );
                    let id = local_hash(&snapshot, entry, config.io_buffer_bytes, scope)?;
                    let exists = known_files.contains_key(&(id, entry.node.size));
                    (id, if exists { None } else { Some(entry.node.id) })
                };
                let copy_id = if let Some(old) = base.file_provenance().get(&entry.path) {
                    old.copy_id
                } else {
                    match known_files.get(&(id, entry.node.size)) {
                        Some((_, true)) => {
                            bail!("renamed native file has ambiguous copy provenance")
                        }
                        Some((Some(_), false)) => {
                            bail!("renamed native copy-history paths need an explicit copy record")
                        }
                        Some((None, false)) => None,
                        None => {
                            ensure!(
                                !missing_copy_source || entry.node.remote.is_some(),
                                "new local path may have lost native copy provenance; explicit copy tracking is required"
                            );
                            None
                        }
                    }
                };
                Value::File {
                    id,
                    size: entry.node.size,
                    executable,
                    copy_id,
                    upload,
                }
            }
        };
        selected.push(Selected {
            path: entry.path.clone(),
            value,
        });
    }
    // New empty directories are not jj-tracked. Preserve canonical tracked
    // empty directories that still exist, rather than silently dropping them.
    let empty = m::TreeId(
        m::encode(&m::Object::Tree(m::Tree::default()), m::Limits::default())?
            .descriptor
            .key
            .id
            .0,
    );
    let selected_paths: BTreeSet<_> = selected.iter().map(|entry| entry.path.clone()).collect();
    for (path, id) in base.tree_provenance() {
        if path.is_empty()
            || *id != empty
            || !current
                .get(path.as_str())
                .is_some_and(|entry| entry.node.kind == NodeKind::Directory)
        {
            continue;
        }
        let prefix = format!("{path}/");
        if !selected_paths.contains(path)
            && !selected_paths
                .range(prefix.clone()..)
                .next()
                .is_some_and(|entry| entry.starts_with(&prefix))
        {
            selected.push(Selected {
                path: path.clone(),
                value: Value::EmptyDirectory(*id),
            });
        }
    }
    selected.sort_by(|left, right| left.path.cmp(&right.path));
    ensure!(
        selected.len() <= config.source_limits.max_entries,
        "native snapshot tracked entry count exceeded"
    );
    Ok(Plan {
        entries: selected,
        stats,
    })
}

async fn ignore_bytes(
    controller: &Controller,
    snapshot: &SourceSnapshot,
    entry: &SourceSnapshotEntry,
    remaining: &mut usize,
    scope: &ActionScope,
) -> Result<Vec<u8>> {
    let size = usize::try_from(entry.node.size)?;
    ensure!(
        size <= *remaining,
        "native snapshot ignore-policy byte budget exceeded"
    );
    *remaining -= size;
    if let Some(artifact) = &entry.node.remote {
        ensure!(
            artifact.digest_function == 9
                && artifact.backend == controller.inner.host.backend()
                && artifact.size == entry.node.size,
            "native ignore-file descriptor mismatch"
        );
        let id = file_id(&artifact.hash)?;
        let file = if let Some(owner) = scope.owner() {
            controller
                .inner
                .provider
                .read_file_with_owner(id, owner)
                .await?
        } else {
            controller.inner.provider.read_file(id).await?
        };
        let (descriptor, reader) = file.into_parts();
        ensure!(
            descriptor.key.kind == m::Kind::File
                && descriptor.key.id.0 == id.0
                && descriptor.size_bytes == entry.node.size,
            "native ignore-file pinned source mismatch"
        );
        let mut bytes = Vec::new();
        reader
            .take(entry.node.size + 1)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(
            bytes.len() == size && blake3::hash(&bytes).as_bytes() == &id.0,
            "native ignore-file payload mismatch"
        );
        Ok(bytes)
    } else {
        snapshot.read_local(entry.node.id, 0, size)
    }
}

pub(super) async fn write(
    controller: &Controller,
    snapshot: Arc<SourceSnapshot>,
    entries: Vec<Selected>,
    scope: &ActionScope,
) -> Result<m::TreeId> {
    let mut values = BTreeMap::new();
    for entry in entries {
        scope.check()?;
        let value = match entry.value {
            Value::File {
                id,
                size,
                executable,
                copy_id,
                upload,
            } => {
                if let Some(inode) = upload {
                    let mut reader = Reader {
                        snapshot: snapshot.clone(),
                        _owner: controller.inner.clone(),
                        inode,
                        size,
                        offset: 0,
                        buffer: controller.inner.config.io_buffer_bytes,
                    };
                    let result = if let Some(owner) = scope.owner() {
                        controller
                            .inner
                            .provider
                            .write_file_with_owner(&mut reader, size, owner)
                            .await
                    } else {
                        controller
                            .inner
                            .provider
                            .write_file(&mut reader, size)
                            .await
                    };
                    // Do not cancel the admitted physical upload. Gate the
                    // next logical stage only after its future has drained.
                    scope.check()?;
                    let actual = result?;
                    ensure!(
                        actual == id && reader.offset == size,
                        "native dirty upload identity or length mismatch"
                    );
                }
                m::TreeValue::File {
                    id,
                    executable,
                    size_bytes: size,
                    copy_id,
                }
            }
            Value::Symlink { target, write } => {
                let target_bytes = target.len() as u64;
                let encoded = m::encode(&m::Object::Symlink(target.clone()), m::Limits::default())?;
                if write {
                    write_metadata(
                        controller,
                        m::Object::Symlink(target),
                        encoded.descriptor,
                        scope,
                    )
                    .await?;
                }
                m::TreeValue::Symlink {
                    id: m::SymlinkId(encoded.descriptor.key.id.0),
                    target_bytes,
                }
            }
            Value::EmptyDirectory(id) => m::TreeValue::Tree(id),
        };
        ensure!(
            values.insert(entry.path, value).is_none(),
            "duplicate native snapshot path"
        );
    }
    let mut directories: BTreeMap<String, BTreeMap<String, m::TreeValue>> =
        BTreeMap::from([(String::new(), BTreeMap::new())]);
    for (path, value) in values {
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path.as_str()));
        ensure!(
            directories
                .entry(parent.to_owned())
                .or_default()
                .insert(name.to_owned(), value)
                .is_none(),
            "duplicate native snapshot path"
        );
        let mut parent = parent;
        while !parent.is_empty() {
            let (grandparent, _) = parent.rsplit_once('/').unwrap_or(("", parent));
            directories.entry(grandparent.to_owned()).or_default();
            parent = grandparent;
        }
    }
    let mut order: Vec<_> = directories.keys().cloned().collect();
    order.sort_by_key(|path| {
        std::cmp::Reverse(if path.is_empty() {
            0
        } else {
            path.split('/').count()
        })
    });
    let mut root = None;
    for directory in order {
        let children = directories
            .remove(&directory)
            .context("native directory disappeared")?;
        let tree = m::Object::Tree(m::Tree {
            entries: children
                .into_iter()
                .map(|(name, value)| m::TreeEntry { name, value })
                .collect(),
        });
        let encoded = m::encode(&tree, m::Limits::default())?;
        write_metadata(controller, tree, encoded.descriptor, scope).await?;
        let id = m::TreeId(encoded.descriptor.key.id.0);
        if directory.is_empty() {
            root = Some(id);
        } else {
            let (parent, name) = directory
                .rsplit_once('/')
                .unwrap_or(("", directory.as_str()));
            ensure!(
                directories
                    .get_mut(parent)
                    .context("native parent directory missing")?
                    .insert(name.to_owned(), m::TreeValue::Tree(id))
                    .is_none(),
                "native file/directory collision"
            );
        }
    }
    root.context("native snapshot root missing")
}
async fn write_metadata(
    controller: &Controller,
    object: m::Object,
    expected: m::Descriptor,
    scope: &ActionScope,
) -> Result<()> {
    scope.check()?;
    let result = if let Some(owner) = scope.owner() {
        controller
            .inner
            .provider
            .write_metadata_with_owner(object, owner)
            .await
    } else {
        controller.inner.provider.write_metadata(object).await
    };
    scope.check()?;
    let actual = result?;
    ensure!(
        actual == expected,
        "native canonical metadata write descriptor mismatch"
    );
    Ok(())
}
struct Reader {
    snapshot: Arc<SourceSnapshot>,
    _owner: Arc<Inner>,
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
        if self.offset == self.size || bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let count = bytes
            .len()
            .min(self.buffer)
            .min((self.size - self.offset) as usize);
        let result = self
            .snapshot
            .read_local(self.inode, self.offset, count)
            .map_err(|error| std::io::Error::other(error.to_string()));
        match result {
            Ok(data) if data.is_empty() => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "native source file truncated",
            ))),
            Ok(data) => {
                bytes[..data.len()].copy_from_slice(&data);
                self.offset += data.len() as u64;
                Poll::Ready(Ok(data.len()))
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

/// Initial collision policy: only an unchanged acknowledged namespace may be
/// replaced. In particular ignored/untracked files are never dropped by a
/// checkout. A later merge-aware overlay policy can preserve them explicitly.
pub(super) fn unchanged(
    snapshot: &SourceSnapshot,
    base: &CompiledWorkspace,
    scope: &ActionScope,
) -> Result<()> {
    let manifest = base.manifest("unused");
    let entries: BTreeMap<_, _> = manifest
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    ensure!(
        snapshot.entries().len() == entries.len(),
        "native checkout would overwrite dirty or untracked paths"
    );
    for entry in snapshot.entries() {
        scope.check()?;
        let old = entries
            .get(entry.path.as_str())
            .context("native checkout collides with an untracked path")?;
        match (&old.content, entry.node.kind) {
            (SourceContent::Directory, NodeKind::Directory) => {}
            (SourceContent::Symlink(target), NodeKind::Symlink) => ensure!(
                entry.target.as_ref() == Some(target),
                "native checkout would replace an unsnapshotted symlink"
            ),
            (SourceContent::File(artifact), NodeKind::File) => {
                ensure!(
                    entry.node.size == artifact.size
                        && (entry.node.mode & 0o111 != 0) == (old.mode & 0o111 != 0),
                    "native checkout would replace unsnapshotted file metadata"
                );
                if let Some(current) = &entry.node.remote {
                    ensure!(
                        current == artifact,
                        "native checkout would replace unsnapshotted contents"
                    );
                } else {
                    ensure!(
                        local_hash(snapshot, entry, 64 * 1024, scope)?.0
                            == file_id(&artifact.hash)?.0,
                        "native checkout would replace unsnapshotted local bytes"
                    );
                }
            }
            _ => bail!("native checkout would replace an unsnapshotted path type"),
        }
    }
    Ok(())
}

pub(super) fn checkout_stats(
    base: &CompiledWorkspace,
    target: &CompiledWorkspace,
) -> CheckoutStats {
    let before = base.manifest("unused");
    let after = target.manifest("unused");
    let before: BTreeMap<_, _> = before
        .entries
        .iter()
        .filter(|entry| !matches!(entry.content, SourceContent::Directory))
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let after: BTreeMap<_, _> = after
        .entries
        .iter()
        .filter(|entry| !matches!(entry.content, SourceContent::Directory))
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let mut stats = CheckoutStats::default();
    for (path, entry) in &after {
        match before.get(path) {
            None => stats.added_files += 1,
            Some(old) if old.mode != entry.mode || old.content != entry.content => {
                stats.updated_files += 1
            }
            _ => {}
        }
    }
    stats.removed_files = before
        .keys()
        .filter(|path| !after.contains_key(*path))
        .count() as u32;
    stats
}
