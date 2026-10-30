//! Disposable verified candidate snapshots. Read-only execution is a separate
//! kernel/helper obligation, not established by these files or their modes.
use crate::candidate_http::CandidateClient;
use anyhow::{Context, Result, bail, ensure};
use private_fs::PrivateDir;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
};
use tinytree_forge_core::checker::{
    CheckerLease, Descriptor, EntryValue, Key, ManifestEntry, ManifestHasher, SnapshotLimits,
    SnapshotManifest,
};
use tinytree_model::{self as model, Object, TreeValue};
use tokio::{sync::watch, time::Instant};

#[derive(Debug)]
pub struct MaterializedSnapshot {
    pub root: PathBuf,
    pub manifest: SnapshotManifest,
    pub manifest_path: PathBuf,
    pub binding: String,
}

fn live(cancel: &watch::Receiver<bool>, deadline: Instant) -> Result<()> {
    ensure!(!*cancel.borrow(), "candidate preparation cancelled");
    ensure!(Instant::now() < deadline, "candidate preparation deadline");
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn path(path: &str, limits: &SnapshotLimits) -> Result<()> {
    ensure!(
        !path.contains('\0') && path.len() as u64 <= limits.max_path_bytes,
        "invalid candidate path"
    );
    if path.is_empty() {
        return Ok(());
    }
    ensure!(
        !path.starts_with('/')
            && !path.ends_with('/')
            && path
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != ".."),
        "invalid candidate path"
    );
    ensure!(
        Path::new(path)
            .components()
            .all(|p| matches!(p, Component::Normal(_)))
            && path.split('/').count() <= limits.max_depth as usize,
        "candidate path depth exceeds limit"
    );
    Ok(())
}

async fn manifest(
    client: &CandidateClient,
    lease: &CheckerLease,
    limits: &SnapshotLimits,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<SnapshotManifest> {
    ensure!(
        lease.manifest.entries > 0 && lease.manifest.entries <= limits.max_entries,
        "candidate entry limit"
    );
    let mut entries = Vec::new();
    let mut cursor = 0;
    let mut hasher =
        ManifestHasher::new(&lease.request.candidate_commit, &lease.request.root_tree)?;
    loop {
        live(cancel, deadline)?;
        let page = client.manifest(lease, cursor, cancel, deadline).await?;
        ensure!(!page.entries.is_empty(), "empty candidate manifest page");
        for entry in page.entries {
            entry.validate(limits)?;
            path(&entry.path, limits)?;
            ensure!(
                entries.len() < limits.max_entries as usize,
                "candidate entry limit"
            );
            if let Some(previous) = entries.last() {
                let previous: &ManifestEntry = previous;
                ensure!(
                    previous.path < entry.path,
                    "candidate manifest is not strictly ordered"
                );
            } else {
                ensure!(entry.path.is_empty(), "candidate manifest root missing");
            }
            hasher.push(&entry)?;
            entries.push(entry);
        }
        let next = u64::try_from(entries.len())?;
        match page.next {
            Some(position) => {
                ensure!(
                    position == next && position > cursor && position < lease.manifest.entries,
                    "candidate manifest cursor differs"
                );
                cursor = position;
            }
            None => {
                ensure!(
                    next == lease.manifest.entries,
                    "candidate manifest coverage differs"
                );
                break;
            }
        }
    }
    let totals = hasher.finish(limits)?;
    ensure!(
        totals.digest == lease.manifest.digest
            && totals.entries == lease.manifest.entries
            && totals.file_bytes == lease.manifest.file_bytes
            && totals.path_bytes == lease.manifest.path_bytes
            && totals.metadata_bytes == lease.manifest.metadata_bytes,
        "candidate manifest totals differ"
    );
    let manifest = SnapshotManifest {
        schema: 1,
        lease: lease.clone(),
        entries,
    };
    manifest.validate()?;
    Ok(manifest)
}

struct Metadata {
    cached: BTreeMap<(u8, String), (Descriptor, Object)>,
    bytes: u64,
}
impl Metadata {
    async fn get(
        &mut self,
        client: &CandidateClient,
        lease: &CheckerLease,
        key: Key,
        expected: Option<&Descriptor>,
        limits: &SnapshotLimits,
        cancel: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> Result<(Descriptor, Object)> {
        ensure!(
            [2, 3, 4].contains(&key.kind),
            "unsupported snapshot metadata kind"
        );
        if let Some(value) = self.cached.get(&(key.kind, key.id.clone())) {
            if let Some(expected) = expected {
                ensure!(
                    &value.0 == expected,
                    "candidate metadata descriptor differs"
                );
            }
            return Ok(value.clone());
        }
        let mut bytes = Vec::new();
        let mut descriptor = None;
        loop {
            live(cancel, deadline)?;
            let chunk = client
                .object(lease, &key, bytes.len() as u64, 65536, cancel, deadline)
                .await?;
            if let Some(previous) = &descriptor {
                ensure!(
                    previous == &chunk.descriptor,
                    "candidate metadata descriptor changed"
                );
            }
            ensure!(
                chunk.descriptor.size_bytes <= limits.max_metadata_object_bytes
                    && (key.kind == 3
                        || chunk.descriptor.size_bytes
                            <= limits.max_metadata_bytes.saturating_sub(self.bytes)),
                "candidate metadata exceeds limit"
            );
            if let Some(expected) = expected {
                ensure!(
                    &chunk.descriptor == expected,
                    "candidate metadata descriptor differs"
                );
            }
            let end = (bytes.len() as u64)
                .checked_add(chunk.data.len() as u64)
                .context("candidate object size overflow")?;
            ensure!(
                end <= chunk.descriptor.size_bytes
                    && chunk.eof == (end == chunk.descriptor.size_bytes),
                "candidate metadata extent differs"
            );
            ensure!(
                chunk.eof || !chunk.data.is_empty(),
                "candidate metadata made no progress"
            );
            descriptor = Some(chunk.descriptor);
            bytes.extend_from_slice(&chunk.data);
            if chunk.eof {
                break;
            }
        }
        let descriptor = descriptor.context("candidate metadata descriptor missing")?;
        let native = descriptor.native()?;
        let model_limits = model::Limits {
            max_object_bytes: usize::try_from(limits.max_metadata_object_bytes)?,
            ..model::Limits::default()
        };
        model::verify(&native, &bytes, model_limits)?;
        let object = model::decode(&bytes, model_limits)?;
        if key.kind != 3 {
            self.bytes = self
                .bytes
                .checked_add(descriptor.size_bytes)
                .context("candidate metadata size overflow")?;
        }
        self.cached
            .insert((key.kind, key.id), (descriptor.clone(), object.clone()));
        Ok((descriptor, object))
    }
}

/// Verify the COMMIT/root TREE and every current-tree path independently from
/// the server's manifest. Commit ancestry and CopyHistory bodies are not read.
async fn verify_tree(
    client: &CandidateClient,
    lease: &CheckerLease,
    manifest: &SnapshotManifest,
    limits: &SnapshotLimits,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<BTreeMap<String, String>> {
    let mut metadata = Metadata {
        cached: BTreeMap::new(),
        bytes: 0,
    };
    let (_, commit) = metadata
        .get(
            client,
            lease,
            Key {
                kind: 3,
                id: lease.request.candidate_commit.clone(),
            },
            None,
            limits,
            cancel,
            deadline,
        )
        .await?;
    let Object::Commit(commit) = commit else {
        bail!("candidate is not a commit");
    };
    ensure!(
        commit.root_tree.terms.len() == 1
            && hex(&commit.root_tree.terms[0].0) == lease.request.root_tree,
        "candidate root tree differs or is conflicted"
    );
    let entries: BTreeMap<&str, &ManifestEntry> = manifest
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    ensure!(
        entries.len() == manifest.entries.len(),
        "duplicate candidate manifest path"
    );
    let mut visited = BTreeSet::new();
    let mut stack = vec![(String::new(), lease.request.root_tree.clone())];
    let mut links = BTreeMap::new();
    while let Some((directory, tree_id)) = stack.pop() {
        live(cancel, deadline)?;
        path(&directory, limits)?;
        let selected = entries
            .get(directory.as_str())
            .context("candidate directory omitted")?;
        ensure!(
            matches!(selected.value, EntryValue::Directory {})
                && selected.descriptor.kind == 2
                && selected.descriptor.id == tree_id,
            "candidate directory differs"
        );
        ensure!(
            visited.insert(directory.clone()),
            "duplicate candidate tree path"
        );
        let (_, tree) = metadata
            .get(
                client,
                lease,
                Key {
                    kind: 2,
                    id: tree_id,
                },
                Some(&selected.descriptor),
                limits,
                cancel,
                deadline,
            )
            .await?;
        let Object::Tree(tree) = tree else {
            bail!("candidate directory is not a tree");
        };
        for item in tree.entries {
            let name = if directory.is_empty() {
                item.name
            } else {
                format!("{directory}/{}", item.name)
            };
            path(&name, limits)?;
            let selected = entries
                .get(name.as_str())
                .context("candidate path omitted")?;
            match item.value {
                TreeValue::Tree(id) => stack.push((name, hex(&id.0))),
                TreeValue::File {
                    id,
                    size_bytes,
                    executable,
                    copy_id,
                } => {
                    ensure!(
                        selected.descriptor.kind == 1
                            && selected.descriptor.id == hex(&id.0)
                            && selected.descriptor.size_bytes == size_bytes,
                        "candidate file descriptor differs"
                    );
                    ensure!(
                        selected.value
                            == EntryValue::File {
                                executable,
                                copy_id: copy_id.map(|id| hex(&id.0))
                            },
                        "candidate file mode or copy identity differs"
                    );
                    ensure!(visited.insert(name), "duplicate candidate file path");
                }
                TreeValue::Symlink { id, target_bytes } => {
                    ensure!(
                        selected.descriptor.kind == 4
                            && selected.descriptor.id == hex(&id.0)
                            && selected.value == EntryValue::Symlink { target_bytes },
                        "candidate symlink differs"
                    );
                    let (_, link) = metadata
                        .get(
                            client,
                            lease,
                            Key {
                                kind: 4,
                                id: hex(&id.0),
                            },
                            Some(&selected.descriptor),
                            limits,
                            cancel,
                            deadline,
                        )
                        .await?;
                    let Object::Symlink(target) = link else {
                        bail!("candidate link is not a symlink");
                    };
                    ensure!(
                        target.len() as u64 == target_bytes,
                        "candidate symlink length differs"
                    );
                    // Conservative execution policy; never reinterpret or follow
                    // an unsupported native target into the checker host.
                    ensure!(
                        !target.is_empty()
                            && !Path::new(&target).is_absolute()
                            && !target.contains('\0')
                            && target.split('/').all(|s| !s.is_empty() && s != ".."),
                        "unsupported candidate symlink target"
                    );
                    ensure!(
                        visited.insert(name.clone()),
                        "duplicate candidate symlink path"
                    );
                    links.insert(name, target);
                }
            }
        }
        ensure!(
            visited.len() <= limits.max_entries as usize
                && stack.len() <= limits.max_entries as usize,
            "candidate tree exceeds entry limit"
        );
    }
    ensure!(
        visited.len() == entries.len(),
        "candidate manifest contains extra paths"
    );
    Ok(links)
}

#[cfg(unix)]
pub async fn materialize(
    client: &CandidateClient,
    lease: &CheckerLease,
    root: &Path,
    manifest_path: &Path,
    limits: &SnapshotLimits,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<MaterializedSnapshot> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};
    lease.validate()?;
    live(cancel, deadline)?;
    ensure!(
        !manifest_path.starts_with(root),
        "snapshot manifest must be outside source"
    );
    let limits = limits.intersect(&lease.limits)?;
    let manifest = manifest(client, lease, &limits, cancel, deadline).await?;
    let links = verify_tree(client, lease, &manifest, &limits, cancel, deadline).await?;
    let anchor = PrivateDir::create_all_durable(root)?;
    ensure!(
        fs::read_dir(root)?.next().is_none(),
        "incomplete snapshot requires owned cleanup before retry"
    );
    let manifest_parent = PrivateDir::open_existing(
        manifest_path
            .parent()
            .context("snapshot manifest parent missing")?,
    )?;
    let manifest_name = manifest_path
        .file_name()
        .context("snapshot manifest filename missing")?;
    let first_components: BTreeSet<&str> = manifest
        .entries
        .iter()
        .filter_map(|entry| entry.path.split('/').next())
        .collect();
    let temporary_name = (0..=manifest.entries.len())
        .map(|i| format!(".checker-download-{i}"))
        .find(|name| !first_components.contains(name.as_str()))
        .context("snapshot temporary path unavailable")?;
    let temporary = PrivateDir::create(root.join(&temporary_name))?;
    for entry in &manifest.entries {
        live(cancel, deadline)?;
        if matches!(entry.value, EntryValue::Directory {}) && !entry.path.is_empty() {
            PrivateDir::create_all_durable(root.join(&entry.path))?;
        }
    }
    for (ordinal, entry) in manifest.entries.iter().enumerate() {
        let EntryValue::File { executable, .. } = entry.value else {
            continue;
        };
        live(cancel, deadline)?;
        let destination = root.join(&entry.path);
        let parent = PrivateDir::open_existing(
            destination
                .parent()
                .context("candidate file parent missing")?,
        )?;
        let temporary_path = temporary.path().join(ordinal.to_string());
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary_path)?;
        let mut offset = 0;
        let mut hash = blake3::Hasher::new();
        loop {
            live(cancel, deadline)?;
            let key = Key {
                kind: 1,
                id: entry.descriptor.id.clone(),
            };
            let chunk = client
                .object(lease, &key, offset, 65536, cancel, deadline)
                .await?;
            ensure!(
                chunk.descriptor == entry.descriptor,
                "candidate file descriptor changed"
            );
            let end = offset
                .checked_add(chunk.data.len() as u64)
                .context("candidate file size overflow")?;
            ensure!(
                end <= entry.descriptor.size_bytes
                    && chunk.eof == (end == entry.descriptor.size_bytes),
                "candidate file extent differs"
            );
            ensure!(
                chunk.eof || !chunk.data.is_empty(),
                "candidate file made no progress"
            );
            hash.update(&chunk.data);
            file.write_all(&chunk.data)?;
            offset = end;
            if chunk.eof {
                break;
            }
        }
        ensure!(
            hash.finalize().to_hex().as_str() == entry.descriptor.id,
            "candidate FILE digest differs"
        );
        file.set_permissions(fs::Permissions::from_mode(if executable {
            0o755
        } else {
            0o644
        }))?;
        file.sync_all()?;
        drop(file);
        ensure!(
            fs::symlink_metadata(&destination)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            "candidate destination already exists"
        );
        fs::rename(&temporary_path, &destination)?;
        fs::File::open(parent.path())?.sync_all()?;
    }
    drop(temporary);
    fs::remove_dir(root.join(temporary_name))?;
    for (name, target) in links {
        live(cancel, deadline)?;
        let destination = root.join(name);
        let parent = PrivateDir::open_existing(
            destination
                .parent()
                .context("candidate symlink parent missing")?,
        )?;
        symlink(target, destination)?;
        fs::File::open(parent.path())?.sync_all()?;
    }
    live(cancel, deadline)?;
    fs::File::open(anchor.path())?.sync_all()?;
    let binding = manifest.immutable_binding()?;
    let bytes = serde_json::to_vec(&manifest)?;
    ensure!(
        bytes.len() <= 32 * 1024 * 1024,
        "snapshot manifest exceeds limit"
    );
    manifest_parent.replace(manifest_name, &bytes)?;
    Ok(MaterializedSnapshot {
        root: root.to_owned(),
        manifest,
        manifest_path: manifest_path.to_owned(),
        binding,
    })
}

#[cfg(not(unix))]
pub async fn materialize(
    _client: &CandidateClient,
    _lease: &CheckerLease,
    _root: &Path,
    _manifest_path: &Path,
    _limits: &SnapshotLimits,
    _cancel: &mut watch::Receiver<bool>,
    _deadline: Instant,
) -> Result<MaterializedSnapshot> {
    bail!("candidate snapshot execution requires Unix")
}
