//! Metadata-only source trees and bounded frozen inventories. A frontend must
//! exclude writes outside `Tree` before attesting to an external write fence.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct SourceLimits {
    pub max_entries: usize,
    pub max_metadata_bytes: usize,
    pub max_path_bytes: usize,
    pub max_depth: usize,
    pub max_local_bytes: u64,
    pub max_receipts: usize,
}
impl Default for SourceLimits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_metadata_bytes: 32 * 1024 * 1024,
            max_path_bytes: 4096,
            max_depth: 128,
            max_local_bytes: 1024 * 1024 * 1024,
            max_receipts: 128,
        }
    }
}
impl SourceLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.max_entries > 0
                && self.max_metadata_bytes > 0
                && self.max_path_bytes > 0
                && self.max_depth > 0
                && self.max_local_bytes > 0
                && self.max_receipts > 0,
            "invalid source limits"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceContent {
    Directory,
    File(Artifact),
    Symlink(String),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceEntry {
    pub path: String,
    pub mode: u32,
    pub content: SourceContent,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceManifest {
    pub identity: String,
    pub root: [u8; 32],
    pub entries: Vec<SourceEntry>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceVersion {
    pub generation: u64,
    pub revision: u64,
    pub fingerprint: [u8; 32],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceOperation {
    Install,
    Acknowledge,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceReceipt {
    pub request_id: String,
    pub operation: SourceOperation,
    pub intent: [u8; 32],
    pub root: [u8; 32],
    pub expected: Option<SourceVersion>,
    pub version: SourceVersion,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Receipt {
    intent: [u8; 32],
    result: SourceReceipt,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct SourceMeta {
    pub(super) root: [u8; 32],
    pub(super) generation: u64,
    pub(super) revision: u64,
    receipts: BTreeMap<String, Receipt>,
}
#[derive(Default)]
pub(super) struct Gate {
    pub(super) frozen: bool,
    active: usize,
    changed: Arc<tokio::sync::Notify>,
}
/// Only these transient gate conditions may be retried when acquiring a source
/// snapshot. Namespace, inventory and limit errors are not contention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceFreezeBusy {
    ActiveOperations,
    Frozen,
}
impl std::fmt::Display for SourceFreezeBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ActiveOperations => "source namespace has active operations",
            Self::Frozen => "source namespace is frozen",
        })
    }
}
impl std::error::Error for SourceFreezeBusy {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceFreezeTimeout;
impl std::fmt::Display for SourceFreezeTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source snapshot acquisition deadline expired")
    }
}
impl std::error::Error for SourceFreezeTimeout {}
pub(super) struct Job {
    gate: Arc<Mutex<Gate>>,
}
impl Drop for Job {
    fn drop(&mut self) {
        let changed = {
            let mut gate = self.gate.lock().unwrap();
            gate.active -= 1;
            (gate.active == 0).then(|| gate.changed.clone())
        };
        if let Some(changed) = changed {
            changed.notify_waiters();
        }
    }
}
struct Freeze {
    gate: Arc<Mutex<Gate>>,
}
impl Drop for Freeze {
    fn drop(&mut self) {
        let changed = {
            let mut gate = self.gate.lock().unwrap();
            gate.frozen = false;
            gate.changed.clone()
        };
        changed.notify_waiters();
    }
}
/// Trusted frontend attestation, not an OS fence. `TreeOnly` is valid only when
/// every authoritative mutation goes through this Tree (for example FUSE).
/// ProjFS/native writers require a real frontend-owned capture/quiescence fence.
#[derive(Clone, Copy, Debug)]
pub enum SourceFrontendFence {
    TreeOnly,
    AuthoritativelyQuiesced,
}
#[derive(Clone, Debug)]
pub struct SourceSnapshotEntry {
    pub path: String,
    pub node: Node,
    pub target: Option<String>,
}
/// Holds mutation exclusion until dropped. No file hydration occurs. Local
/// payload reads are bounded and stable only while this guard remains alive.
pub struct SourceSnapshot {
    tree: Arc<Tree>,
    _freeze: Freeze,
    version: SourceVersion,
    entries: Vec<SourceSnapshotEntry>,
    entries_by_id: HashMap<u64, usize>,
    limits: SourceLimits,
}
impl SourceSnapshot {
    pub fn base_root(&self) -> [u8; 32] {
        // The snapshot owns the exclusion barrier; this header cannot change.
        self.tree
            .namespace
            .lock()
            .unwrap()
            .state
            .source
            .as_ref()
            .unwrap()
            .root
    }
    pub fn version(&self) -> &SourceVersion {
        &self.version
    }
    pub fn entries(&self) -> &[SourceSnapshotEntry] {
        &self.entries
    }
    pub fn read_local(&self, id: u64, offset: u64, count: usize) -> Result<Vec<u8>> {
        ensure!(
            count <= self.limits.max_metadata_bytes,
            "source snapshot read exceeds limit"
        );
        let index = *self
            .entries_by_id
            .get(&id)
            .context("snapshot entry missing")?;
        let entry = &self.entries[index];
        ensure!(
            entry.node.kind == NodeKind::File && entry.node.remote.is_none(),
            "not a local snapshot file"
        );
        read_range(&self.tree.local_path(id), offset, count)
    }
    /// Commit a replacement while retaining the same exclusion barrier. The
    /// caller may first upload local snapshot files; this does not offload them.
    /// Confirm a fully uploaded native root without changing any inode, mode,
    /// ignored/untracked entry or local payload authority. The trusted caller
    /// owns canonical root validation and durable upstream publication.
    pub fn acknowledge_root(
        self,
        root: [u8; 32],
        expected: SourceVersion,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        limits.validate()?;
        validate_request_id(request_id)?;
        ensure!(root != [0; 32], "invalid source baseline root");
        let intent = *blake3::hash(&serde_json::to_vec(&(
            "acknowledge-v1",
            self.tree.identity(),
            root,
            &expected,
        ))?)
        .as_bytes();
        let mut ns = self.tree.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace unavailable or pending publication"
        );
        let source = ns.state.source.as_ref().context("not a source namespace")?;
        if let Some(receipt) = source.receipts.get(request_id) {
            ensure!(
                receipt.intent == intent
                    && receipt.result.operation == SourceOperation::Acknowledge,
                "source request ID reused with different intent"
            );
            return Ok(receipt.result.clone());
        }
        ensure!(expected == self.version, "source snapshot is stale");
        ensure!(
            source.receipts.len() < limits.max_receipts,
            "source receipt limit exceeded"
        );
        let generation = source
            .generation
            .checked_add(1)
            .context("source generation exhausted")?;
        let revision = source
            .revision
            .checked_add(1)
            .context("source revision exhausted")?;
        Tree::change_locked(&mut ns, |state| {
            let source = state.source.as_mut().unwrap();
            source.root = root;
            source.generation = generation;
            source.revision = revision;
            let (_, version) = self.tree.source_inventory(state, limits)?;
            let result = SourceReceipt {
                request_id: request_id.into(),
                operation: SourceOperation::Acknowledge,
                intent,
                root,
                expected: Some(expected),
                version,
            };
            state.source.as_mut().unwrap().receipts.insert(
                request_id.into(),
                Receipt {
                    intent,
                    result: result.clone(),
                },
            );
            Ok(result)
        })
    }
    pub fn install(
        self,
        manifest: SourceManifest,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        self.tree
            .install_source_frozen(manifest, Some(self.version.clone()), request_id, limits)
    }
}
impl Tree {
    pub(super) fn source_job(&self) -> Result<Job> {
        let mut gate = self.source_gate.lock().unwrap();
        ensure!(!gate.frozen, "source namespace is frozen");
        gate.active = gate
            .active
            .checked_add(1)
            .context("source job count exhausted")?;
        Ok(Job {
            gate: self.source_gate.clone(),
        })
    }
    fn source_freeze(&self) -> Result<Freeze> {
        let mut gate = self.source_gate.lock().unwrap();
        if gate.frozen {
            return Err(SourceFreezeBusy::Frozen.into());
        }
        if gate.active != 0 {
            return Err(SourceFreezeBusy::ActiveOperations.into());
        }
        gate.frozen = true;
        Ok(Freeze {
            gate: self.source_gate.clone(),
        })
    }
    pub fn freeze_source(
        self: &Arc<Self>,
        _fence: SourceFrontendFence,
        limits: &SourceLimits,
    ) -> Result<SourceSnapshot> {
        limits.validate()?;
        let freeze = self.source_freeze()?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace has pending publication"
        );
        let (entries, version) = self.source_inventory(&ns.state, limits)?;
        let entries_by_id = entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.node.id, index))
            .collect();
        Ok(SourceSnapshot {
            tree: self.clone(),
            _freeze: freeze,
            entries,
            entries_by_id,
            version,
            limits: limits.clone(),
        })
    }
    /// Wait for Tree-owned physical jobs or an existing snapshot to release
    /// their guards, without cancelling either. No fence is held while waiting.
    /// The frontend attestation has the same requirements as `freeze_source`.
    /// Only typed gate contention is retried; all other failures are immediate.
    pub async fn freeze_source_wait(
        self: &Arc<Self>,
        fence: SourceFrontendFence,
        limits: &SourceLimits,
        deadline: tokio::time::Instant,
    ) -> Result<SourceSnapshot> {
        limits.validate()?;
        let changed = self.source_gate.lock().unwrap().changed.clone();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SourceFreezeTimeout.into());
            }
            // Register before inspecting the gate so a concurrent final job
            // drop cannot be lost between the failed acquisition and waiting.
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.freeze_source(fence, limits) {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) if error.downcast_ref::<SourceFreezeBusy>().is_some() => {}
                Err(error) => return Err(error),
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| SourceFreezeTimeout)?;
        }
    }
    /// Metadata-only atomic publication. The first publication requires an
    /// otherwise empty namespace. Subsequent publication requires an exact
    /// current snapshot token, including all locally authoritative edits.
    /// This entry point requires Tree-only authoritative mutation. Frontends
    /// admitting external native writes must fence/capture them first and use
    /// `SourceSnapshot::install` while retaining that frontend fence.
    pub fn install_source(
        &self,
        manifest: SourceManifest,
        expected: Option<SourceVersion>,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        let _freeze = self.source_freeze()?;
        self.install_source_frozen(manifest, expected, request_id, limits)
    }
    fn validate_manifest(
        &self,
        manifest: &SourceManifest,
        limits: &SourceLimits,
    ) -> Result<BTreeMap<String, SourceEntry>> {
        limits.validate()?;
        ensure!(
            manifest.identity == self.identity && manifest.root != [0; 32],
            "source manifest identity mismatch"
        );
        ensure!(
            manifest.entries.len() <= limits.max_entries,
            "source entry limit exceeded"
        );
        let mut entries = BTreeMap::new();
        let mut bytes = manifest.identity.len();
        for entry in &manifest.entries {
            ensure!(
                !entry.path.is_empty() && entry.path.len() <= limits.max_path_bytes,
                "invalid source path"
            );
            // Apply raw byte/depth limits before allocating a component list or
            // cloning any metadata. Slash-rich inputs cannot amplify memory.
            for (depth, name) in entry.path.split('/').enumerate() {
                ensure!(depth < limits.max_depth, "source path depth exceeded");
                component(name)?;
                ensure!(
                    depth != 0 || !control_name(name),
                    "reserved source administration path"
                );
                ensure!(
                    !name.ends_with(['.', ' '])
                        && !name
                            .chars()
                            .any(|c| c.is_control() || ":*?\"<>|".contains(c)),
                    "nonportable source path"
                );
                let stem = name.split('.').next().unwrap();
                ensure!(
                    !["CON", "PRN", "AUX", "NUL"]
                        .iter()
                        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
                        && !(stem.len() == 4
                            && (stem
                                .get(..3)
                                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("COM")
                                    || prefix.eq_ignore_ascii_case("LPT")))
                            && matches!(stem.as_bytes()[3], b'1'..=b'9')),
                    "reserved source path"
                );
            }
            ensure!(entry.mode & !0o777 == 0, "invalid source mode");
            let extra = match &entry.content {
                SourceContent::File(artifact) => {
                    self.validate_artifact(artifact)?;
                    ensure!(
                        artifact.digest_function == 9,
                        "source files require raw BLAKE3"
                    );
                    artifact
                        .backend
                        .len()
                        .checked_add(artifact.instance.len())
                        .and_then(|n| n.checked_add(artifact.hash.len()))
                        .context("source metadata overflow")?
                }
                SourceContent::Symlink(target) => {
                    ensure!(
                        entry.mode == 0o777
                            && !target.is_empty()
                            && !target.contains('\0')
                            && target.len() <= limits.max_path_bytes,
                        "invalid source symlink"
                    );
                    target.len()
                }
                SourceContent::Directory => 0,
            };
            // Six bytes per UTF-8 byte conservatively bounds JSON escaping,
            // plus fixed keys/descriptor fields, before cloning/serialization.
            let charge = entry
                .path
                .len()
                .checked_add(extra)
                .and_then(|n| n.checked_mul(6))
                .and_then(|n| n.checked_add(512))
                .context("source metadata overflow")?;
            bytes = bytes
                .checked_add(charge)
                .context("source metadata overflow")?;
            ensure!(
                bytes <= limits.max_metadata_bytes,
                "source metadata limit exceeded"
            );
            ensure!(
                entries.insert(entry.path.clone(), entry.clone()).is_none(),
                "duplicate source path"
            );
        }
        for path in entries.keys() {
            if let Some((parent, _)) = path.rsplit_once('/') {
                ensure!(
                    entries
                        .get(parent)
                        .is_some_and(|entry| entry.content == SourceContent::Directory),
                    "source parent directory missing"
                );
            }
        }
        Ok(entries)
    }
    fn install_source_frozen(
        &self,
        manifest: SourceManifest,
        expected: Option<SourceVersion>,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        validate_request_id(request_id)?;
        let entries = self.validate_manifest(&manifest, limits)?;
        let encoded = serde_json::to_vec(&(
            "install-v1",
            manifest.identity.as_str(),
            manifest.root,
            entries.values().collect::<Vec<_>>(),
            &expected,
        ))?;
        ensure!(
            encoded.len() <= limits.max_metadata_bytes,
            "source encoded metadata limit exceeded"
        );
        let intent = *blake3::hash(&encoded).as_bytes();
        let mut ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace unavailable or pending publication"
        );
        if let Some(receipt) = ns
            .state
            .source
            .as_ref()
            .and_then(|source| source.receipts.get(request_id))
        {
            ensure!(
                receipt.intent == intent && receipt.result.operation == SourceOperation::Install,
                "source request ID reused with different intent"
            );
            return Ok(receipt.result.clone());
        }
        let (generation, revision, mut receipts) = match &ns.state.source {
            Some(source) => {
                let (_, actual) = self.source_inventory(&ns.state, limits)?;
                ensure!(
                    expected.as_ref() == Some(&actual),
                    "source snapshot is stale"
                );
                (
                    source
                        .generation
                        .checked_add(1)
                        .context("source generation exhausted")?,
                    source
                        .revision
                        .checked_add(1)
                        .context("source revision exhausted")?,
                    source.receipts.clone(),
                )
            }
            None => {
                ensure!(
                    expected.is_none()
                        && ns.state.nodes.map.len() == 1
                        && ns.state.pending.map.is_empty(),
                    "initial source install requires empty namespace"
                );
                (1, 1, BTreeMap::new())
            }
        };
        ensure!(
            receipts.len() < limits.max_receipts,
            "source receipt limit exceeded"
        );
        let old = if ns.state.source.is_some() {
            self.source_inventory(&ns.state, limits)?
                .0
                .into_iter()
                .map(|entry| entry.node.id)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        Self::change_locked(&mut ns, |state| {
            // Retire old immutable IDs; existing frontend leases/handles still
            // address their exact historical metadata and authoritative payload.
            for id in old {
                let node = state.nodes.get(&id).unwrap().node.clone();
                index_remove(state, &node);
                state.nodes.get_mut(&id).unwrap().node.linked = false;
            }
            let mut ids = BTreeMap::from([(String::new(), 1)]);
            for entry in entries.values() {
                let (parent_path, name) = entry.path.rsplit_once('/').unwrap_or(("", &entry.path));
                let parent = *ids.get(parent_path).context("source parent missing")?;
                let (kind, remote, target) = match &entry.content {
                    SourceContent::Directory => (NodeKind::Directory, None, None),
                    SourceContent::File(artifact) => (NodeKind::File, Some(artifact.clone()), None),
                    SourceContent::Symlink(target) => {
                        (NodeKind::Symlink, None, Some(target.clone()))
                    }
                };
                let node = allocate(state, parent, name, kind, entry.mode, remote, true)?;
                if let Some(target) = target {
                    let record = state.nodes.get_mut(&node.id).unwrap();
                    record.node.size = target.len() as u64;
                    record.target = Some(target);
                }
                ids.insert(entry.path.clone(), node.id);
            }
            // Native directory-cache barriers need a changed root timestamp
            // together with the atomic binding replacement, not a later write.
            touch(state, 1);
            state.source = Some(SourceMeta {
                root: manifest.root,
                generation,
                revision,
                receipts: BTreeMap::new(),
            });
            let (_, version) = self.source_inventory(state, limits)?;
            let result = SourceReceipt {
                request_id: request_id.into(),
                operation: SourceOperation::Install,
                intent,
                root: manifest.root,
                expected: expected.clone(),
                version,
            };
            receipts.insert(
                request_id.into(),
                Receipt {
                    intent,
                    result: result.clone(),
                },
            );
            state.source.as_mut().unwrap().receipts = receipts;
            Ok(result)
        })
    }
    fn source_inventory(
        &self,
        state: &State,
        limits: &SourceLimits,
    ) -> Result<(Vec<SourceSnapshotEntry>, SourceVersion)> {
        let source = state.source.as_ref().context("not a source namespace")?;
        let mut entries = Vec::new();
        let mut stack = vec![(1, String::new())];
        let mut metadata = 0usize;
        let mut local_bytes = 0u64;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"output-core-source-snapshot-v1\0");
        hasher.update(self.identity.as_bytes());
        hasher.update(&source.root);
        hasher.update(&source.generation.to_le_bytes());
        hasher.update(&source.revision.to_le_bytes());
        while let Some((parent, prefix)) = stack.pop() {
            for ((_, id), _) in state.children.range((parent, 0)..=(parent, u64::MAX)) {
                let id = *id;
                let record = record(state, id)?;
                ensure!(record.node.linked, "invalid source child index");
                if parent == 1 && control_name(&record.node.name) {
                    continue;
                }
                let path_bytes = prefix
                    .len()
                    .checked_add(record.node.name.len())
                    .and_then(|n| n.checked_add(usize::from(!prefix.is_empty())))
                    .context("source path overflow")?;
                ensure!(
                    path_bytes <= limits.max_path_bytes,
                    "source path byte limit exceeded"
                );
                let path = if prefix.is_empty() {
                    record.node.name.clone()
                } else {
                    format!("{prefix}/{}", record.node.name)
                };
                ensure!(
                    entries.len() < limits.max_entries
                        && path.len() <= limits.max_path_bytes
                        && parts(&path)?.len() <= limits.max_depth,
                    "source inventory limit exceeded"
                );
                let mut raw_metadata = path
                    .len()
                    .checked_add(record.node.name.len())
                    .and_then(|n| n.checked_add(record.target.as_ref().map_or(0, String::len)))
                    .context("source metadata overflow")?;
                if let Some(artifact) = &record.node.remote {
                    raw_metadata = raw_metadata
                        .checked_add(artifact.backend.len())
                        .and_then(|n| n.checked_add(artifact.instance.len()))
                        .and_then(|n| n.checked_add(artifact.hash.len()))
                        .context("source metadata overflow")?;
                }
                let upper = raw_metadata
                    .checked_mul(6)
                    .and_then(|n| n.checked_add(512))
                    .context("source metadata overflow")?;
                ensure!(
                    upper <= limits.max_metadata_bytes.saturating_sub(metadata),
                    "source inventory metadata exceeded"
                );
                let encoded = serde_json::to_vec(&(&path, &record.node, &record.target))?;
                metadata = metadata
                    .checked_add(encoded.len())
                    .context("source metadata overflow")?;
                ensure!(
                    metadata <= limits.max_metadata_bytes,
                    "source inventory metadata exceeded"
                );
                hasher.update(&(encoded.len() as u64).to_le_bytes());
                hasher.update(&encoded);
                if record.node.kind == NodeKind::File && record.node.remote.is_none() {
                    local_bytes = local_bytes
                        .checked_add(record.node.size)
                        .context("source local byte overflow")?;
                    ensure!(
                        local_bytes <= limits.max_local_bytes,
                        "source local byte limit exceeded"
                    );
                    let mut file = File::open(self.local_path(id))?;
                    ensure!(
                        file.metadata()?.len() == record.node.size,
                        "local source size mismatch"
                    );
                    let mut remaining = record.node.size;
                    let mut buffer = [0; 128 * 1024];
                    while remaining != 0 {
                        let count = file.read(&mut buffer[..remaining.min(128 * 1024) as usize])?;
                        ensure!(count != 0, "local source payload truncated");
                        hasher.update(&buffer[..count]);
                        remaining -= count as u64;
                    }
                    ensure!(
                        file.read(&mut buffer[..1])? == 0,
                        "local source payload grew"
                    );
                }
                if record.node.kind == NodeKind::Directory {
                    stack.push((id, path.clone()));
                }
                entries.push(SourceSnapshotEntry {
                    path,
                    node: record.node.clone(),
                    target: record.target.clone(),
                });
            }
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok((
            entries,
            SourceVersion {
                generation: source.generation,
                revision: source.revision,
                fingerprint: *hasher.finalize().as_bytes(),
            },
        ))
    }
}

fn control_name(name: &str) -> bool {
    name.split(':')
        .next()
        .unwrap_or(name)
        .trim_end_matches(['.', ' '])
        .eq_ignore_ascii_case(".jj")
}

fn is_control(state: &State, mut id: u64) -> Result<bool> {
    for _ in 0..1024 {
        if id == 1 {
            return Ok(false);
        }
        let node = &record(state, id)?.node;
        if node.parent == 1 {
            return Ok(control_name(&node.name));
        }
        id = node.parent;
    }
    anyhow::bail!("source ancestry depth exceeded")
}
pub(super) fn validate_control_change(previous: &State, next: &State) -> Result<()> {
    if previous.source.is_none() {
        return Ok(());
    }
    for id in &next.nodes.dirty {
        let old = previous.nodes.get(id);
        let new = next.nodes.get(id);
        let control =
            old.is_some() && is_control(previous, *id)? || new.is_some() && is_control(next, *id)?;
        if control {
            ensure!(old == new, "source administration is immutable");
        }
    }
    Ok(())
}
impl Tree {
    pub(super) fn source_writable_node(&self, id: u64) -> Result<()> {
        let ns = self.namespace.lock().unwrap();
        ensure!(
            ns.state.source.is_none() || !is_control(&ns.state, id)?,
            "source administration is immutable"
        );
        Ok(())
    }
    pub(super) fn source_writable_child(&self, parent: u64, name: &str) -> Result<()> {
        self.source_writable_node(parent)?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            ns.state.source.is_none() || parent != 1 || !control_name(name),
            "reserved source administration path"
        );
        Ok(())
    }
    pub(super) fn source_writable_path(&self, path: &str) -> Result<()> {
        let ns = self.namespace.lock().unwrap();
        ensure!(
            ns.state.source.is_none()
                || !path
                    .split('/')
                    .next()
                    .is_some_and(|name| control_name(name)),
            "reserved source administration path"
        );
        Ok(())
    }
    /// Install an opaque, immutable frontend identity marker. This is a trusted
    /// control-plane operation, not a filesystem mutation entry point. The
    /// marker is never interpreted as a host path or included in source uploads.
    pub fn install_source_control(&self, locator: &[u8], limits: &SourceLimits) -> Result<Node> {
        limits.validate()?;
        ensure!(
            !locator.is_empty() && locator.len() <= 4096,
            "invalid source control marker"
        );
        let _freeze = self.source_freeze()?;
        let mut ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace unavailable or pending publication"
        );
        ensure!(ns.state.source.is_some(), "not a source namespace");
        if let Some(admin) = child(&ns.state, 1, ".jj") {
            let id = child(&ns.state, admin, "native-workspace")
                .context("incomplete source control state")?;
            let node = &record(&ns.state, id)?.node;
            ensure!(
                node.kind == NodeKind::File
                    && node.remote.is_none()
                    && node.size == locator.len() as u64
                    && read_range(&self.local_path(id), 0, locator.len() + 1)? == locator,
                "source control identity mismatch"
            );
            return Ok(node.clone());
        }
        Self::change_locked(&mut ns, |state| {
            let admin = allocate(state, 1, ".jj", NodeKind::Directory, 0o555, None, true)?;
            let marker = allocate(
                state,
                admin.id,
                "native-workspace",
                NodeKind::File,
                0o444,
                None,
                true,
            )?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.local_path(marker.id))?;
            file.write_all(locator)?;
            file.sync_all()?;
            sync_directory(&self.state_dir.join("local"))?;
            let node = &mut state.nodes.get_mut(&marker.id).unwrap().node;
            node.size = locator.len() as u64;
            Ok(node.clone())
        })
    }
}

fn validate_request_id(request_id: &str) -> Result<()> {
    ensure!(
        !request_id.is_empty()
            && request_id.len() <= 128
            && request_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid source request ID"
    );
    Ok(())
}
impl Tree {
    /// Metadata-only baseline observation, not a snapshot/fingerprint or
    /// authorization to overwrite local changes. Active file I/O is allowed.
    pub fn source_baseline(&self) -> Result<([u8; 32], u64)> {
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed,
            "source namespace commit failed; reopen for recovery"
        );
        let source = ns.state.source.as_ref().context("not a source namespace")?;
        Ok((source.root, source.generation))
    }
    /// A missing receipt is not proof that an earlier operation did not apply.
    /// Reconcile an uncertain durable intent with the exact same request.
    pub fn source_receipt(&self, request_id: &str) -> Result<Option<SourceReceipt>> {
        validate_request_id(request_id)?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed,
            "source namespace commit failed; reopen for recovery"
        );
        Ok(ns
            .state
            .source
            .as_ref()
            .and_then(|source| source.receipts.get(request_id))
            .map(|receipt| receipt.result.clone()))
    }
}

impl Tree {
    /// Trusted acknowledgment cleanup, not a proof of non-application. Retire
    /// only after the controller's durable jj ACK/WAL no longer references the
    /// exact receipt and no request/job can retry it. Retired IDs must never be
    /// reused. A missing receipt remains an unknown outcome, not NotApplied.
    /// Every supplied receipt must still match; otherwise nothing is removed.
    pub fn retire_source_receipts(
        &self,
        receipts: &[SourceReceipt],
        limits: &SourceLimits,
    ) -> Result<usize> {
        limits.validate()?;
        ensure!(
            receipts.len() <= limits.max_receipts,
            "source receipt retirement budget exceeded"
        );
        let mut ids = HashSet::new();
        for receipt in receipts {
            validate_request_id(&receipt.request_id)?;
            ensure!(
                ids.insert(receipt.request_id.as_str()),
                "duplicate source receipt retirement"
            );
        }
        let _freeze = self.source_freeze()?;
        let mut ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace unavailable or pending publication"
        );
        let source = ns.state.source.as_ref().context("not a source namespace")?;
        for receipt in receipts {
            ensure!(
                source
                    .receipts
                    .get(&receipt.request_id)
                    .is_some_and(|stored| stored.result == *receipt),
                "source receipt retirement precondition failed"
            );
        }
        if receipts.is_empty() {
            return Ok(0);
        }
        Self::change_locked(&mut ns, |state| {
            let source = state.source.as_mut().unwrap();
            for receipt in receipts {
                source.receipts.remove(&receipt.request_id);
            }
            Ok(receipts.len())
        })
    }
}
