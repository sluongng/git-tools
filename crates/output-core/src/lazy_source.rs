//! Explicit directory-at-a-time source namespaces. A source pin identifies a
//! baseline, not a remote retention promise. The configured provider owns
//! authorization, canonical verification and the lifetime of that baseline.
use super::*;
use anyhow::bail;
use source::{Freeze, Receipt};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourcePin {
    pub root: [u8; 32],
    pub generation: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceLoadLimits {
    pub max_concurrent: usize,
    /// Admitted physical loads, including work waiting for a concurrent slot.
    /// Additional callers wait asynchronously within their original deadline.
    pub max_pending: usize,
    pub max_directory_entries: usize,
    pub max_directory_bytes: usize,
    pub timeout_ms: u64,
}
impl Default for SourceLoadLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_pending: 16,
            max_directory_entries: 16_384,
            max_directory_bytes: 4 * 1024 * 1024,
            timeout_ms: 30_000,
        }
    }
}
impl SourceLoadLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=64).contains(&self.max_concurrent)
                && self.max_pending >= self.max_concurrent
                && self.max_pending <= 256
                && (1..=1_000_000).contains(&self.max_directory_entries)
                && (1024..=64 * 1024 * 1024).contains(&self.max_directory_bytes)
                && (1..=300_000).contains(&self.timeout_ms),
            "invalid lazy source load limits"
        );
        Ok(())
    }
}
/// These calls must bound canonical reads/allocations before returning. The
/// provider returns the complete verified object for the requested directory;
/// an arbitrary slice of a tree is not authenticated by its whole-object ID.
#[async_trait]
pub trait SourceDirectoryProvider: Send + Sync {
    async fn read_directory(
        &self,
        pin: SourcePin,
        tree_id: [u8; 32],
        base_path: String,
        limits: SourceLoadLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<VerifiedSourceDirectory>;
    async fn read_symlink(
        &self,
        pin: SourcePin,
        object_id: [u8; 32],
        expected_size: u64,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<String>;
}
pub struct LazySourceRoot {
    pub identity: String,
    pub root_tree: [u8; 32],
    pub pin: SourcePin,
    pub provider: Arc<dyn SourceDirectoryProvider>,
    pub load_limits: SourceLoadLimits,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSourceDirectory {
    pub tree_id: [u8; 32],
    pub entries: Vec<SourceDirectoryEntry>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryEntry {
    pub name: String,
    pub mode: u32,
    pub kind: SourceDirectoryEntryKind,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceDirectoryEntryKind {
    Directory {
        tree_id: [u8; 32],
    },
    File {
        artifact: Artifact,
        copy_id: Option<[u8; 32]>,
    },
    Symlink {
        object_id: [u8; 32],
        target_size: u64,
    },
}
/// Original canonical position and identity survive moves and local copy-up.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceBaseOrigin {
    pub path: String,
    pub parent_tree: [u8; 32],
    pub mode: u32,
    pub kind: SourceDirectoryEntryKind,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceNamespaceChange {
    MakeDirectory {
        path: String,
    },
    Remove {
        path: String,
        directory: bool,
        origin: Option<SourceBaseOrigin>,
    },
    Move {
        from: String,
        to: String,
        origin: Option<SourceBaseOrigin>,
    },
}
#[derive(Clone, Debug)]
pub struct SourceDeltaEntry {
    pub path: String,
    pub node: Node,
    pub target: Option<String>,
    pub origin: Option<SourceBaseOrigin>,
}
/// A frozen delta, not a partial complete inventory. Namespace changes are in
/// user-operation order. Apply them to the baseline, then overlay `entries`.
/// Unvisited directories remain represented by their immutable base tree IDs.
pub struct SourceDeltaSnapshot {
    tree: Arc<Tree>,
    _freeze: Freeze,
    baseline: SourcePin,
    baseline_tree: [u8; 32],
    version: SourceVersion,
    changes: Vec<SourceNamespaceChange>,
    entries: Vec<SourceDeltaEntry>,
    directories: Vec<SourceDirectoryLink>,
    limits: SourceLimits,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDirectoryCursor {
    pub pin: SourcePin,
    pub directory_generation: u64,
    pub after: u64,
}
#[derive(Clone, Debug)]
pub struct SourceDirectoryLink {
    pub path: String,
    pub tree_id: [u8; 32],
    pub loaded: bool,
}
pub struct SourceBaselineUpdate {
    pub path: String,
    pub directory: VerifiedSourceDirectory,
    /// Canonical provider-verified targets, keyed by direct entry name. A
    /// directory descriptor alone cannot prove agreement with local targets.
    pub symlink_targets: BTreeMap<String, String>,
}
#[derive(Debug)]
pub struct SourceDirectoryPage {
    pub entries: Vec<Node>,
    pub next: Option<SourceDirectoryCursor>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceNotLoaded;
impl std::fmt::Display for SourceNotLoaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source directory or symlink has not been loaded")
    }
}
impl std::error::Error for SourceNotLoaded {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceLoadBusy;
impl std::fmt::Display for SourceLoadBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("lazy source metadata admission is busy")
    }
}
impl std::error::Error for SourceLoadBusy {}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct LazyMeta {
    pub pin: SourcePin,
    pub root_tree: [u8; 32],
    pub load_limits: SourceLoadLimits,
    pub limits: SourceLimits,
    pub changes: Vec<SourceNamespaceChange>,
    pub change_bytes: usize,
    pub resident_entries: usize,
    pub resident_bytes: usize,
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct DirectoryBase {
    pub tree_id: [u8; 32],
    pub loaded: bool,
}
pub(super) struct Runtime {
    provider: Arc<dyn SourceDirectoryProvider>,
    pin: SourcePin,
    slots: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    flights: Mutex<HashMap<u64, Arc<Flight>>>,
}
// Queued calls and physical loads never copy the potentially large edit journal.
#[derive(Clone)]
struct LoadContext {
    pin: SourcePin,
    load_limits: SourceLoadLimits,
    limits: SourceLimits,
}
struct Flight {
    result: Mutex<Option<std::result::Result<(), LoadFailure>>>,
    ready: Notify,
}
#[derive(Clone)]
enum LoadFailure {
    Core(CoreError),
    Busy,
    NotLoaded,
    Timeout,
    Limit(CacheLimitError),
    Other(String),
}
impl LoadFailure {
    fn capture(error: anyhow::Error) -> Self {
        if let Some(kind) = error.downcast_ref::<CoreError>() {
            Self::Core(*kind)
        } else if error.downcast_ref::<SourceLoadBusy>().is_some() {
            Self::Busy
        } else if error.downcast_ref::<SourceNotLoaded>().is_some() {
            Self::NotLoaded
        } else if error.downcast_ref::<CacheTimeoutError>().is_some() {
            Self::Timeout
        } else if let Some(limit) = error.downcast_ref::<CacheLimitError>() {
            Self::Limit(limit.clone())
        } else {
            Self::Other(error.to_string().chars().take(1024).collect())
        }
    }
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Core(kind) => kind.into(),
            Self::Busy => SourceLoadBusy.into(),
            Self::NotLoaded => SourceNotLoaded.into(),
            Self::Timeout => CacheTimeoutError.into(),
            Self::Limit(limit) => limit.into(),
            Self::Other(message) => anyhow::anyhow!(message),
        }
    }
}

pub(super) fn require_loaded(state: &State, id: u64) -> Result<()> {
    if record(state, id)?
        .source_directory
        .as_ref()
        .is_some_and(|base| !base.loaded)
    {
        return Err(SourceNotLoaded.into());
    }
    Ok(())
}
fn lazy(state: &State) -> Result<&LazyMeta> {
    state
        .source
        .as_ref()
        .and_then(|s| s.lazy.as_ref())
        .context("not a lazy source namespace")
}
fn checked_path(path: &str, limits: &SourceLimits) -> Result<()> {
    ensure!(
        path.len() <= limits.max_path_bytes,
        "source path limit exceeded"
    );
    let names = parts(path)?;
    ensure!(
        names.len() <= limits.max_depth,
        "source depth limit exceeded"
    );
    for name in names {
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
                .any(|s| stem.eq_ignore_ascii_case(s))
                && !(stem.len() == 4
                    && (stem.get(..3).is_some_and(
                        |p| p.eq_ignore_ascii_case("COM") || p.eq_ignore_ascii_case("LPT")
                    ))
                    && matches!(stem.as_bytes()[3], b'1'..=b'9')),
            "reserved source path"
        );
    }
    Ok(())
}
pub(super) fn validate_reopen(state: &State) -> Result<()> {
    let Some(meta) = state.source.as_ref().and_then(|s| s.lazy.as_ref()) else {
        ensure!(
            state.version != 5
                && state
                    .source
                    .as_ref()
                    .is_none_or(|s| s.access.is_read_write()),
            "immutable candidate lacks lazy source"
        );
        ensure!(
            state
                .nodes
                .values()
                .all(|r| r.source_origin.is_none() && r.source_directory.is_none()),
            "lazy records lack source root"
        );
        return Ok(());
    };
    ensure!(
        matches!(state.version, 4 | 5) && meta.pin.root != [0; 32] && meta.root_tree != [0; 32],
        "lazy source format mismatch"
    );
    match state.source.as_ref().unwrap().access {
        SourceAccess::ReadWrite => ensure!(state.version == 4, "immutable source policy missing"),
        SourceAccess::ImmutableCandidate { root_tree } => {
            ensure!(
                state.version == 5
                    && root_tree == meta.root_tree
                    && root_tree == meta.pin.root
                    && meta.changes.is_empty()
                    && meta.change_bytes == 0
                    && state.pending.map.is_empty(),
                "immutable candidate namespace mismatch"
            );
        }
    }
    meta.load_limits.validate()?;
    meta.limits.validate()?;
    ensure!(
        meta.pin.generation > 0
            && meta.pin.generation == state.source.as_ref().unwrap().generation
            && state.source.as_ref().unwrap().revision > 0
            && state.source.as_ref().unwrap().root == meta.root_tree,
        "lazy source generation mismatch"
    );
    let mut bytes = 0usize;
    ensure!(
        meta.changes.len() <= meta.limits.max_entries,
        "lazy journal entry limit exceeded"
    );
    for change in &meta.changes {
        bytes = bytes
            .checked_add(serde_json::to_vec(change)?.len().saturating_add(16))
            .context("lazy journal size overflow")?;
    }
    ensure!(
        bytes == meta.change_bytes && bytes <= meta.limits.max_metadata_bytes,
        "lazy journal size mismatch"
    );
    let actual_bytes = state.nodes.values().try_fold(0usize, |sum, record| {
        sum.checked_add(record_charge(record)?)
            .context("lazy resident size overflow")
    })?;
    ensure!(
        state.nodes.map.len() == meta.resident_entries
            && actual_bytes == meta.resident_bytes
            && actual_bytes <= meta.limits.max_metadata_bytes
            && meta.resident_entries <= meta.limits.max_entries.saturating_add(1),
        "lazy resident accounting mismatch"
    );
    let root = record(state, 1)?
        .source_directory
        .as_ref()
        .context("lazy root link missing")?;
    ensure!(root.tree_id == meta.root_tree, "lazy root tree mismatch");
    for r in state.nodes.values() {
        if let Some(origin) = &r.source_origin {
            checked_path(&origin.path, &meta.limits)?;
        }
        if let Some(directory) = &r.source_directory {
            ensure!(
                r.node.kind == NodeKind::Directory && directory.tree_id != [0; 32],
                "invalid lazy directory record"
            );
            if !directory.loaded {
                for ((_, id), _) in state.children.range((r.node.id, 0)..=(r.node.id, u64::MAX)) {
                    let child = record(state, *id)?;
                    ensure!(
                        r.node.id == 1 && source::control_name(&child.node.name),
                        "unloaded source directory has published children"
                    );
                }
            }
        }
    }
    Ok(())
}
pub(super) fn track_changes(previous: &State, next: &mut State) -> Result<()> {
    let Some(old) = previous.source.as_ref().and_then(|s| s.lazy.as_ref()) else {
        return Ok(());
    };
    let Some(current) = next.source.as_ref().and_then(|s| s.lazy.as_ref()) else {
        return Ok(());
    };
    if old.pin != current.pin {
        return Ok(());
    }
    let mut changes = Vec::new();
    for id in &next.nodes.dirty {
        if *id == 1 {
            continue;
        }
        let before = previous.nodes.get(id);
        let after = next.nodes.get(id);
        if let Some(before) = before.filter(|r| r.node.linked) {
            let from = path_for(previous, *id)?;
            if from.split('/').next().is_some_and(source::control_name) {
                continue;
            }
            checked_path(&from, &old.limits)?;
            if after.is_none_or(|r| !r.node.linked) {
                changes.push(SourceNamespaceChange::Remove {
                    path: from,
                    directory: before.node.kind == NodeKind::Directory,
                    origin: before.source_origin.clone(),
                });
            }
        }
    }
    for id in &next.nodes.dirty {
        if *id == 1 {
            continue;
        }
        let before = previous.nodes.get(id);
        let Some(after) = next.nodes.get(id).filter(|r| r.node.linked) else {
            continue;
        };
        let path = path_for(next, *id)?;
        if path.split('/').next().is_some_and(source::control_name) {
            continue;
        }
        checked_path(&path, &old.limits)?;
        if let Some(before) = before.filter(|r| r.node.linked) {
            // Parent moves imply descendant path changes, but only the directly
            // renamed inode emits a move. Its untouched subtree stays a link.
            if before.node.parent != after.node.parent || before.node.name != after.node.name {
                if after.node.kind == NodeKind::Directory {
                    let mut pending = vec![(*id, path.clone())];
                    while let Some((parent, prefix)) = pending.pop() {
                        for ((_, child_id), _) in
                            next.children.range((parent, 0)..=(parent, u64::MAX))
                        {
                            let child = record(next, *child_id)?;
                            let child_path = format!("{prefix}/{}", child.node.name);
                            checked_path(&child_path, &old.limits)?;
                            if child.node.kind == NodeKind::Directory {
                                pending.push((*child_id, child_path));
                            }
                        }
                    }
                }
                changes.push(SourceNamespaceChange::Move {
                    from: path_for(previous, *id)?,
                    to: path,
                    origin: before.source_origin.clone(),
                });
            }
        } else if after.node.kind == NodeKind::Directory {
            changes.push(SourceNamespaceChange::MakeDirectory { path });
        }
    }
    let meta = next.source.as_mut().unwrap().lazy.as_mut().unwrap();
    for change in changes {
        ensure!(
            meta.changes.len() < meta.limits.max_entries,
            "lazy journal entry limit exceeded"
        );
        let bytes = serde_json::to_vec(&change)?
            .len()
            .checked_add(16)
            .context("lazy journal size overflow")?;
        meta.change_bytes = meta
            .change_bytes
            .checked_add(bytes)
            .context("lazy journal size overflow")?;
        ensure!(
            meta.change_bytes <= meta.limits.max_metadata_bytes,
            "lazy journal byte limit exceeded"
        );
        meta.changes.push(change);
    }
    Ok(())
}
// Conservative upper bound for the resident Record and its indexes, not just
// the wire entry. File locators occur in both Node.remote and base provenance;
// each origin also owns the complete canonical prefix. Six bytes per input
// byte covers JSON string escaping, and fixed slack covers numeric/ID fields.
fn directory_charge(entry: &SourceDirectoryEntry, base_path: &str) -> Result<usize> {
    let artifact_bytes = match &entry.kind {
        SourceDirectoryEntryKind::File { artifact, .. } => artifact
            .backend
            .len()
            .checked_add(artifact.instance.len())
            .and_then(|n| n.checked_add(artifact.hash.len()))
            .and_then(|n| n.checked_mul(2))
            .context("directory charge overflow")?,
        _ => 0,
    };
    entry
        .name
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(base_path.len()))
        .and_then(|n| n.checked_add(usize::from(!base_path.is_empty())))
        .and_then(|n| n.checked_add(artifact_bytes))
        .and_then(|n| n.checked_mul(6))
        .and_then(|n| n.checked_add(2048))
        .context("directory charge overflow")
}

fn checked_directory(
    tree: &Tree,
    dir: &VerifiedSourceDirectory,
    id: [u8; 32],
    base_path: &str,
    limits: &SourceLimits,
    load_limits: &SourceLoadLimits,
) -> Result<()> {
    dir.validate_for_projection(id, base_path, limits, load_limits, &tree.backend)
}
impl VerifiedSourceDirectory {
    /// Validate a whole canonical directory before authorizing its file
    /// locators. This is structural projection validation, not root retention.
    pub fn validate_for_projection(
        &self,
        id: [u8; 32],
        base_path: &str,
        limits: &SourceLimits,
        load_limits: &SourceLoadLimits,
        backend: &str,
    ) -> Result<()> {
        limits.validate()?;
        load_limits.validate()?;
        checked_path(base_path, limits)?;
        ensure!(
            self.entries.len() <= limits.max_entries,
            "source directory entry limit exceeded"
        );
        ensure!(
            self.tree_id == id && self.entries.len() <= load_limits.max_directory_entries,
            "invalid lazy directory identity or count"
        );
        let mut previous: Option<&str> = None;
        let mut bytes = 0usize;
        let mut aliases = HashSet::new();
        for entry in &self.entries {
            component(&entry.name)?;
            ensure!(
                previous.is_none_or(|p| p.as_bytes() < entry.name.as_bytes()),
                "directory names must be canonical and unique"
            );
            ensure!(
                aliases.insert(entry.name.to_lowercase()),
                "case-aliased source entries"
            );
            previous = Some(&entry.name);
            ensure!(
                !base_path.is_empty() || !source::control_name(&entry.name),
                "reserved source administration entry"
            );
            let path = if base_path.is_empty() {
                entry.name.clone()
            } else {
                format!("{base_path}/{}", entry.name)
            };
            checked_path(&path, limits)?;
            ensure!(entry.mode & !0o777 == 0, "invalid source mode");
            match &entry.kind {
                SourceDirectoryEntryKind::Directory { tree_id } => {
                    ensure!(*tree_id != [0; 32], "invalid source directory identity")
                }
                SourceDirectoryEntryKind::File { artifact, .. } => {
                    artifact.validate()?;
                    ensure!(artifact.backend == backend, "CAS backend identity mismatch");
                    ensure!(artifact.digest_function == 9, "source files require BLAKE3");
                }
                SourceDirectoryEntryKind::Symlink {
                    object_id,
                    target_size,
                } => ensure!(
                    *object_id != [0; 32]
                        && *target_size <= limits.max_path_bytes as u64
                        && entry.mode == 0o777,
                    "invalid source symlink"
                ),
            }
            bytes = bytes
                .checked_add(directory_charge(entry, base_path)?)
                .context("directory size overflow")?;
            ensure!(
                bytes <= load_limits.max_directory_bytes && bytes <= limits.max_metadata_bytes,
                "lazy directory byte budget exceeded"
            );
        }
        Ok(())
    }
}

fn source_version(
    tree: &Tree,
    state: &State,
    entries: &[SourceDeltaEntry],
) -> Result<SourceVersion> {
    let meta = lazy(state)?;
    let source = state.source.as_ref().unwrap();
    let mut hash = blake3::Hasher::new();
    hash.update(b"output-core-lazy-source-snapshot-v1\0");
    hash.update(tree.identity.as_bytes());
    hash.update(&meta.pin.root);
    hash.update(&meta.root_tree);
    hash.update(&source.generation.to_le_bytes());
    hash.update(&source.revision.to_le_bytes());
    hash.update(&serde_json::to_vec(&meta.changes)?);
    for entry in entries {
        hash.update(&serde_json::to_vec(&(
            &entry.path,
            &entry.node,
            &entry.target,
            &entry.origin,
        ))?);
        if entry.node.kind == NodeKind::File && entry.node.remote.is_none() {
            let (digest, size) = hash_file_until(&tree.local_path(entry.node.id), 9, None)?;
            ensure!(size == entry.node.size, "local source payload size changed");
            hash.update(digest.as_bytes());
        }
    }
    Ok(SourceVersion {
        generation: source.generation,
        revision: source.revision,
        fingerprint: *hash.finalize().as_bytes(),
    })
}
fn dirty_entries(state: &State, limits: &SourceLimits) -> Result<Vec<SourceDeltaEntry>> {
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    let mut local_bytes = 0u64;
    for r in state
        .nodes
        .values()
        .filter(|r| r.node.linked && r.node.id != 1)
    {
        let path = path_for(state, r.node.id)?;
        if path.split('/').next().is_some_and(source::control_name) {
            continue;
        }
        let dirty = r.source_origin.as_ref().is_none_or(|origin| {
            r.node.kind == NodeKind::File
                && ((r.node.mode & 0o111 != 0) != (origin.mode & 0o111 != 0)
                    || r.source_clean_ctime != Some(r.node.ctime))
        });
        if !dirty {
            continue;
        }
        checked_path(&path, limits)?;
        ensure!(
            entries.len() < limits.max_entries,
            "source delta entry limit exceeded"
        );
        bytes = bytes
            .checked_add(serde_json::to_vec(&(&path, &r.node, &r.target, &r.source_origin))?.len())
            .context("source delta byte overflow")?;
        ensure!(
            bytes <= limits.max_metadata_bytes,
            "source delta metadata limit exceeded"
        );
        if r.node.kind == NodeKind::File && r.node.remote.is_none() {
            local_bytes = local_bytes
                .checked_add(r.node.size)
                .context("source delta local size overflow")?;
            ensure!(
                local_bytes <= limits.max_local_bytes,
                "source delta local byte limit exceeded"
            );
        }
        entries.push(SourceDeltaEntry {
            path,
            node: r.node.clone(),
            target: r.target.clone(),
            origin: r.source_origin.clone(),
        });
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

impl Tree {
    /// Install a new lazy generation without fetching any directory. Replacement
    /// is explicit and uses the same exact frozen-version CAS as full sources.
    pub fn install_lazy_source(
        &self,
        root: LazySourceRoot,
        expected: Option<SourceVersion>,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        let _freeze = self.source_freeze()?;
        self.install_lazy_source_frozen(root, expected, request_id, limits)
    }
    pub fn install_immutable_lazy_source(
        &self,
        root: LazySourceRoot,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        let _freeze = self.source_freeze()?;
        let access = SourceAccess::ImmutableCandidate {
            root_tree: root.root_tree,
        };
        self.install_lazy_source_frozen_access(root, None, request_id, limits, access)
    }
    fn install_lazy_source_frozen(
        &self,
        root: LazySourceRoot,
        expected: Option<SourceVersion>,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        self.install_lazy_source_frozen_access(
            root,
            expected,
            request_id,
            limits,
            SourceAccess::ReadWrite,
        )
    }
    fn install_lazy_source_frozen_access(
        &self,
        root: LazySourceRoot,
        expected: Option<SourceVersion>,
        request_id: &str,
        limits: &SourceLimits,
        access: SourceAccess,
    ) -> Result<SourceReceipt> {
        limits.validate()?;
        root.load_limits.validate()?;
        source::validate_request_id(request_id)?;
        ensure!(
            root.identity == self.identity && root.root_tree != [0; 32] && root.pin.root != [0; 32],
            "lazy source identity mismatch"
        );
        if let SourceAccess::ImmutableCandidate { root_tree } = access {
            require(
                root_tree == root.root_tree && root.pin.root == root_tree && expected.is_none(),
                CoreError::ReadOnly,
            )?;
        }
        let intent_tag = if access.is_read_write() {
            "install-lazy-v1"
        } else {
            "install-immutable-lazy-v1"
        };
        let intent = *blake3::hash(&serde_json::to_vec(&(
            intent_tag,
            &root.identity,
            root.root_tree,
            &root.pin,
            &root.load_limits,
            &expected,
        ))?)
        .as_bytes();
        let result = {
            let mut ns = self.namespace.lock().unwrap();
            ensure!(
                !ns.failed && ns.state.pending.map.is_empty(),
                "source namespace unavailable or pending publication"
            );
            if let Some(existing) = &ns.state.source {
                require(existing.access == access, CoreError::ReadOnly)?;
            }
            let replay = ns
                .state
                .source
                .as_ref()
                .and_then(|s| s.receipts.get(request_id));
            if let Some(receipt) = replay {
                ensure!(
                    receipt.intent == intent
                        && receipt.result.operation == SourceOperation::Install,
                    "source request ID reused with different intent"
                );
                receipt.result.clone()
            } else {
                require(
                    ns.state
                        .source
                        .as_ref()
                        .is_none_or(|s| s.access.is_read_write()),
                    CoreError::ReadOnly,
                )?;
                require(
                    access.is_read_write() || ns.state.source.is_none(),
                    CoreError::ReadOnly,
                )?;
                let (generation, revision, mut receipts) = match &ns.state.source {
                    Some(source) => {
                        let actual = if source.lazy.is_some() {
                            let entries = dirty_entries(&ns.state, limits)?;
                            source_version(self, &ns.state, &entries)?
                        } else {
                            self.source_inventory(&ns.state, limits)?.1
                        };
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
                            "initial lazy source install requires empty namespace"
                        );
                        (1, 1, BTreeMap::new())
                    }
                };
                ensure!(
                    root.pin.generation == generation && receipts.len() < limits.max_receipts,
                    "lazy source generation or receipt limit mismatch"
                );
                let old: Vec<_> = ns
                    .state
                    .nodes
                    .values()
                    .filter(|r| r.node.linked && r.node.id != 1)
                    .map(|r| Ok((r.node.id, path_for(&ns.state, r.node.id)?)))
                    .collect::<Result<_>>()?;
                Self::change_locked_mode(&mut ns, true, |state| {
                    for (id, path) in &old {
                        if !path.split('/').next().is_some_and(source::control_name) {
                            state.nodes.get_mut(id).unwrap().node.linked = false;
                        }
                    }
                    state.version = if access.is_read_write() { 4 } else { 5 };
                    let record = state.nodes.get_mut(&1).unwrap();
                    record.source_directory = Some(DirectoryBase {
                        tree_id: root.root_tree,
                        loaded: false,
                    });
                    record.source_origin = None;
                    record.source_clean_ctime = None;
                    touch(state, 1);
                    state.source = Some(source::SourceMeta {
                        access,
                        root: root.root_tree,
                        generation,
                        revision,
                        receipts: BTreeMap::new(),
                        lazy: Some(LazyMeta {
                            pin: root.pin.clone(),
                            root_tree: root.root_tree,
                            load_limits: root.load_limits.clone(),
                            limits: limits.clone(),
                            changes: Vec::new(),
                            change_bytes: 0,
                            resident_entries: 0,
                            resident_bytes: 0,
                        }),
                    });
                    let entries = dirty_entries(state, limits)?;
                    let version = source_version(self, state, &entries)?;
                    let result = SourceReceipt {
                        request_id: request_id.into(),
                        operation: SourceOperation::Install,
                        intent,
                        root: root.root_tree,
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
                })?
            }
        };
        if self
            .namespace
            .lock()
            .unwrap()
            .state
            .source
            .as_ref()
            .and_then(|s| s.lazy.as_ref())
            .is_some_and(|meta| meta.pin == root.pin)
        {
            self.bind_lazy_runtime(root.provider, root.pin, root.load_limits);
        }
        Ok(result)
    }
    fn bind_lazy_runtime(
        &self,
        provider: Arc<dyn SourceDirectoryProvider>,
        pin: SourcePin,
        limits: SourceLoadLimits,
    ) {
        *self.lazy_runtime.lock().unwrap() = Some(Arc::new(Runtime {
            provider,
            pin,
            slots: Arc::new(Semaphore::new(limits.max_concurrent)),
            pending: Arc::new(Semaphore::new(limits.max_pending)),
            flights: Mutex::new(HashMap::new()),
        }));
    }
    /// Rebind a trusted provider after reopening; never changes the persisted
    /// baseline, journal or cache coverage. No provider identity is inferred.
    /// All persisted immutable file locators, including retired entries and
    /// copy-up origins. This is restart authorization metadata, not an inventory.
    pub fn lazy_source_artifacts(&self) -> Result<Vec<Artifact>> {
        let ns = self.namespace.lock().unwrap();
        ensure!(!ns.failed, "source namespace unavailable");
        let meta = lazy(&ns.state)?;
        let mut artifacts = HashSet::new();
        let mut bytes = 0usize;
        for record in ns.state.nodes.values() {
            for artifact in record
                .node
                .remote
                .iter()
                .chain(
                    record
                        .source_origin
                        .iter()
                        .filter_map(|origin| match &origin.kind {
                            SourceDirectoryEntryKind::File { artifact, .. } => Some(artifact),
                            _ => None,
                        }),
                )
            {
                if artifacts.insert(artifact.clone()) {
                    ensure!(
                        artifacts.len() <= meta.limits.max_entries,
                        "source artifact count exceeded"
                    );
                    bytes = bytes
                        .checked_add(serde_json::to_vec(artifact)?.len())
                        .context("source artifact size overflow")?;
                    ensure!(
                        bytes <= meta.limits.max_metadata_bytes,
                        "source artifact metadata exceeded"
                    );
                }
            }
        }
        Ok(artifacts.into_iter().collect())
    }
    /// Bounded known inode records for native invalidation, not a recursively
    /// complete lazy namespace. Retired/control/root records remain included.
    pub fn known_source_nodes(&self, limits: &SourceLimits) -> Result<Vec<Node>> {
        limits.validate()?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.source.is_some(),
            "source namespace unavailable"
        );
        ensure!(
            ns.state.nodes.map.len() <= limits.max_entries.saturating_add(1),
            "source known inode count exceeded"
        );
        let mut bytes = 0usize;
        let mut nodes = Vec::new();
        for record in ns.state.nodes.values() {
            bytes = bytes
                .checked_add(serde_json::to_vec(&record.node)?.len())
                .context("source known inode size overflow")?;
            ensure!(
                bytes <= limits.max_metadata_bytes,
                "source known inode metadata exceeded"
            );
            nodes.push(record.node.clone());
        }
        Ok(nodes)
    }
    pub fn attach_lazy_source(
        &self,
        provider: Arc<dyn SourceDirectoryProvider>,
        pin: SourcePin,
    ) -> Result<()> {
        let _freeze = self.source_freeze()?;
        let meta = {
            let ns = self.namespace.lock().unwrap();
            lazy(&ns.state)?.clone()
        };
        ensure!(meta.pin == pin, "lazy source provider pin mismatch");
        self.bind_lazy_runtime(provider, pin, meta.load_limits);
        Ok(())
    }
    pub fn lazy_source_pin(&self) -> Option<SourcePin> {
        self.namespace
            .lock()
            .unwrap()
            .state
            .source
            .as_ref()
            .and_then(|s| s.lazy.as_ref())
            .map(|s| s.pin.clone())
    }
    pub async fn ensure_directory_loaded(&self, inode: u64) -> Result<()> {
        let started = tokio::time::Instant::now();
        let meta = {
            let ns = self.namespace.lock().unwrap();
            ensure!(
                !ns.failed,
                "source namespace unavailable; reopen for recovery"
            );
            directory(&ns.state, inode)?;
            let record = record(&ns.state, inode)?;
            require(record.node.linked, CoreError::Stale)?;
            if record.source_directory.as_ref().is_none_or(|d| d.loaded) {
                return Ok(());
            }
            let current = lazy(&ns.state)?;
            LoadContext {
                pin: current.pin.clone(),
                load_limits: current.load_limits.clone(),
                limits: current.limits.clone(),
            }
        };
        let runtime = self
            .lazy_runtime
            .lock()
            .unwrap()
            .clone()
            .ok_or(SourceNotLoaded)?;
        require(runtime.pin == meta.pin, CoreError::Stale)?;
        let deadline = started + Duration::from_millis(meta.load_limits.timeout_ms);
        let existing = runtime.flights.lock().unwrap().get(&inode).cloned();
        let flight = if let Some(flight) = existing {
            flight
        } else {
            let pending =
                tokio::time::timeout_at(deadline, runtime.pending.clone().acquire_owned())
                    .await
                    .map_err(|_| CacheTimeoutError)??;
            ensure!(tokio::time::Instant::now() < deadline, CacheTimeoutError);
            // Admission can wait across cancellation, loading or root replacement.
            // Establish a physical owner before rechecking so replacement cannot
            // race the checks and start a request against a retired provider.
            let job = self.source_job()?;
            {
                let ns = self.namespace.lock().unwrap();
                ensure!(
                    !ns.failed,
                    "source namespace unavailable; reopen for recovery"
                );
                require(lazy(&ns.state)?.pin == meta.pin, CoreError::Stale)?;
                let record = record(&ns.state, inode)?;
                require(record.node.linked, CoreError::Stale)?;
                directory(&ns.state, inode)?;
                if record.source_directory.as_ref().is_none_or(|d| d.loaded) {
                    return Ok(());
                }
            }
            require(
                self.lazy_runtime
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &runtime)),
                CoreError::Stale,
            )?;
            let mut flights = runtime.flights.lock().unwrap();
            if let Some(flight) = flights.get(&inode) {
                flight.clone()
            } else {
                let tree = self
                    .self_ref
                    .upgrade()
                    .context("source namespace owner is gone")?;
                let flight = Arc::new(Flight {
                    result: Mutex::new(None),
                    ready: Notify::new(),
                });
                flights.insert(inode, flight.clone());
                let running = flight.clone();
                let owned = runtime.clone();
                tokio::spawn(async move {
                    let worker = owned.clone();
                    let work = tokio::spawn(async move {
                        let operation = async {
                            let permit = worker.slots.clone().acquire_owned().await?;
                            ensure!(tokio::time::Instant::now() < deadline, CacheTimeoutError);
                            let owner: Arc<dyn std::any::Any + Send + Sync> =
                                Arc::new((tree.clone(), job, pending, permit));
                            tree.load_directory(inode, &meta, worker.provider.clone(), owner)
                                .await
                        };
                        match tokio::time::timeout_at(deadline, operation).await {
                            Ok(result) => result,
                            Err(_) => Err(CacheTimeoutError.into()),
                        }
                    });
                    let result = work
                        .await
                        .unwrap_or_else(|_| Err(anyhow::anyhow!("source metadata worker failed")));
                    *running.result.lock().unwrap() = Some(result.map_err(LoadFailure::capture));
                    running.ready.notify_waiters();
                    owned.flights.lock().unwrap().remove(&inode);
                });
                flight
            }
        };
        tokio::time::timeout_at(deadline, async {
            loop {
                let notified = flight.ready.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(result) = flight.result.lock().unwrap().clone() {
                    return result.map_err(LoadFailure::into_error);
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| CacheTimeoutError)?
    }
    async fn load_directory(
        &self,
        inode: u64,
        meta: &LoadContext,
        provider: Arc<dyn SourceDirectoryProvider>,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<()> {
        let (base, base_path, node_generation) = {
            let ns = self.namespace.lock().unwrap();
            require(lazy(&ns.state)?.pin == meta.pin, CoreError::Stale)?;
            let record = record(&ns.state, inode)?;
            require(record.node.linked, CoreError::Stale)?;
            let base = record
                .source_directory
                .clone()
                .context("source directory base missing")?;
            if base.loaded {
                return Ok(());
            }
            (
                base,
                record
                    .source_origin
                    .as_ref()
                    .map_or(String::new(), |o| o.path.clone()),
                record.node.generation,
            )
        };
        let decoded = provider
            .read_directory(
                meta.pin.clone(),
                base.tree_id,
                base_path.clone(),
                meta.load_limits.clone(),
                owner.clone(),
            )
            .await?;
        checked_directory(
            self,
            &decoded,
            base.tree_id,
            &base_path,
            &meta.limits,
            &meta.load_limits,
        )?;
        let _gate = self.source_gate.lock().unwrap();
        let mut ns = self.namespace.lock().unwrap();
        Self::change_locked_mode(&mut ns, true, |state| {
            require(lazy(state)?.pin == meta.pin, CoreError::Stale)?;
            let current = record(state, inode)?;
            require(
                current.node.linked
                    && current.node.generation == node_generation
                    && current
                        .source_directory
                        .as_ref()
                        .is_some_and(|d| d.tree_id == base.tree_id),
                CoreError::Stale,
            )?;
            if current.source_directory.as_ref().unwrap().loaded {
                return Ok(());
            }
            ensure!(
                state
                    .nodes
                    .map
                    .len()
                    .checked_add(decoded.entries.len())
                    .is_some_and(|n| n <= meta.limits.max_entries.saturating_add(1)),
                "source namespace entry budget exceeded"
            );
            // Reserve the whole candidate expansion before allocating any
            // child Records. The namespace may have grown while provider I/O
            // ran, so this check uses the current resident counter.
            let candidate_bytes = decoded.entries.iter().try_fold(0usize, |sum, entry| {
                sum.checked_add(directory_charge(entry, &base_path)?)
                    .context("source directory expansion overflow")
            })?;
            ensure!(
                lazy(state)?
                    .resident_bytes
                    .checked_add(candidate_bytes)
                    .is_some_and(|bytes| bytes <= meta.limits.max_metadata_bytes),
                "source directory expansion exceeds resident metadata budget"
            );
            let current_path = path_for(state, inode)?;
            for entry in &decoded.entries {
                let visible_path = if current_path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{current_path}/{}", entry.name)
                };
                checked_path(&visible_path, &meta.limits)?;
                ensure!(
                    child(state, inode, &entry.name).is_none(),
                    "unknown source name collides with local entry"
                );
                let path = if base_path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{base_path}/{}", entry.name)
                };
                let (kind, remote, size) = match &entry.kind {
                    SourceDirectoryEntryKind::Directory { .. } => (NodeKind::Directory, None, 0),
                    SourceDirectoryEntryKind::File { artifact, .. } => {
                        (NodeKind::File, Some(artifact.clone()), artifact.size)
                    }
                    SourceDirectoryEntryKind::Symlink { target_size, .. } => {
                        (NodeKind::Symlink, None, *target_size)
                    }
                };
                let node = allocate(state, inode, &entry.name, kind, entry.mode, remote, true)?;
                let record = state.nodes.get_mut(&node.id).unwrap();
                record.node.size = size;
                record.source_origin = Some(SourceBaseOrigin {
                    path,
                    parent_tree: base.tree_id,
                    mode: entry.mode,
                    kind: entry.kind.clone(),
                });
                record.source_clean_ctime = Some(record.node.ctime);
                record.source_directory = match &entry.kind {
                    SourceDirectoryEntryKind::Directory { tree_id } => Some(DirectoryBase {
                        tree_id: *tree_id,
                        loaded: false,
                    }),
                    _ => None,
                };
            }
            state
                .nodes
                .get_mut(&inode)
                .unwrap()
                .source_directory
                .as_mut()
                .unwrap()
                .loaded = true;
            Ok(())
        })
    }
    pub async fn lookup_source(&self, parent: u64, name: &str) -> Result<Node> {
        self.ensure_directory_loaded(parent).await?;
        self.lookup(parent, name)
    }
    pub async fn list_source(&self, parent: u64, after: u64, limit: usize) -> Result<Vec<Node>> {
        self.ensure_directory_loaded(parent).await?;
        self.list(parent, after, limit)
    }
    pub async fn resolve_source(&self, path: &str) -> Result<Node> {
        let mut id = 1;
        for name in parts(path)? {
            id = self.lookup_source(id, name).await?.id;
        }
        self.get(id)
    }
    pub async fn list_source_page(
        &self,
        parent: u64,
        cursor: Option<SourceDirectoryCursor>,
        limit: usize,
    ) -> Result<SourceDirectoryPage> {
        ensure!(limit > 0, "source page limit must be positive");
        self.ensure_directory_loaded(parent).await?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed,
            "source namespace unavailable; reopen for recovery"
        );
        let meta = lazy(&ns.state)?;
        let directory = record(&ns.state, parent)?;
        if let Some(cursor) = &cursor {
            require(
                cursor.pin == meta.pin && cursor.directory_generation == directory.node.ctime,
                CoreError::Stale,
            )?;
        }
        let after = cursor.as_ref().map_or(0, |c| c.after);
        let mut entries: Vec<_> = ns
            .state
            .children
            .range((
                std::ops::Bound::Excluded((parent, after)),
                std::ops::Bound::Included((parent, u64::MAX)),
            ))
            .take(limit.saturating_add(1))
            .map(|((_, id), _)| ns.state.nodes.get(id).unwrap().node.clone())
            .collect();
        let more = entries.len() > limit;
        if more {
            entries.pop();
        }
        let next = if more {
            Some(SourceDirectoryCursor {
                pin: meta.pin.clone(),
                directory_generation: directory.node.ctime,
                after: entries.last().unwrap().id,
            })
        } else {
            None
        };
        Ok(SourceDirectoryPage { entries, next })
    }
    pub async fn readlink_source(&self, inode: u64) -> Result<String> {
        let started = tokio::time::Instant::now();
        let (origin, pin, timeout) = {
            let ns = self.namespace.lock().unwrap();
            ensure!(
                !ns.failed,
                "source namespace unavailable; reopen for recovery"
            );
            let r = record(&ns.state, inode)?;
            require(r.node.linked, CoreError::Stale)?;
            require(r.node.kind == NodeKind::Symlink, CoreError::InvalidInput)?;
            if let Some(target) = &r.target {
                return Ok(target.clone());
            }
            let meta = lazy(&ns.state)?;
            (
                r.source_origin
                    .clone()
                    .context("source symlink origin missing")?,
                meta.pin.clone(),
                meta.load_limits.timeout_ms,
            )
        };
        let SourceDirectoryEntryKind::Symlink {
            object_id,
            target_size,
        } = origin.kind.clone()
        else {
            return Err(CoreError::InvalidInput.into());
        };
        let runtime = self
            .lazy_runtime
            .lock()
            .unwrap()
            .clone()
            .ok_or(SourceNotLoaded)?;
        require(runtime.pin == pin, CoreError::Stale)?;
        let deadline = started + Duration::from_millis(timeout);
        let pending = tokio::time::timeout_at(deadline, runtime.pending.clone().acquire_owned())
            .await
            .map_err(|_| CacheTimeoutError)??;
        ensure!(tokio::time::Instant::now() < deadline, CacheTimeoutError);
        let tree = self.self_ref.upgrade().context("source owner gone")?;
        let job = self.source_job()?;
        {
            let ns = self.namespace.lock().unwrap();
            ensure!(
                !ns.failed,
                "source namespace unavailable; reopen for recovery"
            );
            require(lazy(&ns.state)?.pin == pin, CoreError::Stale)?;
            let record = record(&ns.state, inode)?;
            require(record.node.linked, CoreError::Stale)?;
            require(
                record.node.kind == NodeKind::Symlink,
                CoreError::InvalidInput,
            )?;
            ensure!(
                record.source_origin.as_ref() == Some(&origin),
                "source symlink changed"
            );
            if let Some(target) = &record.target {
                return Ok(target.clone());
            }
        }
        require(
            self.lazy_runtime
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &runtime)),
            CoreError::Stale,
        )?;
        let task = tokio::spawn(async move {
            let operation = async move {
                let permit = runtime.slots.clone().acquire_owned().await?;
                ensure!(tokio::time::Instant::now() < deadline, CacheTimeoutError);
                let owner: Arc<dyn std::any::Any + Send + Sync> =
                    Arc::new((tree.clone(), job, pending, permit));
                let target = runtime
                    .provider
                    .read_symlink(pin.clone(), object_id, target_size, owner.clone())
                    .await?;
                ensure!(
                    !target.is_empty()
                        && !target.contains('\0')
                        && target.len() as u64 == target_size,
                    "invalid source symlink target"
                );
                let _gate = tree.source_gate.lock().unwrap();
                let mut ns = tree.namespace.lock().unwrap();
                Tree::change_locked_mode(&mut ns, true, |state| {
                    require(lazy(state)?.pin == pin, CoreError::Stale)?;
                    let r = state
                        .nodes
                        .get_mut(&inode)
                        .context("source symlink disappeared")?;
                    require(r.node.linked, CoreError::Stale)?;
                    ensure!(
                        r.source_origin.as_ref() == Some(&origin),
                        "source symlink changed"
                    );
                    r.target = Some(target.clone());
                    Ok(target)
                })
            };
            tokio::time::timeout_at(deadline, operation)
                .await
                .map_err(|_| CacheTimeoutError)?
        });
        task.await.context("source symlink job failed")?
    }
    pub fn freeze_lazy_source(
        self: &Arc<Self>,
        _fence: SourceFrontendFence,
        limits: &SourceLimits,
    ) -> Result<SourceDeltaSnapshot> {
        limits.validate()?;
        let freeze = self.source_freeze()?;
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed && ns.state.pending.map.is_empty(),
            "source namespace unavailable or pending publication"
        );
        let meta = lazy(&ns.state)?;
        let entries = dirty_entries(&ns.state, limits)?;
        let version = source_version(self, &ns.state, &entries)?;
        let mut directories = Vec::new();
        for r in ns.state.nodes.values().filter(|r| r.node.linked) {
            if let Some(base) = &r.source_directory {
                directories.push(SourceDirectoryLink {
                    path: path_for(&ns.state, r.node.id)?,
                    tree_id: base.tree_id,
                    loaded: base.loaded,
                });
            }
        }
        directories.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(SourceDeltaSnapshot {
            tree: self.clone(),
            _freeze: freeze,
            baseline: meta.pin.clone(),
            baseline_tree: meta.root_tree,
            version,
            changes: meta.changes.clone(),
            entries,
            directories,
            limits: limits.clone(),
        })
    }
    pub async fn freeze_lazy_source_wait(
        self: &Arc<Self>,
        fence: SourceFrontendFence,
        limits: &SourceLimits,
        deadline: tokio::time::Instant,
    ) -> Result<SourceDeltaSnapshot> {
        let changed = self.source_gate.lock().unwrap().changed.clone();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SourceFreezeTimeout.into());
            }
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.freeze_lazy_source(fence, limits) {
                Ok(snapshot) => return Ok(snapshot),
                Err(e) if e.downcast_ref::<SourceFreezeBusy>().is_some() => {}
                Err(e) => return Err(e),
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| SourceFreezeTimeout)?;
        }
    }
}

fn record_charge(record: &Record) -> Result<usize> {
    Ok(serde_json::to_vec(record)?
        .len()
        .checked_add(64)
        .context("lazy record size overflow")?)
}
pub(super) fn account_records(previous: &State, next: &mut State) -> Result<()> {
    let Some(meta) = next.source.as_ref().and_then(|s| s.lazy.as_ref()) else {
        return Ok(());
    };
    let (mut count, mut bytes) = match previous.source.as_ref().and_then(|s| s.lazy.as_ref()) {
        Some(before) if before.pin == meta.pin => (before.resident_entries, before.resident_bytes),
        _ => {
            let mut count = 0usize;
            let mut bytes = 0usize;
            for record in previous.nodes.values() {
                count = count.checked_add(1).context("lazy record count overflow")?;
                bytes = bytes
                    .checked_add(record_charge(record)?)
                    .context("lazy record size overflow")?;
            }
            (count, bytes)
        }
    };
    for id in &next.nodes.dirty {
        if let Some(old) = previous.nodes.get(id) {
            count = count
                .checked_sub(1)
                .context("lazy record count underflow")?;
            bytes = bytes
                .checked_sub(record_charge(old)?)
                .context("lazy record size underflow")?;
        }
        if let Some(new) = next.nodes.get(id) {
            count = count.checked_add(1).context("lazy record count overflow")?;
            bytes = bytes
                .checked_add(record_charge(new)?)
                .context("lazy record size overflow")?;
        }
    }
    let meta = next.source.as_mut().unwrap().lazy.as_mut().unwrap();
    ensure!(
        count <= meta.limits.max_entries.saturating_add(1)
            && bytes <= meta.limits.max_metadata_bytes,
        "lazy namespace resident metadata budget exceeded"
    );
    meta.resident_entries = count;
    meta.resident_bytes = bytes;
    Ok(())
}

impl SourceDeltaSnapshot {
    pub fn baseline(&self) -> &SourcePin {
        &self.baseline
    }
    pub fn baseline_tree(&self) -> [u8; 32] {
        self.baseline_tree
    }
    pub fn version(&self) -> &SourceVersion {
        &self.version
    }
    pub fn changes(&self) -> &[SourceNamespaceChange] {
        &self.changes
    }
    pub fn entries(&self) -> &[SourceDeltaEntry] {
        &self.entries
    }
    /// Known directory links, not a complete recursively expanded inventory.
    pub fn directories(&self) -> &[SourceDirectoryLink] {
        &self.directories
    }
    pub fn read_local(&self, id: u64, offset: u64, count: usize) -> Result<Vec<u8>> {
        ensure!(
            count <= self.limits.max_metadata_bytes,
            "source delta read exceeds limit"
        );
        let entry = self
            .entries
            .iter()
            .find(|e| e.node.id == id)
            .context("source delta entry missing")?;
        ensure!(
            entry.node.kind == NodeKind::File && entry.node.remote.is_none(),
            "not a local source delta file"
        );
        read_range(&self.tree.local_path(id), offset, count)
    }
    pub fn install(
        self,
        root: LazySourceRoot,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        let expected = self.version.clone();
        self.install_with_expected(root, expected, request_id, limits)
    }
    /// Publish while retaining this snapshot's exclusion fence. Supplying the
    /// original expected version also permits exact durable receipt replay from
    /// a newly captured, later snapshot without a drop-and-refreeze race.
    pub fn install_with_expected(
        self,
        root: LazySourceRoot,
        expected: SourceVersion,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        self.tree
            .install_lazy_source_frozen(root, Some(expected), request_id, limits)
    }
    /// Advance a validated baseline without dropping local bytes or changing
    /// inode IDs. The caller supplies rebuilt canonical directories for every
    /// affected materialized ancestry path, and owns remote publication/retention.
    /// Untouched directory IDs are reused; excluded local entries stay local.
    pub fn acknowledge_root(
        self,
        root: LazySourceRoot,
        mut updates: Vec<SourceBaselineUpdate>,
        expected: SourceVersion,
        request_id: &str,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        self.tree.require_source_writable()?;
        limits.validate()?;
        root.load_limits.validate()?;
        source::validate_request_id(request_id)?;
        ensure!(
            root.identity == self.tree.identity
                && root.root_tree != [0; 32]
                && root.pin.root != [0; 32],
            "lazy source identity mismatch"
        );
        ensure!(
            root.pin.generation
                == expected
                    .generation
                    .checked_add(1)
                    .context("source generation exhausted")?,
            "lazy acknowledgement generation mismatch"
        );
        updates.sort_by(|a, b| a.path.cmp(&b.path));
        ensure!(
            updates.windows(2).all(|w| w[0].path != w[1].path),
            "duplicate source baseline directory"
        );
        let previous_meta = {
            let ns = self.tree.namespace.lock().unwrap();
            lazy(&ns.state)?.clone()
        };
        let mut new_meta = previous_meta.clone();
        new_meta.pin = root.pin.clone();
        new_meta.root_tree = root.root_tree;
        new_meta.load_limits = root.load_limits.clone();
        new_meta.limits = limits.clone();
        let mut total = 0usize;
        let mut count = 0usize;
        for update in &updates {
            checked_path(&update.path, limits)?;
            ensure!(
                update.symlink_targets.len() <= update.directory.entries.len(),
                "source symlink proof count exceeded"
            );
            for (name, target) in &update.symlink_targets {
                let entry = update
                    .directory
                    .entries
                    .iter()
                    .find(|entry| &entry.name == name)
                    .context("source symlink proof has no entry")?;
                let SourceDirectoryEntryKind::Symlink { target_size, .. } = &entry.kind else {
                    bail!("source target proof is not a symlink");
                };
                ensure!(
                    !target.is_empty()
                        && !target.contains('\0')
                        && target.len() as u64 == *target_size,
                    "source symlink target proof is invalid"
                );
                total = total
                    .checked_add(target.len().saturating_mul(6))
                    .context("source target proof byte overflow")?;
                ensure!(
                    total <= limits.max_metadata_bytes,
                    "source target proof byte budget exceeded"
                );
            }
            checked_directory(
                &self.tree,
                &update.directory,
                update.directory.tree_id,
                &update.path,
                &new_meta.limits,
                &new_meta.load_limits,
            )?;
            count = count
                .checked_add(update.directory.entries.len())
                .context("source rebase count overflow")?;
            ensure!(
                count <= limits.max_entries,
                "source rebase entry limit exceeded"
            );
            for entry in &update.directory.entries {
                total = total
                    .checked_add(directory_charge(entry, &update.path)?)
                    .context("source rebase byte overflow")?;
                ensure!(
                    total <= limits.max_metadata_bytes,
                    "source rebase byte limit exceeded"
                );
            }
        }
        let indexed: BTreeMap<_, _> = updates.iter().map(|u| (u.path.as_str(), u)).collect();
        let wire: Vec<_> = updates
            .iter()
            .map(|u| {
                (
                    &u.path,
                    &u.symlink_targets,
                    u.directory.tree_id,
                    u.directory
                        .entries
                        .iter()
                        .map(|e| (&e.name, e.mode, &e.kind))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        let intent = *blake3::hash(&serde_json::to_vec(&(
            "acknowledge-lazy-v1",
            &root.identity,
            root.root_tree,
            &root.pin,
            &root.load_limits,
            &expected,
            wire,
        ))?)
        .as_bytes();
        let result = {
            let mut ns = self.tree.namespace.lock().unwrap();
            ensure!(
                !ns.failed && ns.state.pending.map.is_empty(),
                "source namespace unavailable or pending publication"
            );
            let source = ns
                .state
                .source
                .as_ref()
                .context("source namespace missing")?;
            if let Some(receipt) = source.receipts.get(request_id) {
                ensure!(
                    receipt.intent == intent
                        && receipt.result.operation == SourceOperation::Acknowledge,
                    "source request ID reused with different intent"
                );
                receipt.result.clone()
            } else {
                ensure!(expected == self.version, "source delta snapshot is stale");
                ensure!(
                    source.receipts.len() < limits.max_receipts,
                    "source receipt limit exceeded"
                );
                ensure!(
                    lazy(&ns.state)?.pin == self.baseline,
                    "source baseline changed"
                );
                let actual_entries = dirty_entries(&ns.state, limits)?;
                ensure!(
                    source_version(&self.tree, &ns.state, &actual_entries)? == expected,
                    "source delta snapshot is stale"
                );
                if root.root_tree != self.baseline_tree || !updates.is_empty() {
                    ensure!(
                        indexed
                            .get("")
                            .is_some_and(|update| update.directory.tree_id == root.root_tree),
                        "changed source root lacks canonical directory update"
                    );
                }
                // Every touched current ancestry needs an authenticated final
                // directory. Otherwise clearing the operation journal could
                // silently resurrect a deletion in the unchanged baseline.
                let mut affected = HashSet::new();
                for change in &self.changes {
                    match change {
                        SourceNamespaceChange::Remove {
                            path,
                            origin: Some(_),
                            ..
                        } => {
                            affected.insert(
                                path.rsplit_once('/')
                                    .map_or("", |(parent, _)| parent)
                                    .to_owned(),
                            );
                        }
                        SourceNamespaceChange::Move {
                            from,
                            to,
                            origin: Some(_),
                        } => {
                            for path in [from, to] {
                                affected.insert(
                                    path.rsplit_once('/')
                                        .map_or("", |(parent, _)| parent)
                                        .to_owned(),
                                );
                            }
                        }
                        _ => {} // Purely local/untracked paths have no baseline effect.
                    }
                }
                for entry in self.entries.iter().filter(|entry| entry.origin.is_some()) {
                    affected.insert(
                        entry
                            .path
                            .rsplit_once('/')
                            .map_or("", |(parent, _)| parent)
                            .to_owned(),
                    );
                }
                for mut parent in affected {
                    loop {
                        match resolve_state(&ns.state, &parent) {
                            Ok(Some(id))
                                if record(&ns.state, id)?.node.kind == NodeKind::Directory =>
                            {
                                break;
                            }
                            Err(error) if error.downcast_ref::<SourceNotLoaded>().is_some() => {
                                return Err(error);
                            }
                            Err(error)
                                if error.downcast_ref::<CoreError>()
                                    != Some(&CoreError::NotDirectory) =>
                            {
                                return Err(error);
                            }
                            _ => {
                                ensure!(
                                    !parent.is_empty(),
                                    "source acknowledgement ancestry is missing"
                                );
                                parent = parent
                                    .rsplit_once('/')
                                    .map_or("", |(parent, _)| parent)
                                    .to_owned();
                            }
                        }
                    }
                    ensure!(
                        indexed.contains_key(parent.as_str()),
                        "source acknowledgement omits changed ancestry"
                    );
                }
                for update in &updates {
                    if !update.path.is_empty() {
                        let (parent, name) = update
                            .path
                            .rsplit_once('/')
                            .unwrap_or(("", update.path.as_str()));
                        let ancestor = indexed
                            .get(parent)
                            .context("source acknowledgement omits canonical parent")?;
                        ensure!(ancestor.directory.entries.iter().any(|entry| entry.name == name
                            && matches!(&entry.kind, SourceDirectoryEntryKind::Directory { tree_id } if *tree_id == update.directory.tree_id)),
                            "source acknowledgement directories are disconnected");
                    }
                }
                let mut replacement: Vec<(u64, SourceBaseOrigin, Option<DirectoryBase>)> =
                    Vec::new();
                let mut updated_dirs = Vec::new();
                for update in &updates {
                    let parent = resolve_state(&ns.state, &update.path)?
                        .context("source baseline directory is not materialized")?;
                    directory(&ns.state, parent)?;
                    require_loaded(&ns.state, parent)?;
                    let names: HashSet<_> = update
                        .directory
                        .entries
                        .iter()
                        .map(|e| e.name.as_str())
                        .collect();
                    for ((_, id), _) in ns.state.children.range((parent, 0)..=(parent, u64::MAX)) {
                        let current = record(&ns.state, *id)?;
                        if current.source_origin.is_some() {
                            ensure!(
                                names.contains(current.node.name.as_str()),
                                "source acknowledgement cannot drop a linked base entry"
                            );
                        }
                    }
                    for entry in &update.directory.entries {
                        let id = child(&ns.state, parent, &entry.name)
                            .context("canonical source entry is absent from working namespace")?;
                        let current = record(&ns.state, id)?;
                        let path = if update.path.is_empty() {
                            entry.name.clone()
                        } else {
                            format!("{}/{}", update.path, entry.name)
                        };
                        let base = match &entry.kind {
                            SourceDirectoryEntryKind::Directory { tree_id } => {
                                require(
                                    current.node.kind == NodeKind::Directory,
                                    CoreError::NotDirectory,
                                )?;
                                let loaded =
                                    current.source_directory.as_ref().is_none_or(|d| d.loaded);
                                if loaded
                                    && current
                                        .source_directory
                                        .as_ref()
                                        .is_none_or(|d| d.tree_id != *tree_id)
                                {
                                    ensure!(
                                        indexed
                                            .get(path.as_str())
                                            .is_some_and(|u| u.directory.tree_id == *tree_id),
                                        "changed materialized directory lacks canonical update"
                                    );
                                }
                                Some(DirectoryBase {
                                    tree_id: *tree_id,
                                    loaded,
                                })
                            }
                            SourceDirectoryEntryKind::File { artifact, .. } => {
                                require(
                                    current.node.kind == NodeKind::File,
                                    CoreError::InvalidInput,
                                )?;
                                ensure!(
                                    (current.node.mode & 0o111 != 0) == (entry.mode & 0o111 != 0),
                                    "canonical source executable mode differs"
                                );
                                if let Some(actual) = &current.node.remote {
                                    ensure!(
                                        actual == artifact,
                                        "canonical remote source file differs"
                                    );
                                } else {
                                    let (hash, size) =
                                        hash_file_until(&self.tree.local_path(id), 9, None)?;
                                    ensure!(
                                        hash == artifact.hash && size == artifact.size,
                                        "canonical local source file differs"
                                    );
                                }
                                None
                            }
                            SourceDirectoryEntryKind::Symlink {
                                object_id,
                                target_size,
                            } => {
                                require(
                                    current.node.kind == NodeKind::Symlink,
                                    CoreError::InvalidInput,
                                )?;
                                ensure!(
                                    current.node.size == *target_size,
                                    "canonical source symlink size differs"
                                );
                                let unchanged = current.source_origin.as_ref().is_some_and(|origin| matches!(
                                    &origin.kind, SourceDirectoryEntryKind::Symlink { object_id: previous, target_size: previous_size }
                                        if previous == object_id && previous_size == target_size));
                                if let Some(proof) = update.symlink_targets.get(&entry.name) {
                                    ensure!(
                                        current
                                            .target
                                            .as_ref()
                                            .is_some_and(|target| target == proof)
                                            || (unchanged && current.target.is_none()),
                                        "canonical source symlink target differs"
                                    );
                                } else {
                                    ensure!(
                                        unchanged,
                                        "changed source symlink requires canonical target proof"
                                    );
                                }
                                None
                            }
                        };
                        replacement.push((
                            id,
                            SourceBaseOrigin {
                                path,
                                parent_tree: update.directory.tree_id,
                                mode: entry.mode,
                                kind: entry.kind.clone(),
                            },
                            base,
                        ));
                    }
                    updated_dirs.push((parent, update.directory.tree_id));
                }
                Tree::change_locked_mode(&mut ns, true, |state| {
                    for (id, origin, base) in replacement {
                        let record = state.nodes.get_mut(&id).unwrap();
                        record.source_origin = Some(origin);
                        record.source_directory = base;
                        record.source_clean_ctime = Some(record.node.ctime);
                    }
                    for (id, tree_id) in updated_dirs {
                        state.nodes.get_mut(&id).unwrap().source_directory = Some(DirectoryBase {
                            tree_id,
                            loaded: true,
                        });
                    }
                    let source = state.source.as_mut().unwrap();
                    source.root = root.root_tree;
                    source.generation = root.pin.generation;
                    source.revision = source
                        .revision
                        .checked_add(1)
                        .context("source revision exhausted")?;
                    new_meta.changes.clear();
                    new_meta.change_bytes = 0;
                    source.lazy = Some(new_meta.clone());
                    let entries = dirty_entries(state, limits)?;
                    let version = source_version(&self.tree, state, &entries)?;
                    let result = SourceReceipt {
                        request_id: request_id.into(),
                        operation: SourceOperation::Acknowledge,
                        intent,
                        root: root.root_tree,
                        expected: Some(expected.clone()),
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
                })?
            }
        };
        if self
            .tree
            .namespace
            .lock()
            .unwrap()
            .state
            .source
            .as_ref()
            .and_then(|s| s.lazy.as_ref())
            .is_some_and(|m| m.pin == root.pin)
        {
            self.tree
                .bind_lazy_runtime(root.provider, root.pin, root.load_limits);
        }
        Ok(result)
    }
}

impl Tree {
    pub(super) async fn ensure_source_path_ancestors(&self, path: &str) -> Result<()> {
        let mut id = 1;
        for name in parts(path)? {
            self.ensure_directory_loaded(id).await?;
            match self.lookup(id, name) {
                Ok(node) => {
                    require(node.kind == NodeKind::Directory, CoreError::NotDirectory)?;
                    id = node.id;
                }
                Err(error) if error.downcast_ref::<CoreError>() == Some(&CoreError::NotFound) => {
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
    pub(super) fn require_complete_source_namespace(&self) -> Result<()> {
        ensure!(
            self.namespace
                .lock()
                .unwrap()
                .state
                .source
                .as_ref()
                .is_none_or(|s| s.lazy.is_none()),
            "operation requires a complete source namespace"
        );
        Ok(())
    }
}

pub(super) fn has_user_changes(previous: &State, next: &State) -> Result<bool> {
    for id in &next.nodes.dirty {
        if *id == 1 {
            continue;
        }
        let before = previous.nodes.get(id);
        let after = next.nodes.get(id);
        let visible = after
            .filter(|r| r.node.linked)
            .or_else(|| before.filter(|r| r.node.linked));
        let Some(visible) = visible else {
            continue;
        };
        let state = if after.is_some_and(|r| r.node.linked) {
            next
        } else {
            previous
        };
        if path_for(state, visible.node.id)?
            .split('/')
            .next()
            .is_some_and(source::control_name)
        {
            continue;
        }
        if before.is_none_or(|b| !b.node.linked) || after.is_none_or(|a| !a.node.linked) {
            return Ok(true);
        }
        let (before, after) = (before.unwrap(), after.unwrap());
        if before.node.parent != after.node.parent
            || before.node.name != after.node.name
            || before.node.ctime != after.node.ctime
            || before.node.mode != after.node.mode
        {
            return Ok(true);
        }
    }
    Ok(false)
}

impl std::fmt::Debug for SourceDeltaSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceDeltaSnapshot")
            .field("baseline", &self.baseline)
            .field("version", &self.version)
            .field("changes", &self.changes.len())
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}
