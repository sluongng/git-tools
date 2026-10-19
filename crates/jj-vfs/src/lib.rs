//! A compiled Jujutsu working-copy backend for a mounted Git VFS.
//!
//! Unlike jj's ordinary disk scanner this backend asks the mount's durable
//! journal which paths changed and reuses Git object identities exposed by the
//! projection. An untouched projected file is never opened for hashing.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fs2::FileExt;
use futures::StreamExt;

use jj_lib::backend::{CommitId, CopyId, FileId, TreeId, TreeValue};
use jj_lib::commit::Commit;
use jj_lib::conflict_labels::ConflictLabels;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::local_working_copy::{TreeState, TreeStateSettings};
use jj_lib::merge::Merge;
use jj_lib::merged_tree::MergedTree;
use jj_lib::merged_tree_builder::MergedTreeBuilder;
use jj_lib::object_id::ObjectId;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::{WorkspaceName, WorkspaceNameBuf};
use jj_lib::repo_path::{RepoPath, RepoPathBuf};
use jj_lib::settings::UserSettings;
use jj_lib::store::Store;
use jj_lib::working_copy::*;
use rustix::fs::{AtFlags, FileType as FsType, Mode, OFlags};
use serde::{Deserialize, Serialize};
use workspace_overlay::overlay::changes_since;
use xattr::FileExt as _;

pub const NAME: &str = "git-vfs";

/// The first initialization adopts the base commit already visible on the mount.
/// Further checkouts always use jj's differential checkout implementation.
pub struct VfsWorkingCopyFactory {
    pub initial_overlay: Option<PathBuf>,
    pub initial_base: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct State {
    version: u32,
    operation: String,
    workspace: String,
    trees: Vec<String>,
    labels: Vec<String>,
    overlay: PathBuf,
    git_executable: PathBuf,
    token: String,
    #[serde(default)]
    snapshot_policy: String,
    adopt_base: Option<String>,
}

struct VfsWorkingCopy {
    root: PathBuf,
    directory: PathBuf,
    store: Arc<Store>,
    settings: TreeStateSettings,
    state: State,
    operation: OperationId,
    workspace: WorkspaceNameBuf,
    tree: MergedTree,
    sparse: Vec<RepoPathBuf>,
}

fn state_error(error: impl Into<anyhow::Error>) -> WorkingCopyStateError {
    let error = error.into();
    WorkingCopyStateError {
        message: format!("Git VFS working-copy state: {error:#}"),
        err: error.into(),
    }
}
fn snapshot_error(error: impl Into<anyhow::Error>) -> SnapshotError {
    let error = error.into();
    SnapshotError::Other {
        message: format!("Git VFS snapshot: {error:#}"),
        err: error.into(),
    }
}
fn checkout_error(error: impl Into<anyhow::Error>) -> CheckoutError {
    let error = error.into();
    CheckoutError::Other {
        message: format!("Git VFS checkout: {error:#}"),
        err: error.into(),
    }
}

impl State {
    fn read(directory: &Path) -> Result<Self> {
        let state: Self = serde_json::from_slice(&fs::read(directory.join("git-vfs.json"))?)?;
        ensure!(state.version == 1, "unsupported state format");
        ensure!(
            !state.trees.is_empty() && state.trees.len() % 2 == 1,
            "invalid merged tree arity"
        );
        ensure!(
            state.labels.is_empty() || state.labels.len() == state.trees.len(),
            "invalid conflict label arity"
        );
        Ok(state)
    }
    fn save(&self, directory: &Path) -> Result<()> {
        // jj's mutation lock serializes this fixed temporary name. Keep metadata
        // on the mount so the overlay's durability and remount lifetime apply.
        let temporary = directory.join("git-vfs.json.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    fs::remove_file(&temporary)?;
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)
                } else {
                    Err(e)
                }
            })?;
        file.write_all(&serde_json::to_vec(self)?)?;
        file.sync_all()?;
        fs::rename(temporary, directory.join("git-vfs.json"))?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

impl VfsWorkingCopy {
    fn from_state(
        store: Arc<Store>,
        root: PathBuf,
        directory: PathBuf,
        state: State,
        settings: TreeStateSettings,
    ) -> Result<Self> {
        let operation =
            OperationId::try_from_hex(&state.operation).context("invalid operation id")?;
        let workspace = WorkspaceNameBuf::from(state.workspace.clone());
        let trees = state
            .trees
            .iter()
            .map(|id| TreeId::try_from_hex(id).context("invalid tree id"))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            store
                .backend_impl::<jj_lib::git_backend::GitBackend>()
                .is_some()
                && store.commit_id_length() == 20,
            "Git VFS requires a SHA-1 Git backend"
        );
        ensure!(
            trees.iter().all(|tree| tree.as_bytes().len() == 20),
            "invalid Git tree identity length"
        );
        let tree = MergedTree::new(
            store.clone(),
            Merge::from_vec(trees),
            ConflictLabels::from_vec(state.labels.clone()),
        );
        Ok(Self {
            root,
            directory,
            store,
            settings,
            state,
            operation,
            workspace,
            tree,
            sparse: vec![RepoPathBuf::root()],
        })
    }
}

impl WorkingCopyFactory for VfsWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        directory: PathBuf,
        operation: OperationId,
        workspace: WorkspaceNameBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let result = (|| -> Result<_> {
            let overlay = self.initial_overlay.clone().context("use git-jj init-vfs for a new VFS workspace; ordinary jj workspace add is not supported")?.canonicalize()?;
            let base = self
                .initial_base
                .clone()
                .context("missing projected base commit")?;
            // Validate the journal endpoint before writing any jj state.
            changes_since(&overlay, "")?;
            fs::create_dir_all(&directory)?;
            let state = State {
                version: 1,
                operation: operation.hex(),
                workspace: workspace.as_str().to_owned(),
                trees: vec![store.empty_tree_id().hex()],
                labels: vec![],
                overlay,
                git_executable: jj_lib::git::GitSettings::from_settings(settings)?.executable_path,
                token: String::new(),
                snapshot_policy: String::new(),
                adopt_base: Some(base),
            };
            state.save(&directory)?;
            VfsWorkingCopy::from_state(
                store,
                root,
                directory,
                state,
                TreeStateSettings::try_from_user_settings(settings)?,
            )
        })();
        result
            .map(|wc| Box::new(wc) as Box<dyn WorkingCopy>)
            .map_err(state_error)
    }
    fn load_working_copy(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        directory: PathBuf,
        settings: &UserSettings,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let result = (|| -> Result<_> {
            let state = State::read(&directory)?;
            VfsWorkingCopy::from_state(
                store,
                root,
                directory,
                state,
                TreeStateSettings::try_from_user_settings(settings)?,
            )
        })();
        result
            .map(|wc| Box::new(wc) as Box<dyn WorkingCopy>)
            .map_err(state_error)
    }
}

#[async_trait(?Send)]
impl WorkingCopy for VfsWorkingCopy {
    fn name(&self) -> &str {
        NAME
    }
    fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace
    }
    fn operation_id(&self) -> &OperationId {
        &self.operation
    }
    fn tree(&self) -> Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }
    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse)
    }
    async fn start_mutation(&self) -> Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let result = (|| -> Result<_> {
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.directory.join("git-vfs.lock"))?;
            lock.lock_exclusive()?;
            let state = State::read(&self.directory)?;
            ensure!(
                state.operation == self.operation.hex(),
                "working copy changed concurrently; retry the command"
            );
            let current = VfsWorkingCopy::from_state(
                self.store.clone(),
                self.root.clone(),
                self.directory.clone(),
                state,
                self.settings.clone(),
            )?;
            Ok(LockedVfsWorkingCopy {
                old_tree: self.tree.clone(),
                old_operation: self.operation.clone(),
                wc: current,
                _lock: lock,
            })
        })();
        result
            .map(|wc| Box::new(wc) as Box<dyn LockedWorkingCopy>)
            .map_err(state_error)
    }
}

struct LockedVfsWorkingCopy {
    old_tree: MergedTree,
    old_operation: OperationId,
    wc: VfsWorkingCopy,
    _lock: File,
}

impl LockedVfsWorkingCopy {
    async fn snapshot_inner(
        &mut self,
        options: &SnapshotOptions<'_>,
    ) -> Result<(MergedTree, SnapshotStats)> {
        ensure!(
            self.wc.state.adopt_base.is_none(),
            "initial checkout has not completed"
        );
        // Sample before scanning: concurrent writes remain in the next range.
        let (token, mut changed) = changes_since(&self.wc.state.overlay, &self.wc.state.token)?;
        let policy = format!(
            "{:?}|{:?}|{:?}|{}",
            options.base_ignores,
            options.start_tracking_matcher,
            options.force_tracking_matcher,
            options.max_new_file_size
        );
        if policy != self.wc.state.snapshot_policy {
            changed = vec![b"/".to_vec()];
        }
        if changed.is_empty() {
            return Ok((self.wc.tree.clone(), SnapshotStats::default()));
        }
        let mut prefixes = changed
            .iter()
            .map(|p| {
                if p == b"/" {
                    Ok(String::new())
                } else {
                    Ok(std::str::from_utf8(p)?.trim_end_matches('/').to_owned())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        prefixes.retain(|path| !path.split('/').any(|part| part == ".git" || part == ".jj"));
        // Ignore-file changes can make unchanged, previously ignored files
        // trackable. Revisit that directory rather than only the ignore file.
        for path in &mut prefixes {
            if path == ".gitignore" {
                path.clear();
            } else if let Some(parent) = path.strip_suffix("/.gitignore") {
                *path = parent.to_owned();
            }
        }
        if prefixes.is_empty() {
            self.wc.state.token = token;
            return Ok((self.wc.tree.clone(), SnapshotStats::default()));
        }
        let prefix_paths = prefixes
            .iter()
            .map(|p| RepoPathBuf::from_internal_string(p.clone()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let matcher = jj_lib::matchers::PrefixMatcher::new(&prefix_paths);
        let mut previous = BTreeMap::new();
        for (path, value) in self.wc.tree.entries_matching(&matcher) {
            previous.insert(path, value?);
        }
        let mut candidates: BTreeSet<RepoPathBuf> = previous.keys().cloned().collect();
        let root = ProjectionRoot {
            fd: File::open(&self.wc.root)?,
            label: self.wc.root.clone(),
        };
        let mut ignores = BTreeMap::new();
        ignores.insert(
            RepoPathBuf::root(),
            chain_ignore(&root, RepoPath::root(), options.base_ignores.clone())?,
        );
        for prefix in &prefixes {
            let relative = RepoPathBuf::from_internal_string(prefix.clone())?;
            collect_paths(&root, &relative, &mut candidates, &mut ignores, options)?;
        }
        let mut builder = MergedTreeBuilder::new(self.wc.tree.clone());
        let mut stats = SnapshotStats::default();
        for path in candidates {
            if reserved(&path) {
                continue;
            }
            let old = previous.get(&path);
            if old.is_some_and(|value| {
                value
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, TreeValue::GitSubmodule(_)))
            }) {
                anyhow::bail!(
                    "Git submodules are unsupported; refusing to snapshot {}",
                    path.as_internal_file_string()
                );
            }
            let Some((parent, name, metadata)) = root.metadata(&path)? else {
                builder.set_or_remove(path, Merge::absent());
                continue;
            };
            if metadata.is_dir() {
                builder.set_or_remove(path, Merge::absent());
                continue;
            }
            let tracked = old.is_some_and(|v| !v.is_absent());
            let force = options.force_tracking_matcher.matches(&path);
            if !tracked && !force {
                if !options.start_tracking_matcher.matches(&path) {
                    stats
                        .untracked_paths
                        .insert(path, UntrackedReason::FileNotAutoTracked);
                    continue;
                }
                if is_ignored(&root, &path, &mut ignores)? {
                    continue;
                }
                if metadata.len() > options.max_new_file_size {
                    stats.untracked_paths.insert(
                        path,
                        UntrackedReason::FileTooLarge {
                            size: metadata.len(),
                            max_size: options.max_new_file_size,
                        },
                    );
                    continue;
                }
            }
            if let Some(progress) = options.progress {
                progress(&path);
            }
            // Only the base-only identity contract is safe to reuse: the normal
            // gitsha1 xattr can hash mutable bytes not yet stored as Git objects.
            let mut file = if metadata.is_file() {
                let fd = rustix::fs::openat(
                    &parent,
                    name.as_str(),
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                    Mode::empty(),
                )?;
                let file = File::from(fd);
                ensure!(
                    file.metadata()?.is_file(),
                    "file changed type during snapshot"
                );
                Some(file)
            } else {
                None
            };
            let identity = if let Some(file) = &file {
                match file.get_xattr("user.gitsha1.base") {
                    Ok(id) => id,
                    Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
                    Err(error) => return Err(error.into()),
                }
            } else {
                None
            };
            let value = if metadata.is_symlink() {
                let target = rustix::fs::readlinkat(&parent, name.as_str(), Vec::new())?;
                let target = target.to_str().context("symlink target is not UTF-8")?;
                TreeValue::Symlink(self.wc.store.write_symlink(&path, target).await?)
            } else if metadata.is_file() {
                let id = if let Some(identity) = identity {
                    ensure!(
                        identity.len() == 20,
                        "projection returned a non-SHA1 identity"
                    );
                    FileId::new(identity)
                } else {
                    let mut bytes = Vec::new();
                    file.as_mut().unwrap().read_to_end(&mut bytes)?;
                    self.wc
                        .store
                        .write_file(&path, &mut bytes.as_slice())
                        .await?
                };
                // Preserve copy history when the file identity is unchanged.
                let copy_id = old
                    .and_then(|v| v.as_resolved())
                    .and_then(Option::as_ref)
                    .and_then(|v| match v {
                        TreeValue::File {
                            id: old_id,
                            copy_id,
                            ..
                        } if *old_id == id => Some(copy_id.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(CopyId::placeholder);
                TreeValue::File {
                    id,
                    executable: metadata.mode & 0o111 != 0,
                    copy_id,
                }
            } else {
                anyhow::bail!(
                    "unsupported special file: {}",
                    path.as_internal_file_string()
                );
            };
            if old.is_some_and(|old| !old.is_resolved()) {
                // Do not silently convert unresolved conflicts into ordinary
                // marker text. A later backend extension can parse them using
                // jj's conflict APIs, without corrupting this first version.
                anyhow::bail!(
                    "snapshotting conflicted paths is not yet supported: {}",
                    path.as_internal_file_string()
                );
            }
            builder.set_or_remove(path, Merge::normal(value));
        }
        let tree = builder.write_tree().await?;
        self.wc.tree = tree.clone();
        self.wc.state.token = token;
        self.wc.state.snapshot_policy = policy;
        Ok((tree, stats))
    }
}

fn reserved(path: &RepoPath) -> bool {
    path.as_internal_file_string()
        .split('/')
        .any(|part| part == ".git" || part == ".jj")
}
fn collect_paths(
    root: &ProjectionRoot,
    path: &RepoPath,
    out: &mut BTreeSet<RepoPathBuf>,
    ignores: &mut BTreeMap<RepoPathBuf, Arc<GitIgnoreFile>>,
    options: &SnapshotOptions<'_>,
) -> Result<()> {
    if reserved(path) {
        return Ok(());
    }
    let dir = if path.is_root() {
        Some(root.fd.try_clone()?)
    } else {
        let Some((parent, name, metadata)) = root.metadata(path)? else {
            return Ok(());
        };
        if !metadata.is_dir() {
            out.insert(path.to_owned());
            return Ok(());
        }
        if ignores_for(root, path, ignores)?.matches_dir(path)
            && options.force_tracking_matcher.visit(path).is_nothing()
        {
            return Ok(());
        }
        root.open_directory(&parent, &name)?
    };
    let Some(dir) = dir else {
        return Ok(());
    };
    for entry in rustix::fs::Dir::read_from(&dir)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .to_str()
            .context("non-UTF8 projected path")?;
        if name == "." || name == ".." {
            continue;
        }
        let child = if path.is_root() {
            name.to_owned()
        } else {
            format!("{}/{name}", path.as_internal_file_string())
        };
        collect_paths(
            root,
            &RepoPathBuf::from_internal_string(child)?,
            out,
            ignores,
            options,
        )?;
    }
    Ok(())
}

struct ProjectionRoot {
    fd: File,
    label: PathBuf,
}
struct ProjectedMetadata {
    mode: u32,
    size: u64,
}
impl ProjectedMetadata {
    fn is_dir(&self) -> bool {
        FsType::from_raw_mode(self.mode) == FsType::Directory
    }
    fn is_file(&self) -> bool {
        FsType::from_raw_mode(self.mode) == FsType::RegularFile
    }
    fn is_symlink(&self) -> bool {
        FsType::from_raw_mode(self.mode) == FsType::Symlink
    }
    fn len(&self) -> u64 {
        self.size
    }
}
impl ProjectionRoot {
    fn open_directory(&self, parent: &File, name: &str) -> Result<Option<File>> {
        match rustix::fs::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => Ok(Some(File::from(fd))),
            Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP) => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
    fn parent(&self, path: &RepoPath) -> Result<Option<(File, String)>> {
        let mut parts: Vec<_> = path.as_internal_file_string().split('/').collect();
        let name = parts.pop().context("root has no parent")?.to_owned();
        let mut parent = self.fd.try_clone()?;
        for component in parts {
            let Some(next) = self.open_directory(&parent, component)? else {
                return Ok(None);
            };
            parent = next;
        }
        Ok(Some((parent, name)))
    }
    fn metadata(&self, path: &RepoPath) -> Result<Option<(File, String, ProjectedMetadata)>> {
        let Some((parent, name)) = self.parent(path)? else {
            return Ok(None);
        };
        match rustix::fs::statat(&parent, name.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
            Ok(metadata) => {
                ensure!(metadata.st_size >= 0, "negative file size");
                Ok(Some((
                    parent,
                    name,
                    ProjectedMetadata {
                        mode: metadata.st_mode,
                        size: metadata.st_size as u64,
                    },
                )))
            }
            Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP) => {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}
fn chain_ignore(
    root: &ProjectionRoot,
    prefix: &RepoPath,
    parent: Arc<GitIgnoreFile>,
) -> Result<Arc<GitIgnoreFile>> {
    let relative = if prefix.is_root() {
        ".gitignore".to_owned()
    } else {
        format!("{}/.gitignore", prefix.as_internal_file_string())
    };
    let path = RepoPathBuf::from_internal_string(relative)?;
    let Some((directory, name)) = root.parent(&path)? else {
        return Ok(parent);
    };
    let fd = match rustix::fs::openat(
        &directory,
        name.as_str(),
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP) => {
            return Ok(parent);
        }
        Err(error) => return Err(error.into()),
    };
    let mut file = File::from(fd);
    ensure!(
        file.metadata()?.is_file(),
        "ignore path is not a regular file"
    );
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;
    Ok(parent.chain(
        prefix,
        &root.label.join(path.as_internal_file_string()),
        &contents,
    )?)
}
fn ignores_for(
    root: &ProjectionRoot,
    path: &RepoPath,
    cache: &mut BTreeMap<RepoPathBuf, Arc<GitIgnoreFile>>,
) -> Result<Arc<GitIgnoreFile>> {
    let parent = path.parent().unwrap_or(RepoPath::root());
    if let Some(ignores) = cache.get(parent) {
        return Ok(ignores.clone());
    }
    let inherited = if parent.is_root() {
        cache.get(RepoPath::root()).unwrap().clone()
    } else {
        ignores_for(root, parent, cache)?
    };
    let result = chain_ignore(root, parent, inherited)?;
    cache.insert(parent.to_owned(), result.clone());
    Ok(result)
}

fn is_ignored(
    root: &ProjectionRoot,
    path: &RepoPath,
    cache: &mut BTreeMap<RepoPathBuf, Arc<GitIgnoreFile>>,
) -> Result<bool> {
    let mut ancestors = Vec::new();
    let mut parent = path.parent();
    while let Some(path) = parent {
        if !path.is_root() {
            ancestors.push(path);
        }
        parent = path.parent();
    }
    for directory in ancestors.into_iter().rev() {
        if ignores_for(root, directory, cache)?.matches_dir(directory) {
            return Ok(true);
        }
    }
    Ok(ignores_for(root, path, cache)?.matches_file(path))
}

#[async_trait]
impl LockedWorkingCopy for LockedVfsWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        &self.old_operation
    }
    fn old_tree(&self) -> &MergedTree {
        &self.old_tree
    }
    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> Result<(MergedTree, SnapshotStats), SnapshotError> {
        self.snapshot_inner(options).await.map_err(snapshot_error)
    }
    async fn check_out(&mut self, commit: &Commit) -> Result<CheckoutStats, CheckoutError> {
        let tree = commit.tree();
        if self.wc.tree.tree_ids() != tree.tree_ids() {
            for (_, value) in tree.entries() {
                if value?
                    .iter()
                    .flatten()
                    .any(|value| matches!(value, TreeValue::GitSubmodule(_)))
                {
                    return Err(checkout_error(anyhow::anyhow!(
                        "Git submodules are unsupported; refusing to modify the working copy"
                    )));
                }
            }
        }
        if let Some(base) = &self.wc.state.adopt_base {
            let base = CommitId::try_from_hex(base)
                .ok_or_else(|| checkout_error(anyhow::anyhow!("invalid initial base")))?;
            let expected = self.wc.store.get_commit(&base)?.tree();
            if expected.tree_ids() != tree.tree_ids() {
                return Err(checkout_error(anyhow::anyhow!(
                    "initial checkout must match the mounted base"
                )));
            }
            self.wc.tree = tree;
            self.wc.state.adopt_base = None;
            return Ok(CheckoutStats::default());
        }
        if tree.has_conflict() {
            return Err(checkout_error(anyhow::anyhow!(
                "conflicted-tree checkout is not yet supported by this backend"
            )));
        }
        if self.wc.tree.tree_ids() == tree.tree_ids() && self.wc.tree.labels() == tree.labels() {
            return Ok(CheckoutStats::default());
        }
        prefetch_checkout(&self.wc, &tree)
            .await
            .map_err(checkout_error)?;
        // TreeState keeps upstream checkout safety (untracked-file collisions,
        // file/directory transitions, executable bits and symlinks).
        let mut disk = TreeState::init_without_saving(
            self.wc.store.clone(),
            self.wc.root.clone(),
            self.wc.directory.clone(),
            &self.wc.settings,
        );
        disk.reset(&self.wc.tree)
            .await
            .map_err(|e| checkout_error(anyhow::anyhow!(e)))?;
        let stats = disk.check_out(&tree)?;
        self.wc.tree = tree;
        Ok(stats)
    }
    fn rename_workspace(&mut self, workspace: WorkspaceNameBuf) {
        self.wc.workspace = workspace;
    }
    async fn reset(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.wc.tree = commit.tree();
        self.wc.state.token.clear();
        Ok(())
    }
    async fn recover(&mut self, commit: &Commit) -> Result<(), ResetError> {
        self.reset(commit).await
    }
    fn sparse_patterns(&self) -> Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.wc.sparse)
    }
    async fn set_sparse_patterns(
        &mut self,
        patterns: Vec<RepoPathBuf>,
    ) -> Result<CheckoutStats, CheckoutError> {
        if patterns != self.wc.sparse {
            return Err(checkout_error(anyhow::anyhow!(
                "sparse checkout is unsupported: the VFS already projects lazily"
            )));
        }
        Ok(CheckoutStats::default())
    }
    async fn finish(
        mut self: Box<Self>,
        operation: OperationId,
    ) -> Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        self.wc.state.operation = operation.hex();
        self.wc.state.workspace = self.wc.workspace.as_str().to_owned();
        self.wc.state.trees = self.wc.tree.tree_ids().iter().map(ObjectId::hex).collect();
        self.wc.state.labels = self.wc.tree.labels().as_slice().to_vec();
        self.wc
            .state
            .save(&self.wc.directory)
            .map_err(state_error)?;
        self.wc.operation = operation;
        Ok(Box::new(self.wc))
    }
}

/// gix does not hydrate promisor blobs on reads. Ask native Git to make only
/// the checkout's changed target objects available before jj materializes them.
async fn prefetch_checkout(wc: &VfsWorkingCopy, target: &MergedTree) -> Result<()> {
    let mut objects = BTreeSet::new();
    let matcher = jj_lib::matchers::EverythingMatcher;
    let mut changes = wc.tree.diff_stream_for_file_system(target, &matcher);
    while let Some(change) = changes.next().await {
        for value in change.values?.after.iter().flatten() {
            match value {
                TreeValue::File { id, .. } => {
                    objects.insert(id.hex());
                }
                TreeValue::Symlink(id) => {
                    objects.insert(id.hex());
                }
                _ => {}
            }
        }
    }
    if objects.is_empty() {
        return Ok(());
    }
    let backend = wc
        .store
        .backend_impl::<jj_lib::git_backend::GitBackend>()
        .context("Git VFS requires jj's Git backend")?;
    let mut child = std::process::Command::new(&wc.state.git_executable)
        .arg("--git-dir")
        .arg(backend.git_repo_path())
        .args(["cat-file", "--batch-check=%(objectname)"])
        .env_remove("GIT_NO_LAZY_FETCH")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let expected = objects.len();
    let mut input = child.stdin.take().unwrap();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        for object in objects {
            writeln!(input, "{object}")?;
        }
        Ok(())
    });
    let output = child.wait_with_output()?;
    writer
        .join()
        .map_err(|_| anyhow::anyhow!("object prefetch writer panicked"))??;
    ensure!(
        output.status.success(),
        "native Git could not hydrate checkout objects"
    );
    let lines: Vec<_> = output
        .stdout
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect();
    ensure!(
        lines.len() == expected
            && lines
                .iter()
                .all(|line| line.len() == 40 && line.iter().all(u8::is_ascii_hexdigit)),
        "checkout contains objects unavailable from the promisor remote"
    );
    Ok(())
}
