//! Native source mount lifecycle. Metadata state and jj administration stay
//! outside the projection; the projected .jj marker contains only an opaque ID.
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use jj_native_backend::NativeObjectIo;
use local_cas::LocalCas;
use native_workspace::{Compiler, Limits, Scope};
use output_core::{
    Projection, SourceDeltaSnapshot, SourceFrontendFence, SourceLimits, SourceSnapshot, Tree,
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tinytree_model as m;

#[derive(Clone, Debug)]
pub struct Config {
    /// Absolute normalized root and private state; their parents must exist.
    pub root: PathBuf,
    pub state: PathBuf,
    pub workspace_identity: [u8; 32],
    pub scope: Scope,
    /// Immutable bootstrap seed. Later root switches use checked control APIs,
    /// never changing this field to overwrite a recognized dirty workspace.
    pub root_merge: m::TreeMerge,
    pub compiler_limits: Limits,
    pub source_limits: SourceLimits,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Header {
    format: u32,
    workspace_identity: [u8; 32],
    backend: String,
    mount_root: PathBuf,
    bootstrap_root: [u8; 32],
    ready: bool,
    namespace_identity: Option<String>,
}
enum RootGuard {
    Private {
        _directory: PrivateDir,
    },
    #[cfg(windows)]
    ProjFs {
        _guard: output_projfs::SourceRootGuard,
    },
}
struct StateOwner {
    directory: PrivateDir,
    _mount_root: RootGuard,
    _lock: File,
}
impl std::fmt::Debug for StateOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateOwner").finish_non_exhaustive()
    }
}

pub struct PreparedWorkspace {
    config: Config,
    owner: Arc<StateOwner>,
    tree: Arc<Tree>,
    compiler: Arc<Compiler>,
}
impl std::fmt::Debug for PreparedWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWorkspace").finish_non_exhaustive()
    }
}
impl PreparedWorkspace {
    pub fn tree(&self) -> Arc<Tree> {
        self.tree.clone()
    }
    pub fn compiler(&self) -> Arc<Compiler> {
        self.compiler.clone()
    }
    pub fn root(&self) -> &Path {
        &self.config.root
    }
    pub fn workspace_identity(&self) -> [u8; 32] {
        self.config.workspace_identity
    }
    /// Portable protocol/testing path only: no native writer can bypass Tree.
    /// Mounted callers use NativeMount::capture_quiescent instead.
    pub fn capture_tree_only(&self) -> Result<SourceSnapshot> {
        self.tree
            .freeze_source(SourceFrontendFence::TreeOnly, &self.config.source_limits)
    }
    /// Explicit lazy delta capture; unopened base subtrees are not deletions.
    pub fn capture_lazy_tree_only(&self) -> Result<SourceDeltaSnapshot> {
        self.tree
            .freeze_lazy_source(SourceFrontendFence::TreeOnly, &self.config.source_limits)
    }
}

fn resolved_tree(merge: &m::TreeMerge) -> Result<m::TreeId> {
    if merge.terms.len() != 1 {
        return Err(native_workspace::UnresolvedMerge {
            merge: merge.clone(),
        }
        .into());
    }
    ensure!(
        merge.labels == [String::new()],
        "invalid resolved native tree merge labels"
    );
    Ok(merge.terms[0])
}

/// Bind a lazy immutable source baseline without enumerating its directories.
/// This does not mount or fetch regular file payloads and is RBE-testable.
/// Existing ready state is never reset or reinstalled from the bootstrap seed.
pub async fn prepare(
    config: Config,
    provider: Arc<dyn NativeObjectIo>,
    cache: Arc<LocalCas>,
) -> Result<PreparedWorkspace> {
    validate(&config)?;
    let projected = canonical_candidate(&config.root)?;
    let state = canonical_candidate(&config.state)?;
    let cached = cache.root_path();
    ensure!(
        !overlaps(&projected, &state) && !overlaps(&projected, cached) && !overlaps(&state, cached),
        "native workspace mount, state and shared cache overlap"
    );
    let state_config = config.clone();
    let (owner, header) = tokio::task::spawn_blocking(move || open_state(&state_config)).await??;
    let compiler = Arc::new(
        Compiler::new(
            provider,
            config.scope.clone(),
            config.compiler_limits.clone(),
        )?
        .with_lifetime_guard(owner.clone()),
    );
    let open_owner = owner.clone();
    let source = compiler.source();
    let backend = config.scope.backend().to_owned();
    let ready = header.ready;
    let tree = tokio::task::spawn_blocking(move || -> Result<Arc<Tree>> {
        let namespace = open_owner.directory.path().join("namespace");
        let existing = preflight_namespace(&namespace, ready)?;
        let _directory = PrivateDir::create(&namespace)?;
        if existing {
            Tree::open_existing_with_cache_and_source(namespace, backend, cache, source)
        } else {
            Tree::open_with_cache_and_source(namespace, backend, cache, source)
        }
    })
    .await??;
    if let Some(identity) = &header.namespace_identity {
        ensure!(
            identity == tree.identity(),
            "native workspace namespace identity changed"
        );
    }
    if header.ready {
        if let Some(pin) = tree.lazy_source_pin() {
            // The inode root is always a local namespace handle. The persisted
            // source baseline is the canonical immutable tree identity instead.
            let (baseline, _) = tree.source_baseline()?;
            ensure!(pin.root == baseline, "native lazy baseline pin mismatch");
            let lazy = compiler.lazy_tree_for_source(
                m::TreeId(baseline),
                pin.generation,
                config.source_limits.clone(),
            )?;
            let source = lazy.source_root(tree.identity());
            compiler.restore_lazy_artifacts(&tree.lazy_source_artifacts()?)?;
            tree.attach_lazy_source(source.provider, pin)?;
        } else {
            // Existing complete namespaces retain their explicit full metadata
            // authorization path; do not reinterpret them as partial lazy state.
            let current = {
                let snapshot =
                    tree.freeze_source(SourceFrontendFence::TreeOnly, &config.source_limits)?;
                m::TreeId(snapshot.base_root())
            };
            compiler.compile_tree(current).await?;
        }
    } else if tree.lazy_source_pin().is_none() && tree.source_baseline().is_ok() {
        // Resume the exact bootstrap receipt of a pre-lazy installation that
        // stopped before its ready header was made durable.
        let compiled = compiler.compile_merge(&config.root_merge).await?;
        let manifest = compiled.manifest(tree.identity());
        let install_tree = tree.clone();
        let limits = config.source_limits.clone();
        let install_owner = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = install_owner;
            install_tree.install_source(manifest, None, "native-bootstrap-v1", &limits)
        })
        .await??;
    } else {
        let root = resolved_tree(&config.root_merge)?;
        let lazy = compiler.lazy_tree_for_source(root, 1, config.source_limits.clone())?;
        let install_tree = tree.clone();
        let source = lazy.source_root(tree.identity());
        let limits = config.source_limits.clone();
        let install_owner = owner.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _owner = install_owner;
            // Exact receipt replay only; never replace a recognized dirty view.
            install_tree.install_lazy_source(source, None, "native-bootstrap-v1", &limits)?;
            Ok(())
        })
        .await??;
        compiler.restore_lazy_artifacts(&tree.lazy_source_artifacts()?)?;
    }
    let marker = hex(&config.workspace_identity).into_bytes();
    let control_tree = tree.clone();
    let control_owner = owner.clone();
    let limits = config.source_limits.clone();
    let mut completed = header;
    completed.ready = true;
    completed.namespace_identity = Some(tree.identity().to_owned());
    tokio::task::spawn_blocking(move || -> Result<()> {
        control_tree.install_source_control(&marker, &limits)?;
        control_owner
            .directory
            .replace("native-vfs.json", &serde_json::to_vec(&completed)?)?;
        Ok(())
    })
    .await??;
    Ok(PreparedWorkspace {
        config,
        owner,
        tree,
        compiler,
    })
}

pub struct NativeMount {
    prepared: PreparedWorkspace,
    projection: Arc<dyn Projection>,
}
impl std::fmt::Debug for NativeMount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeMount").finish_non_exhaustive()
    }
}
impl NativeMount {
    pub fn tree(&self) -> Arc<Tree> {
        self.prepared.tree()
    }
    pub fn compiler(&self) -> Arc<Compiler> {
        self.prepared.compiler()
    }
    pub fn projection(&self) -> Arc<dyn Projection> {
        self.projection.clone()
    }
    pub fn root(&self) -> &Path {
        self.prepared.root()
    }
    pub fn state_dir(&self) -> &Path {
        self.prepared.owner.directory.path()
    }
    pub fn workspace_identity(&self) -> [u8; 32] {
        self.prepared.workspace_identity()
    }
    pub fn backend_identity(&self) -> &str {
        self.prepared.config.scope.backend()
    }
    /// Caller must quiesce and flush native users first. Windows native writers
    /// are not Tree-authoritative: reject rather than omit dirty NTFS bytes.
    pub fn capture_quiescent(&self) -> Result<SourceSnapshot> {
        #[cfg(windows)]
        {
            anyhow::bail!(
                "native Windows source capture requires an authoritative NTFS capture provider"
            )
        }
        #[cfg(not(windows))]
        {
            self.prepared.capture_tree_only()
        }
    }
    /// A lazy namespace must be captured as a delta, never as an incomplete
    /// complete manifest. Native users must be quiesced and flushed first.
    pub fn capture_lazy_quiescent(&self) -> Result<SourceDeltaSnapshot> {
        #[cfg(windows)]
        {
            anyhow::bail!(
                "native Windows lazy source capture requires an authoritative dirty journal"
            )
        }
        #[cfg(not(windows))]
        {
            self.prepared.capture_lazy_tree_only()
        }
    }
    /// Provider-fenced checked root publication. Providers without a native
    /// publication/invalidation fence return Unsupported; there is no raw Tree
    /// fallback. Compilation alone never changes the mounted namespace.
    pub async fn checkout_resolved(
        &self,
        merge: &m::TreeMerge,
        expected: output_core::SourceVersion,
        request_id: String,
    ) -> Result<output_core::SourceReceipt> {
        if self.prepared.tree.lazy_source_pin().is_some() {
            // Replays use the original expected version even if the root was
            // installed before the controller could persist its completion.
            let generation = expected
                .generation
                .checked_add(1)
                .context("lazy source generation exhausted")?;
            let lazy = self.prepared.compiler.lazy_tree_for_source(
                resolved_tree(merge)?,
                generation,
                self.prepared.config.source_limits.clone(),
            )?;
            return self
                .projection
                .publish_lazy_source(
                    lazy.source_root(self.prepared.tree.identity()),
                    expected,
                    request_id,
                    self.prepared.config.source_limits.clone(),
                )
                .await;
        }
        let compiled = self.prepared.compiler.compile_merge(merge).await?;
        self.projection
            .publish_source(
                compiled.manifest(self.prepared.tree.identity()),
                expected,
                request_id,
                self.prepared.config.source_limits.clone(),
            )
            .await
    }
    /// Caller holds the exclusive controller session/owner while proving an
    /// interrupted source request cannot publish after this barrier.
    pub async fn wait_source_idle(&self) -> Result<()> {
        self.projection.wait_source_idle().await
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.projection.shutdown().await
    }
}
pub async fn start(
    config: Config,
    provider: Arc<dyn NativeObjectIo>,
    cache: Arc<LocalCas>,
) -> Result<NativeMount> {
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (config, provider, cache);
        anyhow::bail!("native source projection unsupported on this platform")
    }
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        mount_prepared(prepare(config, provider, cache).await?).await
    }
}
/// Start a native provider after successful metadata preparation. On macOS an
/// error may own a recoverable NFS session; retain it until explicit shutdown.
pub async fn mount_prepared(prepared: PreparedWorkspace) -> Result<NativeMount> {
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = prepared;
        anyhow::bail!("native source projection unsupported on this platform")
    }
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        #[cfg(target_os = "linux")]
        let projection: Arc<dyn Projection> = output_fuse::start_native_source(
            prepared.tree(),
            prepared.root(),
            prepared.workspace_identity(),
        )
        .await?;
        #[cfg(target_os = "macos")]
        let projection: Arc<dyn Projection> =
            Arc::new(output_nfs::NfsMount::start(prepared.tree(), prepared.root()).await?);
        #[cfg(windows)]
        let projection: Arc<dyn Projection> = {
            let path = prepared.owner.directory.path().join("projection");
            let pinned_owner = prepared.owner.clone();
            let private_state = tokio::task::spawn_blocking(move || {
                let _owner = pinned_owner;
                PrivateDir::create(path)
            })
            .await??;
            Arc::new(
                output_projfs::start(prepared.tree(), prepared.root(), private_state.path())
                    .await?,
            )
        };
        Ok(NativeMount {
            prepared,
            projection,
        })
    }
}
/// Pins recognized private state while the daemon detaches an independently
/// proven disconnected kernel mount. This guard never opens the projected root
/// or opens/repairs the namespace database.
#[derive(Debug)]
pub struct RecoveryGuard {
    _directory: PrivateDir,
    _namespace: File,
    _database: File,
    _version: File,
    _lock: File,
}
pub fn lock_recovery(
    state: &Path,
    root: &Path,
    workspace_identity: [u8; 32],
    backend: &str,
    expected_namespace: &str,
) -> Result<RecoveryGuard> {
    #[cfg(target_os = "linux")]
    {
        lock_recovery_linux(state, root, workspace_identity, backend, expected_namespace)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (state, root, workspace_identity, backend, expected_namespace);
        anyhow::bail!("native disconnected-mount recovery currently requires Linux")
    }
}

#[cfg(target_os = "linux")]
fn lock_recovery_linux(
    state: &Path,
    root: &Path,
    workspace_identity: [u8; 32],
    backend: &str,
    expected_namespace: &str,
) -> Result<RecoveryGuard> {
    use std::{io::Read, os::unix::fs::MetadataExt};
    ensure!(
        workspace_identity != [0; 32],
        "invalid native recovery identity"
    );
    for path in [state, root] {
        ensure!(
            path.is_absolute()
                && path.as_os_str().len() <= 4096
                && !path
                    .components()
                    .any(|part| matches!(part, Component::CurDir | Component::ParentDir)),
            "native recovery paths must be absolute and normalized"
        );
    }
    ensure!(
        !overlaps(state, root),
        "native recovery state and mount overlap"
    );
    ensure!(
        !expected_namespace.is_empty() && expected_namespace.len() <= 128,
        "native recovery namespace identity is invalid"
    );
    let directory = PrivateDir::open_existing(state)?;
    ensure!(
        fs::symlink_metadata(directory.path().join("owner.lock"))?.is_file(),
        "native recovery owner lock is missing"
    );
    let lock = directory.open_lock("owner.lock")?;
    lock.try_lock_exclusive()
        .context("native workspace state remains owned")?;
    let header: Header = serde_json::from_slice(&directory.read("native-vfs.json", 32 * 1024)?)?;
    ensure!(
        header.format == 1
            && header.ready
            && header.workspace_identity == workspace_identity
            && header.mount_root == root
            && header.backend == backend
            && header.namespace_identity.as_deref() == Some(expected_namespace),
        "native recovery header does not match the persisted mount proof"
    );
    // Fjall creates ordinary 0755 children. The retained 0700 state directory
    // is the access boundary; do not chmod or reject recognized readonly DB
    // children merely because their leaf modes inherit less-private defaults.
    let owner = fs::metadata(directory.path())?.uid();
    let namespace_path = directory.path().join("namespace");
    let namespace = open_recovery_child(&namespace_path, owner, true)?;
    let database_path = namespace_path.join("namespace.fjall");
    let database = open_recovery_child(&database_path, owner, true)?;
    let mut version = open_recovery_child(&database_path.join("version"), owner, false)?;
    let mut bytes = Vec::with_capacity(5);
    (&mut version).take(5).read_to_end(&mut bytes)?;
    ensure!(
        bytes == b"FJL\x03",
        "unrecognized native recovery namespace database"
    );
    Ok(RecoveryGuard {
        _directory: directory,
        _namespace: namespace,
        _database: database,
        _version: version,
        _lock: lock,
    })
}

#[cfg(target_os = "linux")]
fn open_recovery_child(path: &Path, owner: u32, directory: bool) -> Result<File> {
    use std::os::unix::fs::MetadataExt;
    let valid = |metadata: &fs::Metadata| {
        metadata.uid() == owner
            && metadata.mode() & 0o022 == 0
            && if directory {
                metadata.is_dir()
            } else {
                metadata.is_file()
            }
    };
    let before = fs::symlink_metadata(path)?;
    ensure!(valid(&before), "untrusted native recovery child");
    let file = File::open(path)?;
    let opened = file.metadata()?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        valid(&opened)
            && valid(&after)
            && before.dev() == opened.dev()
            && before.ino() == opened.ino()
            && opened.dev() == after.dev()
            && opened.ino() == after.ino(),
        "native recovery child changed while opening"
    );
    Ok(file)
}

fn validate(config: &Config) -> Result<()> {
    for path in [&config.root, &config.state] {
        ensure!(
            path.is_absolute()
                && path.as_os_str().len() <= 4096
                && !path
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::CurDir)),
            "native workspace paths must be absolute and normalized"
        );
    }
    ensure!(
        !config.root.starts_with(&config.state) && !config.state.starts_with(&config.root),
        "native workspace mount and state overlap"
    );
    ensure!(
        config.workspace_identity != [0; 32],
        "native workspace identity must be opaque and nonzero"
    );
    ensure!(
        config.root_merge.terms.len() == 1 && config.root_merge.labels == [String::new()],
        "native workspace bootstrap requires a resolved root merge"
    );
    config.compiler_limits.validate()?;
    Ok(())
}
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
fn canonical_candidate(path: &Path) -> Result<PathBuf> {
    let mut ancestor = path;
    let mut missing = Vec::new();
    while !ancestor.try_exists()? {
        missing.push(
            ancestor
                .file_name()
                .context("native workspace path ancestor")?
                .to_owned(),
        );
        ancestor = ancestor
            .parent()
            .context("native workspace path ancestor")?;
    }
    let mut resolved = ancestor.canonicalize()?;
    for part in missing.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}
fn open_state(config: &Config) -> Result<(Arc<StateOwner>, Header)> {
    let directory = if config.state.try_exists()? {
        PrivateDir::open_existing(&config.state)?
    } else {
        PrivateDir::create(&config.state)?
    };
    let empty = fs::read_dir(directory.path())?.next().is_none();
    // Inspect recognition BEFORE opening/creating even a lock file.
    let previous = if empty {
        None
    } else {
        Some(
            serde_json::from_slice::<Header>(&directory.read("native-vfs.json", 32 * 1024)?)
                .context("unrecognized native workspace state")?,
        )
    };
    let expected = Header {
        format: 1,
        workspace_identity: config.workspace_identity,
        backend: config.scope.backend().to_owned(),
        mount_root: config.root.clone(),
        bootstrap_root: config.root_merge.terms[0].0,
        ready: false,
        namespace_identity: None,
    };
    if let Some(previous) = &previous {
        let mut binding = previous.clone();
        binding.ready = false;
        binding.namespace_identity = None;
        ensure!(
            binding == expected,
            "native workspace state identity or bootstrap mismatch"
        );
        ensure!(
            !previous.ready
                || previous
                    .namespace_identity
                    .as_ref()
                    .is_some_and(|id| !id.is_empty() && id.len() <= 4096),
            "native workspace namespace identity missing"
        );
        ensure!(
            fs::symlink_metadata(directory.path().join("owner.lock"))?.is_file(),
            "native workspace owner lock missing"
        );
    }
    #[cfg(windows)]
    let mount_root = if previous.as_ref().is_some_and(|h| h.ready)
        && directory
            .path()
            .join("projection/instance.json")
            .try_exists()?
    {
        let identity = previous
            .as_ref()
            .and_then(|h| h.namespace_identity.as_deref())
            .context("native workspace namespace identity missing")?;
        RootGuard::ProjFs {
            _guard: output_projfs::SourceRootGuard::open(
                &config.root,
                &directory.path().join("projection"),
                identity,
            )?,
        }
    } else {
        let root = PrivateDir::create(&config.root)?;
        ensure!(
            fs::read_dir(root.path())?.next().is_none(),
            "native workspace mountpoint must be empty"
        );
        RootGuard::Private { _directory: root }
    };
    #[cfg(not(windows))]
    let mount_root = {
        let root = PrivateDir::create(&config.root)?;
        ensure!(
            fs::read_dir(root.path())?.next().is_none(),
            "native workspace mountpoint must be empty"
        );
        RootGuard::Private { _directory: root }
    };
    let lock = directory.open_lock("owner.lock")?;
    lock.try_lock_exclusive()
        .context("native workspace state already owned")?;
    let header = previous.unwrap_or(expected);
    if empty {
        directory.replace("native-vfs.json", &serde_json::to_vec(&header)?)?;
    }
    Ok((
        Arc::new(StateOwner {
            directory,
            _mount_root: mount_root,
            _lock: lock,
        }),
        header,
    ))
}
fn preflight_namespace(path: &Path, ready: bool) -> Result<bool> {
    if !path.try_exists()? {
        ensure!(!ready, "native workspace namespace missing");
        return Ok(false);
    }
    let namespace = PrivateDir::open_existing(path)?;
    let empty = fs::read_dir(namespace.path())?.next().is_none();
    if empty {
        ensure!(!ready, "native workspace namespace missing");
        return Ok(false);
    }
    let database = namespace.path().join("namespace.fjall");
    ensure!(
        fs::symlink_metadata(&database)?.is_dir(),
        "native workspace namespace database missing"
    );
    let version = database.join("version");
    ensure!(
        fs::symlink_metadata(&version)?.is_file()
            && fs::symlink_metadata(&version)?.len() == 4
            && fs::read(version)? == b"FJL\x03",
        "unrecognized native workspace namespace database"
    );
    Ok(true)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
