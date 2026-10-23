//! Daemon-owned native source control. No Git conversion or materialized
//! fallback is implemented here. Snapshot acknowledgements preserve local
//! payloads and untracked namespace entries; they do not authorize offloading.
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use fs2::FileExt;
use jj_lib::{
    object_id::ObjectId,
    op_store::OperationId,
    working_copy::{CheckoutStats, SnapshotOptions, SnapshotStats},
};
use jj_native_backend::NativeObjectIo;
use jj_vfs::native::{
    NativeProjectionController, NativeProjectionMutation, NativeTree, ProjectionBinding,
    ProjectionConnector, ProjectionFailure, ProjectionOutcome, ProjectionReceipt, ProjectionState,
};
use native_vfs::NativeMount;
use native_workspace::Compiler;
use output_core::{
    SourceLimits, SourceOperation, SourceReceipt, SourceSnapshot, SourceVersion, Tree,
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tinytree_model as m;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

mod policy;
mod scope;
mod snapshot;
pub use policy::{
    PolicyCandidate, PolicyDecision, PolicyIgnore, PolicyInventory, PolicyKind, SnapshotPolicy,
    evaluate_inventory, options_fingerprint,
};
pub use scope::{ActionScope, ActionScopeError, AdmissionScope};

#[derive(Clone, Debug)]
pub struct Config {
    /// Opaque configured repository identity, matching the native jj factory.
    pub repository: String,
    /// Actual private external jj working-copy state, never a client-selected path.
    pub working_copy_state: PathBuf,
    pub source_limits: SourceLimits,
    pub max_ledger_bytes: usize,
    pub max_pending_requests: usize,
    pub max_file_bytes: u64,
    pub max_snapshot_bytes: u64,
    pub max_ignore_bytes: usize,
    pub io_buffer_bytes: usize,
    pub admission_timeout: Duration,
    pub operation_timeout: Duration,
}
impl Config {
    pub fn new(repository: String, working_copy_state: PathBuf) -> Self {
        Self {
            repository,
            working_copy_state,
            source_limits: SourceLimits::default(),
            max_ledger_bytes: 4 * 1024 * 1024,
            max_pending_requests: 128,
            max_file_bytes: 256 * 1024 * 1024,
            max_snapshot_bytes: 1024 * 1024 * 1024,
            max_ignore_bytes: 256 * 1024,
            io_buffer_bytes: 64 * 1024,
            admission_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(30),
        }
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.repository.is_empty()
                && self.repository.len() <= 256
                && self
                    .repository
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte)),
            "invalid configured repository identity"
        );
        ensure!(
            self.working_copy_state.is_absolute()
                && self.max_ledger_bytes > 0
                && self.max_ledger_bytes <= 64 * 1024 * 1024
                && self.max_pending_requests > 0
                && self.max_pending_requests <= 1024
                && self.max_file_bytes > 0
                && self.max_file_bytes <= 16 * 1024 * 1024 * 1024
                && self.max_snapshot_bytes >= self.max_file_bytes
                && self.max_snapshot_bytes <= 64 * 1024 * 1024 * 1024
                && self.max_ignore_bytes > 0
                && self.max_ignore_bytes <= 1024 * 1024
                && self.io_buffer_bytes > 0
                && self.io_buffer_bytes <= 1024 * 1024
                && !self.admission_timeout.is_zero()
                && self.admission_timeout <= Duration::from_secs(3600),
            "invalid native controller limits"
        );
        ensure!(
            !self.operation_timeout.is_zero()
                && self.operation_timeout <= Duration::from_secs(3600),
            "invalid native controller operation timeout"
        );
        Ok(())
    }
}

enum Host {
    Mounted(Arc<NativeMount>),
    #[cfg(any(test, feature = "protocol-test-support"))]
    Protocol {
        tree: Arc<Tree>,
        compiler: Arc<Compiler>,
        root: PathBuf,
        state: PathBuf,
        identity: [u8; 32],
        backend: String,
    },
}
impl Host {
    fn tree(&self) -> Arc<Tree> {
        match self {
            Self::Mounted(mount) => mount.tree(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { tree, .. } => tree.clone(),
        }
    }
    fn compiler(&self) -> Arc<Compiler> {
        match self {
            Self::Mounted(mount) => mount.compiler(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { compiler, .. } => compiler.clone(),
        }
    }
    fn root(&self) -> &Path {
        match self {
            Self::Mounted(mount) => mount.root(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { root, .. } => root,
        }
    }
    fn state_dir(&self) -> &Path {
        match self {
            Self::Mounted(mount) => mount.state_dir(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { state, .. } => state,
        }
    }
    fn identity(&self) -> [u8; 32] {
        match self {
            Self::Mounted(mount) => mount.workspace_identity(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { identity, .. } => *identity,
        }
    }
    fn backend(&self) -> &str {
        match self {
            Self::Mounted(mount) => mount.backend_identity(),
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { backend, .. } => backend,
        }
    }
    fn check_ready(&self) -> Result<()> {
        #[cfg(any(test, feature = "protocol-test-support"))]
        if matches!(self, Self::Protocol { .. }) {
            return Ok(());
        }
        // A stopped/unmounted provider exposes no matching synthetic marker.
        // Production callers cannot construct Host::Protocol or fabricate a
        // NativeMount from the metadata-only prepare() result.
        let mut bytes = Vec::new();
        File::open(self.root().join(".jj/native-workspace"))?
            .take(65)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes == hex(&self.identity()).as_bytes(),
            "native provider marker is unavailable or changed"
        );
        Ok(())
    }
    async fn capture(
        &self,
        limits: &SourceLimits,
        deadline: tokio::time::Instant,
    ) -> Result<SourceSnapshot> {
        match self {
            Self::Mounted(mount) => {
                // NFS needs a real client flush/quiescence fence. ProjFS needs
                // authoritative NTFS dirty capture, not a scan of Tree alone.
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = (mount, limits, deadline);
                    bail!(
                        "native provider source capture fence is not implemented on this platform"
                    );
                }
                #[cfg(target_os = "linux")]
                {
                    mount
                        .tree()
                        .freeze_source_wait(
                            output_core::SourceFrontendFence::TreeOnly,
                            limits,
                            deadline,
                        )
                        .await
                }
            }
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { tree, .. } => {
                tree.freeze_source_wait(
                    output_core::SourceFrontendFence::TreeOnly,
                    limits,
                    deadline,
                )
                .await
            }
        }
    }
    async fn publish(
        &self,
        tree: &NativeTree,
        expected: SourceVersion,
        id: &str,
        limits: &SourceLimits,
        scope: Option<&ActionScope>,
    ) -> Result<SourceReceipt> {
        let merge = resolved_merge(tree)?;
        match self {
            Self::Mounted(mount) => {
                let compiled = mount.compiler().compile_merge(&merge).await?;
                if let Some(scope) = scope {
                    scope.check()?;
                }
                mount
                    .projection()
                    .publish_source(
                        compiled.manifest(mount.tree().identity()),
                        expected,
                        id.into(),
                        limits.clone(),
                    )
                    .await
            }
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { tree, compiler, .. } => {
                let compiled = compiler.compile_merge(&merge).await?;
                if let Some(scope) = scope {
                    scope.check()?;
                }
                tree.install_source(
                    compiled.manifest(tree.identity()),
                    Some(expected),
                    id,
                    limits,
                )
            }
        }
    }
    async fn wait_source_idle(&self) -> Result<()> {
        match self {
            Self::Mounted(mount) => mount.wait_source_idle().await,
            // Protocol fixtures have synchronous Tree publication and no
            // detached native-provider jobs. They are not mounted acceptance.
            #[cfg(any(test, feature = "protocol-test-support"))]
            Self::Protocol { .. } => Ok(()),
        }
    }
}
fn resolved_merge(tree: &NativeTree) -> Result<m::TreeMerge> {
    tree.validate()?;
    ensure!(
        tree.terms.len() == 1 && (tree.labels.is_empty() || tree.labels == [String::new()]),
        "unresolved native source projection is not implemented; merge terms were not flattened"
    );
    Ok(m::TreeMerge {
        terms: vec![m::TreeId(tree.terms[0])],
        labels: vec![String::new()],
    })
}
fn projected(repository: &str, root: [u8; 32], generation: u64) -> ProjectionState {
    ProjectionState {
        repository: repository.into(),
        generation,
        tree: NativeTree {
            terms: vec![root],
            labels: Vec::new(),
        },
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    format: u32,
    repository: String,
    root: PathBuf,
    working_copy_state: PathBuf,
    workspace: [u8; 32],
    backend: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Action {
    Snapshot { policy: [u8; 32] },
    CheckOut { tree: NativeTree },
    Reset { tree: NativeTree, recover: bool },
}
impl Action {
    fn operation(&self) -> SourceOperation {
        match self {
            Self::CheckOut { .. } => SourceOperation::Install,
            _ => SourceOperation::Acknowledge,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    before: ProjectionState,
    action: Action,
    expected: SourceVersion,
    target: Option<[u8; 32]>,
    receipt: Option<ProjectionReceipt>,
    source_receipt: Option<SourceReceipt>,
    rejected: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Acknowledgement {
    state: ProjectionState,
    operation: String,
    receipt: Option<ProjectionReceipt>,
    retire: Vec<SourceReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    binding: Binding,
    baseline: ProjectionState,
    requests: BTreeMap<String, Intent>,
    acknowledgement: Option<Acknowledgement>,
}

struct Owner {
    directory: PrivateDir,
    _working_copy: PrivateDir,
    _lock: File,
}
struct Inner {
    host: Host,
    provider: Arc<dyn NativeObjectIo>,
    config: Config,
    owner: Owner,
    ledger: Mutex<Ledger>,
    poisoned: AtomicBool,
    session: Arc<AsyncMutex<()>>,
}
#[derive(Clone)]
pub struct Controller {
    inner: Arc<Inner>,
}

/// Explicit unmounted fixture configuration. Available only in the test-support
/// build; it is not native-provider acceptance or a user workspace fallback.
#[cfg(feature = "protocol-test-support")]
pub struct ProtocolFixture {
    pub tree: Arc<Tree>,
    pub compiler: Arc<Compiler>,
    pub root: PathBuf,
    pub state: PathBuf,
    pub workspace_identity: [u8; 32],
    pub backend: String,
}
impl Controller {
    #[cfg(feature = "protocol-test-support")]
    pub async fn open_protocol_fixture(
        fixture: ProtocolFixture,
        provider: Arc<dyn NativeObjectIo>,
        config: Config,
    ) -> Result<Self> {
        Self::open_host(
            Host::Protocol {
                tree: fixture.tree,
                compiler: fixture.compiler,
                root: fixture.root,
                state: fixture.state,
                identity: fixture.workspace_identity,
                backend: fixture.backend,
            },
            provider,
            config,
        )
        .await
    }
    /// Only an actual native mount is accepted. Metadata-only preparation is
    /// intentionally not a production constructor for a ready controller.
    pub async fn open(
        mount: Arc<NativeMount>,
        provider: Arc<dyn NativeObjectIo>,
        config: Config,
    ) -> Result<Self> {
        Self::open_host(Host::Mounted(mount), provider, config).await
    }
    async fn open_host(
        host: Host,
        provider: Arc<dyn NativeObjectIo>,
        mut config: Config,
    ) -> Result<Self> {
        config.validate()?;
        host.check_ready()?;
        config.working_copy_state = config.working_copy_state.canonicalize()?;
        ensure!(
            !config.working_copy_state.starts_with(host.root())
                && !host.root().starts_with(&config.working_copy_state),
            "jj state overlaps the native projection"
        );
        let working_copy = PrivateDir::open_existing(&config.working_copy_state)?;
        let path = host.state_dir().join("jj-controller");
        let directory = if path.try_exists()? {
            PrivateDir::open_existing(&path)?
        } else {
            PrivateDir::create(&path)?
        };
        let empty = fs::read_dir(directory.path())?.next().is_none();
        let binding = Binding {
            format: 1,
            repository: config.repository.clone(),
            root: host.root().canonicalize()?,
            working_copy_state: config.working_copy_state.clone(),
            workspace: host.identity(),
            backend: host.backend().into(),
        };
        let (root, generation) = host.tree().source_baseline()?;
        let ledger = if empty {
            Ledger {
                binding: binding.clone(),
                baseline: projected(&config.repository, root, generation),
                requests: BTreeMap::new(),
                acknowledgement: None,
            }
        } else {
            let ledger: Ledger =
                serde_json::from_slice(&directory.read("ledger.json", config.max_ledger_bytes)?)?;
            ensure!(
                ledger.binding == binding,
                "native controller ledger binding mismatch"
            );
            ledger
        };
        validate_ledger(&ledger, &config)?;
        let lock = directory.open_lock("owner.lock")?;
        lock.try_lock_exclusive()
            .context("native controller already owned")?;
        if empty {
            directory.replace("ledger.json", &serde_json::to_vec(&ledger)?)?;
        }
        let controller = Self {
            inner: Arc::new(Inner {
                host,
                provider,
                config,
                owner: Owner {
                    directory,
                    _working_copy: working_copy,
                    _lock: lock,
                },
                ledger: Mutex::new(ledger),
                poisoned: AtomicBool::new(false),
                session: Arc::new(AsyncMutex::new(())),
            }),
        };
        controller.reconcile_pending().await?;
        Ok(controller)
    }
    async fn admission(&self) -> Result<OwnedMutexGuard<()>> {
        tokio::time::timeout(
            self.inner.config.admission_timeout,
            self.inner.session.clone().lock_owned(),
        )
        .await
        .context("native controller admission timed out")
    }
    fn healthy(&self) -> Result<()> {
        ensure!(
            !self.inner.poisoned.load(Ordering::Acquire),
            "native controller durable state is uncertain; reopen for recovery"
        );
        self.inner.host.check_ready()
    }
    fn save(&self, ledger: &mut Ledger, next: Ledger) -> Result<()> {
        validate_ledger(&next, &self.inner.config)?;
        let bytes = serde_json::to_vec(&next)?;
        ensure!(
            bytes.len() <= self.inner.config.max_ledger_bytes,
            "native controller ledger limit exceeded"
        );
        if let Err(error) = self.inner.owner.directory.replace("ledger.json", &bytes) {
            self.inner.poisoned.store(true, Ordering::Release);
            return Err(error.into());
        }
        *ledger = next;
        Ok(())
    }
    /// Explicitly reconcile incomplete durable intents while owning the sole
    /// controller session. A missing receipt alone never proves nonapplication.
    pub async fn reconcile_pending(&self) -> Result<()> {
        let _guard = self.admission().await?;
        self.healthy()?;
        self.recover_drained().await
    }
    /// Metadata-only signal for a reconnecting transport. Ordinary ready-state
    /// reads need not wait behind a long-lived but already-checkpointed session.
    pub fn requires_reconciliation(&self) -> bool {
        self.inner.poisoned.load(Ordering::Acquire)
            || self
                .inner
                .ledger
                .lock()
                .unwrap()
                .requests
                .values()
                .any(|intent| !intent.rejected && intent.receipt.is_none())
    }
    // None denotes an admitted but unresolved request, not missing metadata.
    // Terminal receipts are immutable history and need no command-session gate.
    fn receipt_metadata(&self, id: &str) -> Option<ProjectionOutcome> {
        let ledger = self.inner.ledger.lock().unwrap();
        match ledger.requests.get(id) {
            None => Some(ProjectionOutcome::Unknown),
            Some(intent) if intent.rejected => Some(ProjectionOutcome::NotApplied),
            Some(intent) => intent.receipt.clone().map(ProjectionOutcome::Applied),
        }
    }
    async fn recover_drained(&self) -> Result<()> {
        let requests = self.inner.ledger.lock().unwrap().requests.clone();
        for (id, intent) in requests {
            if intent.rejected || intent.receipt.is_some() {
                continue;
            }
            // The session and exclusive owner prevent new controller actions.
            // A cancelled native publication can outlive its RPC, so drain the
            // actual provider before inspecting/finalizing its logical outcome.
            let deadline = tokio::time::Instant::now() + self.inner.config.operation_timeout;
            tokio::time::timeout_at(deadline, self.inner.host.wait_source_idle())
                .await
                .context("native publication recovery drain timed out")??;
            let frozen = self
                .inner
                .host
                .capture(&self.inner.config.source_limits, deadline)
                .await?;
            ensure!(
                tokio::time::Instant::now() < deadline,
                "native publication recovery deadline expired"
            );
            let Some(source) = self.inner.host.tree().source_receipt(&id)? else {
                // A terminal abort is justified by ALL of: exclusive ownership,
                // physical native publication drain, a held source exclusion
                // fence, and the exact original root/control generation still
                // being visible. Editor changes do not discard their bytes.
                let before = resolved_merge(&intent.before.tree)?;
                ensure!(
                    frozen.base_root() == before.terms[0].0
                        && frozen.version().generation == intent.before.generation,
                    "missing native receipt has a changed baseline; state preserved"
                );
                let mut ledger = self.inner.ledger.lock().unwrap();
                ensure!(
                    ledger.baseline == intent.before,
                    "native recovery baseline changed"
                );
                let mut next = ledger.clone();
                let pending = next
                    .requests
                    .get_mut(&id)
                    .context("native recovery intent disappeared")?;
                ensure!(
                    pending.receipt.is_none() && !pending.rejected,
                    "native recovery intent changed"
                );
                pending.rejected = true;
                self.save(&mut ledger, next)?;
                drop(frozen);
                continue;
            };
            verify_source(&id, &intent, &source)?;
            drop(frozen);
            let source = if let Action::CheckOut { tree } = &intent.action {
                // Core durability alone is not native publication completion.
                // Replay the exact provider-fenced request before adopting it.
                self.inner
                    .host
                    .publish(
                        tree,
                        intent.expected.clone(),
                        &id,
                        &self.inner.config.source_limits,
                        None,
                    )
                    .await?
            } else {
                source
            };
            self.complete(&id, source)?;
        }
        let (root, generation) = self.inner.host.tree().source_baseline()?;
        ensure!(
            self.inner.ledger.lock().unwrap().baseline
                == projected(&self.inner.config.repository, root, generation),
            "native controller and namespace baseline disagree; state preserved"
        );
        self.retire_acknowledged()?;
        Ok(())
    }
    fn prepare(
        &self,
        id: &str,
        before: &ProjectionState,
        action: Action,
        expected: SourceVersion,
    ) -> Result<()> {
        ensure!(
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
            "invalid native request identity"
        );
        let mut ledger = self.inner.ledger.lock().unwrap();
        ensure!(
            ledger.baseline == *before,
            "native controller baseline is stale"
        );
        ensure!(
            expected.generation == before.generation,
            "native namespace generation changed outside the controller"
        );
        ensure!(
            !ledger.requests.contains_key(id)
                && ledger.requests.len() < self.inner.config.max_pending_requests,
            "native request is already known or request budget is exhausted"
        );
        ensure!(
            ledger
                .requests
                .values()
                .all(|intent| intent.rejected || intent.receipt.is_some()),
            "another native outcome remains uncertain"
        );
        let target = match &action {
            Action::Snapshot { .. } => None,
            Action::CheckOut { tree } | Action::Reset { tree, .. } => {
                Some(resolved_merge(tree)?.terms[0].0)
            }
        };
        let mut next = ledger.clone();
        next.requests.insert(
            id.into(),
            Intent {
                before: before.clone(),
                action,
                expected,
                target,
                receipt: None,
                source_receipt: None,
                rejected: false,
            },
        );
        self.save(&mut ledger, next)
    }
    fn target(&self, id: &str, root: [u8; 32]) -> Result<()> {
        let mut ledger = self.inner.ledger.lock().unwrap();
        let mut next = ledger.clone();
        let intent = next.requests.get_mut(id).context("native intent missing")?;
        ensure!(
            intent.target.is_none_or(|target| target == root),
            "native target changed during replay"
        );
        intent.target = Some(root);
        self.save(&mut ledger, next)
    }
    fn rejected(&self, id: &str, error: anyhow::Error) -> ProjectionFailure {
        let mut ledger = self.inner.ledger.lock().unwrap();
        let mut next = ledger.clone();
        let Some(intent) = next.requests.get_mut(id) else {
            return ProjectionFailure::Ambiguous(error);
        };
        intent.rejected = true;
        match self.save(&mut ledger, next) {
            Ok(()) => ProjectionFailure::Rejected(error),
            Err(save) => ProjectionFailure::Ambiguous(
                save.context("native rejection could not be durably sealed"),
            ),
        }
    }
    fn complete(&self, id: &str, source: SourceReceipt) -> Result<ProjectionReceipt> {
        let mut ledger = self.inner.ledger.lock().unwrap();
        let intent = ledger.requests.get(id).context("native intent missing")?;
        verify_source(id, intent, &source)?;
        let receipt = ProjectionReceipt {
            request_id: id.into(),
            before: intent.before.clone(),
            after: projected(
                &self.inner.config.repository,
                source.root,
                source.version.generation,
            ),
        };
        if let Some(previous) = &intent.receipt {
            ensure!(previous == &receipt, "native receipt changed");
            return Ok(previous.clone());
        }
        let mut next = ledger.clone();
        let intent = next.requests.get_mut(id).unwrap();
        intent.receipt = Some(receipt.clone());
        intent.source_receipt = Some(source);
        next.baseline = receipt.after.clone();
        self.save(&mut ledger, next)?;
        Ok(receipt)
    }
    fn acknowledge_inner(
        &self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        let mut ledger = self.inner.ledger.lock().unwrap();
        if ledger.requests.is_empty()
            && ledger.acknowledgement.as_ref().is_some_and(|ack| {
                ack.state == *state
                    && ack.operation == operation.hex()
                    && ack.receipt.as_ref() == receipt
            })
        {
            drop(ledger);
            return self.retire_acknowledged();
        }
        ensure!(
            ledger.baseline == *state
                && ledger
                    .requests
                    .values()
                    .all(|intent| intent.rejected || intent.receipt.is_some()),
            "native acknowledgement has an uncertain or mismatching outcome"
        );
        if let Some(receipt) = receipt {
            ensure!(
                ledger
                    .requests
                    .get(&receipt.request_id)
                    .and_then(|intent| intent.receipt.as_ref())
                    == Some(receipt),
                "native acknowledgement receipt mismatch"
            );
        }
        let mut next = ledger.clone();
        let mut retire = next
            .acknowledgement
            .as_ref()
            .map(|ack| ack.retire.clone())
            .unwrap_or_default();
        retire.extend(
            next.requests
                .values()
                .filter_map(|intent| intent.source_receipt.clone()),
        );
        ensure!(
            retire.len() <= self.inner.config.max_pending_requests,
            "native receipt cleanup requires completion"
        );
        next.requests.clear();
        next.acknowledgement = Some(Acknowledgement {
            state: state.clone(),
            operation: operation.hex(),
            receipt: receipt.cloned(),
            retire,
        });
        self.save(&mut ledger, next)?;
        drop(ledger);
        // The jj ACK is already durable. Busy native readers may defer cleanup;
        // they never turn acknowledged data into an unknown/unapplied outcome.
        let _ = self.retire_acknowledged();
        Ok(())
    }
    fn retire_acknowledged(&self) -> Result<()> {
        let retire = self
            .inner
            .ledger
            .lock()
            .unwrap()
            .acknowledgement
            .as_ref()
            .map(|ack| ack.retire.clone())
            .unwrap_or_default();
        if retire.is_empty() {
            return Ok(());
        }
        let mut present = Vec::new();
        for receipt in &retire {
            if let Some(current) = self.inner.host.tree().source_receipt(&receipt.request_id)? {
                ensure!(&current == receipt, "acknowledged source receipt changed");
                present.push(current);
            }
        }
        if !present.is_empty() {
            self.inner
                .host
                .tree()
                .retire_source_receipts(&present, &self.inner.config.source_limits)?;
        }
        let mut ledger = self.inner.ledger.lock().unwrap();
        let mut next = ledger.clone();
        next.acknowledgement.as_mut().unwrap().retire.clear();
        self.save(&mut ledger, next)
    }
}
impl Controller {
    pub async fn begin_session(&self, expected: &ProjectionState) -> Result<Mutation> {
        let guard = self.admission().await?;
        self.healthy()?;
        if self.requires_reconciliation() {
            self.recover_drained().await?;
        }
        self.retire_acknowledged()?;
        ensure!(
            self.inspect()? == *expected,
            "native working-copy baseline is stale"
        );
        ensure!(
            self.inner
                .ledger
                .lock()
                .unwrap()
                .requests
                .values()
                .all(|intent| intent.rejected || intent.receipt.is_some()),
            "native publication outcome requires recovery"
        );
        Ok(Mutation {
            controller: self.clone(),
            current: expected.clone(),
            admission_scope: None,
            _guard: guard,
        })
    }
}
fn verify_source(id: &str, intent: &Intent, source: &SourceReceipt) -> Result<()> {
    ensure!(
        !intent.rejected
            && source.request_id == id
            && source.operation == intent.action.operation()
            && source.expected.as_ref() == Some(&intent.expected)
            && intent.target == Some(source.root)
            && intent.expected.generation == intent.before.generation
            && intent.expected.generation.checked_add(1) == Some(source.version.generation),
        "source publication receipt does not match native intent"
    );
    Ok(())
}
fn validate_ledger(ledger: &Ledger, config: &Config) -> Result<()> {
    ensure!(
        ledger.binding.format == 1
            && ledger.binding.repository == config.repository
            && ledger.requests.len() <= config.max_pending_requests,
        "invalid native ledger header or size"
    );
    ledger.baseline.validate(&config.repository)?;
    resolved_merge(&ledger.baseline.tree)?;
    for (id, intent) in &ledger.requests {
        ensure!(
            !id.is_empty() && id.len() <= 128,
            "invalid retained request identity"
        );
        intent.before.validate(&config.repository)?;
        ensure!(
            !(intent.rejected && intent.receipt.is_some()),
            "native intent is both rejected and applied"
        );
        if let Some(receipt) = &intent.receipt {
            ensure!(
                receipt.request_id == *id
                    && receipt.before == intent.before
                    && intent.target == receipt.after.tree.terms.first().copied(),
                "invalid retained native receipt"
            );
            receipt.after.validate(&config.repository)?;
            let source = intent
                .source_receipt
                .as_ref()
                .context("native receipt lacks durable source proof")?;
            ensure!(
                receipt.after
                    == projected(&config.repository, source.root, source.version.generation)
                    && ledger.baseline.generation >= receipt.after.generation,
                "native result does not match its durable source proof"
            );
            verify_source(
                id,
                intent,
                intent
                    .source_receipt
                    .as_ref()
                    .context("native receipt lacks durable source proof")?,
            )?;
        } else {
            ensure!(
                intent.source_receipt.is_none(),
                "native source proof lacks matching result"
            );
        }
    }
    if let Some(ack) = &ledger.acknowledgement {
        ack.state.validate(&config.repository)?;
        ensure!(
            ack.retire.len() <= config.max_pending_requests
                && OperationId::try_from_hex(&ack.operation).is_some(),
            "invalid native acknowledgement"
        );
    }
    Ok(())
}

impl ProjectionConnector for Controller {
    fn connect(&self, binding: &ProjectionBinding) -> Result<Arc<dyn NativeProjectionController>> {
        self.healthy()?;
        let expected = &self.inner.ledger.lock().unwrap().binding;
        ensure!(
            binding.root == expected.root
                && binding.state_path == expected.working_copy_state
                && binding.repository == expected.repository,
            "native working-copy connector binding mismatch"
        );
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl NativeProjectionController for Controller {
    fn inspect(&self) -> Result<ProjectionState> {
        self.healthy()?;
        let (root, generation) = self.inner.host.tree().source_baseline()?;
        let baseline = self.inner.ledger.lock().unwrap().baseline.clone();
        ensure!(
            baseline == projected(&self.inner.config.repository, root, generation),
            "native publication requires recovery"
        );
        Ok(baseline)
    }
    async fn begin(&self, expected: &ProjectionState) -> Result<Box<dyn NativeProjectionMutation>> {
        Ok(Box::new(self.begin_session(expected).await?))
    }
    async fn receipt(&self, id: &str) -> Result<ProjectionOutcome> {
        self.healthy()?;
        if let Some(outcome) = self.receipt_metadata(id) {
            return Ok(outcome);
        }
        // Only unresolved admitted work needs exclusive admission and physical
        // recovery. Waiting for ordinary metadata lookup would deadlock a
        // caller querying a receipt while retaining its own live mutation.
        let _guard = self.admission().await?;
        self.healthy()?;
        if let Some(outcome) = self.receipt_metadata(id) {
            return Ok(outcome);
        }
        self.recover_drained().await?;
        self.receipt_metadata(id)
            .context("native receipt remains unresolved after recovery")
    }
    async fn acknowledge(
        &self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        let _guard = self.admission().await?;
        self.healthy()?;
        self.acknowledge_inner(state, operation, receipt)
    }
}
pub struct Mutation {
    controller: Controller,
    current: ProjectionState,
    admission_scope: Option<ActionScope>,
    _guard: OwnedMutexGuard<()>,
}
impl Mutation {
    /// Set one transport action's absolute deadline and independent EOF flag.
    /// The next action may set a new scope; an admitted physical stage is never
    /// aborted merely because this scope expires while that stage is running.
    pub fn set_admission_scope(&mut self, scope: ActionScope) {
        self.admission_scope = Some(scope);
    }
    pub fn set_action_scope(&mut self, scope: ActionScope) {
        self.set_admission_scope(scope);
    }
    fn action_scope(&self) -> ActionScope {
        let deadline = tokio::time::Instant::now() + self.controller.inner.config.operation_timeout;
        self.admission_scope.as_ref().map_or_else(
            || ActionScope::new(deadline, Arc::new(AtomicBool::new(false))),
            |scope| scope.bounded(deadline),
        )
    }
    fn check_admission(
        &self,
        id: &str,
        scope: &ActionScope,
    ) -> std::result::Result<(), ProjectionFailure> {
        scope.check().map_err(|error| {
            if self
                .controller
                .inner
                .ledger
                .lock()
                .unwrap()
                .requests
                .contains_key(id)
            {
                ambiguous(error)
            } else {
                rejected(error)
            }
        })
    }
    fn replay(
        &mut self,
        id: &str,
        action: &Action,
    ) -> std::result::Result<Option<ProjectionReceipt>, ProjectionFailure> {
        let intent = self
            .controller
            .inner
            .ledger
            .lock()
            .unwrap()
            .requests
            .get(id)
            .cloned();
        let Some(intent) = intent else {
            return Ok(None);
        };
        if &intent.action != action {
            return Err(ambiguous(anyhow::anyhow!(
                "native request identity was reused with a different action"
            )));
        }
        if intent.rejected {
            return Err(rejected(anyhow::anyhow!(
                "native request was durably rejected"
            )));
        }
        let Some(receipt) = intent.receipt else {
            return Err(ambiguous(anyhow::anyhow!(
                "native request outcome requires receipt recovery"
            )));
        };
        if self.current != receipt.before && self.current != receipt.after {
            return Err(ambiguous(anyhow::anyhow!("native replay baseline moved")));
        }
        self.current = receipt.after.clone();
        Ok(Some(receipt))
    }
}
impl Mutation {
    pub async fn snapshot_with_policy(
        &mut self,
        id: &str,
        policy: &dyn SnapshotPolicy,
    ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
        let scope = self.action_scope();
        self.check_admission(id, &scope)?;
        self.controller.healthy().map_err(rejected)?;
        let fingerprint = policy.fingerprint();
        if let Some(receipt) = self.replay(
            id,
            &Action::Snapshot {
                policy: fingerprint,
            },
        )? {
            scope.check().map_err(ambiguous)?;
            return Ok((receipt, SnapshotStats::default()));
        }
        let snapshot = Arc::new(
            self.controller
                .inner
                .host
                .capture(
                    &self.controller.inner.config.source_limits,
                    scope.deadline(),
                )
                .await
                .map_err(rejected)?,
        );
        scope.check().map_err(rejected)?;
        let base = self
            .controller
            .inner
            .host
            .compiler()
            .compile_tree(m::TreeId(snapshot.base_root()))
            .await
            .map_err(rejected)?;
        scope.check().map_err(rejected)?;
        let plan = snapshot::plan(&self.controller, snapshot.clone(), &base, policy, &scope)
            .await
            .map_err(rejected)?;
        scope.check().map_err(rejected)?;
        self.controller
            .prepare(
                id,
                &self.current,
                Action::Snapshot {
                    policy: fingerprint,
                },
                snapshot.version().clone(),
            )
            .map_err(rejected)?;
        scope.check().map_err(ambiguous)?;
        let root =
            match snapshot::write(&self.controller, snapshot.clone(), plan.entries, &scope).await {
                Ok(root) => root,
                Err(error) if error.downcast_ref::<ActionScopeError>().is_some() => {
                    return Err(ambiguous(error));
                }
                Err(error) => return Err(self.controller.rejected(id, error)),
            };
        scope.check().map_err(ambiguous)?;
        self.controller.target(id, root.0).map_err(ambiguous)?;
        let snapshot = Arc::try_unwrap(snapshot).map_err(|_| {
            ambiguous(anyhow::anyhow!(
                "native snapshot still has physical readers"
            ))
        })?;
        let expected = snapshot.version().clone();
        scope.check().map_err(ambiguous)?;
        let source = snapshot
            .acknowledge_root(
                root.0,
                expected,
                id,
                &self.controller.inner.config.source_limits,
            )
            .map_err(ambiguous)?;
        let receipt = self.controller.complete(id, source).map_err(ambiguous)?;
        self.current = receipt.after.clone();
        // Bookkeeping of an already-applied root is completed even if the
        // caller deadline/EOF arrived during that indivisible publication.
        scope.check().map_err(ambiguous)?;
        Ok((receipt, plan.stats))
    }
}
fn rejected(error: impl Into<anyhow::Error>) -> ProjectionFailure {
    ProjectionFailure::Rejected(error.into())
}
fn ambiguous(error: impl Into<anyhow::Error>) -> ProjectionFailure {
    ProjectionFailure::Ambiguous(error.into())
}

#[async_trait]
impl NativeProjectionMutation for Mutation {
    async fn snapshot(
        &mut self,
        id: &str,
        options: &SnapshotOptions<'_>,
    ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
        self.snapshot_with_policy(id, &policy::OptionsPolicy { options })
            .await
    }
    async fn check_out(
        &mut self,
        id: &str,
        tree: &NativeTree,
    ) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure> {
        let scope = self.action_scope();
        self.check_admission(id, &scope)?;
        self.controller.healthy().map_err(rejected)?;
        let merge = resolved_merge(tree).map_err(rejected)?;
        if let Some(receipt) = self.replay(id, &Action::CheckOut { tree: tree.clone() })? {
            scope.check().map_err(ambiguous)?;
            return Ok((receipt, CheckoutStats::default()));
        }
        let target = self
            .controller
            .inner
            .host
            .compiler()
            .compile_merge(&merge)
            .await
            .map_err(rejected)?;
        scope.check().map_err(rejected)?;
        let snapshot = Arc::new(
            self.controller
                .inner
                .host
                .capture(
                    &self.controller.inner.config.source_limits,
                    scope.deadline(),
                )
                .await
                .map_err(rejected)?,
        );
        scope.check().map_err(rejected)?;
        let base = self
            .controller
            .inner
            .host
            .compiler()
            .compile_tree(m::TreeId(snapshot.base_root()))
            .await
            .map_err(rejected)?;
        scope.check().map_err(rejected)?;
        snapshot::unchanged(&snapshot, &base, &scope).map_err(rejected)?;
        let stats = snapshot::checkout_stats(&base, &target);
        let expected = snapshot.version().clone();
        scope.check().map_err(rejected)?;
        self.controller
            .prepare(
                id,
                &self.current,
                Action::CheckOut { tree: tree.clone() },
                expected.clone(),
            )
            .map_err(rejected)?;
        drop(snapshot);
        scope.check().map_err(ambiguous)?;
        let source = self
            .controller
            .inner
            .host
            .publish(
                tree,
                expected,
                id,
                &self.controller.inner.config.source_limits,
                Some(&scope),
            )
            .await
            .map_err(ambiguous)?;
        let receipt = self.controller.complete(id, source).map_err(ambiguous)?;
        self.current = receipt.after.clone();
        scope.check().map_err(ambiguous)?;
        Ok((receipt, stats))
    }
    async fn reset(
        &mut self,
        id: &str,
        tree: &NativeTree,
        recover: bool,
    ) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
        let scope = self.action_scope();
        self.check_admission(id, &scope)?;
        self.controller.healthy().map_err(rejected)?;
        let merge = resolved_merge(tree).map_err(rejected)?;
        if let Some(receipt) = self.replay(
            id,
            &Action::Reset {
                tree: tree.clone(),
                recover,
            },
        )? {
            scope.check().map_err(ambiguous)?;
            return Ok(receipt);
        }
        self.controller
            .inner
            .host
            .compiler()
            .compile_merge(&merge)
            .await
            .map_err(rejected)?;
        scope.check().map_err(rejected)?;
        let snapshot = self
            .controller
            .inner
            .host
            .capture(
                &self.controller.inner.config.source_limits,
                scope.deadline(),
            )
            .await
            .map_err(rejected)?;
        let expected = snapshot.version().clone();
        scope.check().map_err(rejected)?;
        self.controller
            .prepare(
                id,
                &self.current,
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
                merge.terms[0].0,
                expected,
                id,
                &self.controller.inner.config.source_limits,
            )
            .map_err(ambiguous)?;
        let receipt = self.controller.complete(id, source).map_err(ambiguous)?;
        self.current = receipt.after.clone();
        scope.check().map_err(ambiguous)?;
        Ok(receipt)
    }
    async fn finish(
        &mut self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        let scope = self.action_scope();
        scope.check()?;
        ensure!(&self.current == state, "native finish baseline mismatch");
        self.controller.healthy()?;
        scope.check()?;
        self.controller
            .acknowledge_inner(state, operation, receipt)?;
        scope.check()
    }
}

#[cfg(test)]
mod tests;
