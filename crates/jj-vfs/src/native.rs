//! Native Tinytree working copies backed exclusively by a live lazy projection.
//!
//! The controller owns mount/provider semantics, immutable reads, frozen dirty
//! capture, and authoritative checkout receipts. This adapter does not scan or
//! materialize the working directory. Its state directory must be private and
//! outside the projected root; it is authoritative jj state, not an LRU cache.
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use fs2::FileExt;
use jj_lib::{
    backend::TreeId,
    commit::Commit,
    conflict_labels::ConflictLabels,
    merge::Merge,
    merged_tree::MergedTree,
    object_id::ObjectId,
    op_store::OperationId,
    operation::Operation,
    ref_name::{WorkspaceName, WorkspaceNameBuf},
    repo_path::RepoPathBuf,
    settings::UserSettings,
    store::Store,
    working_copy::*,
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};

pub const NAME: &str = "tinytree-vfs-v1";
const STATE: &str = "native-vfs.json";
const LOCK: &str = "native-vfs.lock";
const MAX_STATE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TERMS: usize = 1023;
const MAX_LABEL_BYTES: usize = 256 * 1024;

/// Exact ordered native merge terms, including remove/base terms and labels.
/// No simplification, conflict rendering or file reads occur in this adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTree {
    pub terms: Vec<[u8; 32]>,
    pub labels: Vec<String>,
}
impl NativeTree {
    pub fn from_merged_tree(tree: &MergedTree) -> Result<Self> {
        let result = Self {
            terms: tree
                .tree_ids()
                .iter()
                .map(|id| {
                    id.as_bytes()
                        .try_into()
                        .context("native tree identity must be 32 bytes")
                })
                .collect::<Result<_>>()?,
            labels: tree.labels().as_slice().to_vec(),
        };
        result.validate()?;
        Ok(result)
    }
    /// Validate merge arity and bounded persisted-label representation.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.terms.is_empty() && self.terms.len() <= MAX_TERMS && self.terms.len() % 2 == 1,
            "invalid native merged-tree arity"
        );
        ensure!(
            self.labels.is_empty() || self.labels.len() == self.terms.len(),
            "invalid native conflict label arity"
        );
        ensure!(
            self.labels
                .iter()
                .try_fold(0usize, |sum, label| sum.checked_add(label.len()))
                .is_some_and(|bytes| bytes <= MAX_LABEL_BYTES),
            "native conflict labels exceed state budget"
        );
        Ok(())
    }
    fn merged(&self, store: Arc<Store>) -> Result<MergedTree> {
        self.validate()?;
        Ok(MergedTree::new(
            store,
            Merge::from_vec(
                self.terms
                    .iter()
                    .map(|id| TreeId::new(id.to_vec()))
                    .collect::<Vec<_>>(),
            ),
            ConflictLabels::from_vec(self.labels.clone()),
        ))
    }
}

/// The generation is a controller-baseline epoch, not an editor-write counter.
/// Ordinary dirty edits must still permit a snapshot of this baseline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionState {
    pub repository: String,
    pub generation: u64,
    pub tree: NativeTree,
}
impl ProjectionState {
    /// Validate this state against the trusted configured repository identity.
    pub fn validate(&self, repository: &str) -> Result<()> {
        ensure!(
            self.repository == repository,
            "native projection repository mismatch"
        );
        self.tree.validate()
    }
}

/// Trusted connector configuration, never repository-selected network settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionBinding {
    pub root: PathBuf,
    pub state_path: PathBuf,
    pub repository: String,
}

/// An authoritative, replayable result for exactly one durable client intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionReceipt {
    pub request_id: String,
    pub before: ProjectionState,
    pub after: ProjectionState,
}

/// `NotApplied` must be an authoritative terminal/sealed outcome: absence of an
/// idempotency record, a lost connection or a cache miss is not such a proof.
#[derive(Clone, Debug)]
pub enum ProjectionOutcome {
    Applied(ProjectionReceipt),
    NotApplied,
    Unknown,
}

/// Distinguish a guaranteed no-op rejection from an uncertain remote result.
/// Only `Rejected` permits dropping the durable intent automatically.
#[derive(Debug)]
pub enum ProjectionFailure {
    Rejected(anyhow::Error),
    Ambiguous(anyhow::Error),
}
impl std::fmt::Display for ProjectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(error) | Self::Ambiguous(error) => write!(f, "{error}"),
        }
    }
}
impl std::error::Error for ProjectionFailure {}

pub trait ProjectionConnector: Send + Sync {
    /// Attest root/repository/admin binding and a live native provider BEFORE
    /// returning. Never create a normal checkout when the mount is unavailable.
    fn connect(&self, binding: &ProjectionBinding) -> Result<Arc<dyn NativeProjectionController>>;
}

#[async_trait]
pub trait NativeProjectionController: Send + Sync {
    /// Read-only mounted-provider inspection. Must not hydrate ordinary files.
    fn inspect(&self) -> Result<ProjectionState>;
    /// Acquire the controller's mutation/frozen-generation session. Physical
    /// jobs retain its fencing through cancellation and native completion.
    async fn begin(&self, expected: &ProjectionState) -> Result<Box<dyn NativeProjectionMutation>>;
    async fn receipt(&self, request_id: &str) -> Result<ProjectionOutcome>;
    /// Idempotent acknowledgement after exact jj state is durably installed.
    async fn acknowledge(
        &self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()>;
}

#[async_trait]
pub trait NativeProjectionMutation: Send {
    /// Freeze/capture changed bytes, honoring jj tracking/ignore/size policy.
    /// Reuse unchanged native IDs; do not read them merely for hashing.
    async fn snapshot(
        &mut self,
        request_id: &str,
        options: &SnapshotOptions<'_>,
    ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure>;
    /// Publish a whole lazy namespace generation, not per-file staging loops.
    async fn check_out(
        &mut self,
        request_id: &str,
        tree: &NativeTree,
    ) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure>;
    /// Change the logical baseline WITHOUT touching user files. Recovery must
    /// not assume old tree objects remain available or overwrite dirty edits.
    async fn reset(
        &mut self,
        request_id: &str,
        tree: &NativeTree,
        recover: bool,
    ) -> std::result::Result<ProjectionReceipt, ProjectionFailure>;
    /// Finish while still holding this mutation's controller fence. Dropping a
    /// session on an error is not acknowledgement or permission for client GC.
    async fn finish(
        &mut self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Action {
    Snapshot,
    CheckOut { tree: NativeTree },
    Reset { tree: NativeTree, recover: bool },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    request_id: String,
    before: ProjectionState,
    action: Action,
    receipt: Option<ProjectionReceipt>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Acknowledgement {
    operation: String,
    state: ProjectionState,
    receipt: Option<ProjectionReceipt>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    root: PathBuf,
    repository: String,
    operation: String,
    workspace: String,
    projection: ProjectionState,
    pending: Option<Pending>,
    unfinished: Option<ProjectionReceipt>,
    acknowledgement: Option<Acknowledgement>,
}
impl State {
    fn validate(&self, binding: &ProjectionBinding) -> Result<()> {
        ensure!(
            self.version == 1 && self.root == binding.root && self.repository == binding.repository,
            "native working-copy state binding/version mismatch"
        );
        ensure!(
            !self.workspace.is_empty() && self.workspace.len() <= 1024,
            "invalid native workspace name"
        );
        ensure!(
            self.operation.len() <= 256
                && !self.operation.is_empty()
                && OperationId::try_from_hex(&self.operation).is_some(),
            "invalid operation identity"
        );
        self.projection.validate(&binding.repository)?;
        ensure!(
            (self.pending.is_none() && self.unfinished.is_none()) || self.acknowledgement.is_none(),
            "inconsistent native recovery state"
        );
        if let Some(receipt) = &self.unfinished {
            validate_request_id(&receipt.request_id)?;
            receipt.before.validate(&binding.repository)?;
            receipt.after.validate(&binding.repository)?;
            ensure!(
                receipt.after == self.projection
                    && receipt.after.generation > receipt.before.generation,
                "unfinished native result does not match projection"
            );
        }
        if let Some(pending) = &self.pending {
            validate_request_id(&pending.request_id)?;
            ensure!(
                pending.before == self.projection,
                "pending native baseline mismatch"
            );
            match &pending.action {
                Action::Snapshot => {}
                Action::CheckOut { tree } | Action::Reset { tree, .. } => tree.validate()?,
            }
            if let Some(receipt) = &pending.receipt {
                validate_receipt(pending, receipt)?;
            }
        }
        if let Some(ack) = &self.acknowledgement {
            ensure!(
                ack.operation == self.operation && ack.state == self.projection,
                "native acknowledgement state mismatch"
            );
            if let Some(receipt) = &ack.receipt {
                validate_request_id(&receipt.request_id)?;
                receipt.before.validate(&binding.repository)?;
                receipt.after.validate(&binding.repository)?;
                ensure!(
                    receipt.after == self.projection,
                    "native acknowledgement receipt mismatch"
                );
            }
        }
        Ok(())
    }
    fn read(directory: &PrivateDir, binding: &ProjectionBinding) -> Result<Self> {
        let state: Self = serde_json::from_slice(&directory.read(STATE, MAX_STATE_BYTES)?)?;
        state.validate(binding)?;
        Ok(state)
    }
    fn save(&self, directory: &PrivateDir, binding: &ProjectionBinding) -> Result<()> {
        self.validate(binding)?;
        let bytes = serde_json::to_vec(self)?;
        ensure!(
            bytes.len() <= MAX_STATE_BYTES,
            "native working-copy state exceeds budget"
        );
        directory.replace(STATE, &bytes)?;
        Ok(())
    }
}
fn validate_request_id(id: &str) -> Result<()> {
    ensure!(
        uuid::Uuid::parse_str(id).is_ok(),
        "invalid native projection request identity"
    );
    Ok(())
}
fn validate_receipt(pending: &Pending, receipt: &ProjectionReceipt) -> Result<()> {
    ensure!(
        receipt.request_id == pending.request_id && receipt.before == pending.before,
        "native projection receipt does not match its intent"
    );
    receipt.after.validate(&pending.before.repository)?;
    ensure!(
        receipt.after.generation > pending.before.generation,
        "native projection generation did not advance"
    );
    match &pending.action {
        Action::Snapshot => {}
        Action::CheckOut { tree } | Action::Reset { tree, .. } => ensure!(
            &receipt.after.tree == tree,
            "native projection receipt changed requested merge terms"
        ),
    }
    Ok(())
}
fn binding(root: &Path, state_path: &Path, repository: &str) -> Result<ProjectionBinding> {
    ensure!(
        !repository.is_empty()
            && repository.len() <= 256
            && repository
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
        "invalid configured native repository identity"
    );
    let root = root.canonicalize().context("resolve native mount root")?;
    let state_path = state_path
        .canonicalize()
        .context("resolve native working-copy state")?;
    ensure!(
        !root.starts_with(&state_path) && !state_path.starts_with(&root),
        "native working-copy state must be outside the projection"
    );
    Ok(ProjectionBinding {
        root,
        state_path,
        repository: repository.into(),
    })
}
fn native_store(store: &Store) -> Result<()> {
    ensure!(
        store.backend().name() == "tinytree-native-v1"
            && store.commit_id_length() == 32
            && store.empty_tree_id().as_bytes().len() == 32,
        "native VFS requires the Tinytree native backend"
    );
    Ok(())
}
fn state_error(error: impl Into<anyhow::Error>) -> WorkingCopyStateError {
    let error = error.into();
    WorkingCopyStateError {
        message: format!("Native VFS working-copy state: {error:#}"),
        err: error.into(),
    }
}
fn snapshot_error(error: impl Into<anyhow::Error>) -> SnapshotError {
    let error = error.into();
    SnapshotError::Other {
        message: format!("Native VFS snapshot: {error:#}"),
        err: error.into(),
    }
}
fn checkout_error(error: impl Into<anyhow::Error>) -> CheckoutError {
    let error = error.into();
    CheckoutError::Other {
        message: format!("Native VFS checkout: {error:#}"),
        err: error.into(),
    }
}
fn reset_error(error: impl Into<anyhow::Error>) -> ResetError {
    let error = error.into();
    ResetError::Other {
        message: format!("Native VFS reset: {error:#}"),
        err: error.into(),
    }
}

pub struct NativeVfsWorkingCopyFactory {
    repository: String,
    connector: Arc<dyn ProjectionConnector>,
}
impl NativeVfsWorkingCopyFactory {
    /// The repository identity comes from trusted host configuration.
    pub fn new(repository: String, connector: Arc<dyn ProjectionConnector>) -> Self {
        Self {
            repository,
            connector,
        }
    }
    fn connect(
        &self,
        root: &Path,
        state_path: &Path,
        store: &Store,
    ) -> Result<(
        ProjectionBinding,
        Arc<PrivateDir>,
        Arc<dyn NativeProjectionController>,
    )> {
        native_store(store)?;
        let binding = binding(root, state_path, &self.repository)?;
        // Neither open_existing nor connect is allowed to create/repair state.
        let directory = Arc::new(PrivateDir::open_existing(&binding.state_path)?);
        let controller = self.connector.connect(&binding)?;
        controller.inspect()?.validate(&self.repository)?;
        Ok((binding, directory, controller))
    }
    /// Install metadata for an exact retained working-copy snapshot on a fresh
    /// lazy projection, without creating a jj operation or changing user files.
    ///
    /// The caller must hold its restore journal/command fence and independently
    /// verify OP/VIEW identity, exact index associations and supported quiescent
    /// capture provenance. These arguments do not prove source intent absence.
    /// Source request IDs/receipts and host-bound state must never be replayed.
    /// Existing state is never adopted, overwritten or repaired on failure.
    pub fn restore_fresh_working_copy(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        state_path: PathBuf,
        operation: OperationId,
        workspace: WorkspaceNameBuf,
        expected_tree: &NativeTree,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let restore = || -> Result<Box<dyn WorkingCopy>> {
            ensure!(
                operation.as_bytes().len() == 64,
                "retained operation identity must be 64 bytes"
            );
            expected_tree.validate()?;
            self.initialize_fresh(
                store,
                root,
                state_path,
                operation,
                workspace,
                Some(expected_tree),
            )
        };
        restore().map_err(state_error)
    }
    fn initialize_fresh(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        state_path: PathBuf,
        operation: OperationId,
        workspace: WorkspaceNameBuf,
        expected_tree: Option<&NativeTree>,
    ) -> Result<Box<dyn WorkingCopy>> {
        let (binding, directory, controller) = self.connect(&root, &state_path, &store)?;
        let lock = directory.open_lock(LOCK)?;
        lock.lock_exclusive()?;
        if expected_tree.is_some() {
            // A restore must not adopt pending state or unknown foreign files.
            ensure!(
                std::fs::read_dir(directory.path())?
                    .all(|entry| entry.is_ok_and(|entry| entry.file_name() == LOCK)),
                "native working-copy directory is not fresh"
            );
        } else {
            match directory.read(STATE, MAX_STATE_BYTES) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => bail!("native working-copy state already exists"),
            }
        }
        let projection = controller.inspect()?;
        projection.validate(&binding.repository)?;
        if let Some(expected) = expected_tree {
            ensure!(
                &projection.tree == expected,
                "restored native projection differs from retained tree"
            );
        }
        let state = State {
            version: 1,
            root: binding.root.clone(),
            repository: binding.repository.clone(),
            operation: operation.hex(),
            workspace: workspace.as_str().into(),
            projection,
            pending: None,
            unfinished: None,
            acknowledgement: None,
        };
        state.save(&directory, &binding)?;
        Ok(Box::new(NativeWorkingCopy::new(
            store, binding, directory, controller, state,
        )?))
    }
    /// Explicit recovery, not an automatic reset. The caller supplies an actual
    /// retained jj operation from this workspace's op store. For an applied
    /// mutation its WC commit tree must equal the authoritative receipt exactly.
    /// A missing outcome leaves the durable intent untouched.
    pub async fn recover_pending(
        &self,
        store: Arc<Store>,
        root: &Path,
        state_path: &Path,
        operation: Option<&Operation>,
        expected_receipt: Option<&ProjectionReceipt>,
    ) -> Result<RecoveryOutcome> {
        let (binding, directory, controller) = self.connect(root, state_path, &store)?;
        let lock = directory.open_lock(LOCK)?;
        lock.lock_exclusive()?;
        let mut state = State::read(&directory, &binding)?;
        if let Some(ack) = &state.acknowledgement {
            let id = OperationId::try_from_hex(&ack.operation)
                .context("invalid acknowledgement operation")?;
            controller
                .acknowledge(&ack.state, &id, ack.receipt.as_ref())
                .await?;
            state.acknowledgement = None;
            state.save(&directory, &binding)?;
        }
        let pending = match state.pending.clone() {
            Some(pending) => pending,
            None => match state.unfinished.clone() {
                Some(receipt) => Pending {
                    request_id: receipt.request_id.clone(),
                    before: receipt.before.clone(),
                    action: Action::Snapshot,
                    receipt: Some(receipt),
                },
                None => return Ok(RecoveryOutcome::NoPending),
            },
        };
        match controller.receipt(&pending.request_id).await? {
            ProjectionOutcome::Unknown => {
                bail!("native mutation outcome remains unknown; state preserved")
            }
            ProjectionOutcome::NotApplied => {
                ensure!(
                    pending.receipt.is_none() && expected_receipt.is_none(),
                    "controller contradicted an acknowledged native mutation"
                );
                ensure!(
                    controller.inspect()? == pending.before,
                    "unapplied native baseline changed"
                );
                state.pending = None;
                // Retire the host's sealed rejection only after this exact
                // no-op recovery is durable. A prior applied snapshot still
                // needs its real jj operation proof and must not be ACKed as
                // the older operation merely because a later call did not run.
                if state.unfinished.is_none() {
                    state.acknowledgement = Some(Acknowledgement {
                        operation: state.operation.clone(),
                        state: state.projection.clone(),
                        receipt: None,
                    });
                }
                state.save(&directory, &binding)?;
                if let Some(ack) = &state.acknowledgement {
                    let operation = OperationId::try_from_hex(&ack.operation)
                        .context("invalid recovered no-op operation")?;
                    controller.acknowledge(&ack.state, &operation, None).await?;
                    state.acknowledgement = None;
                    state.save(&directory, &binding)?;
                }
                Ok(RecoveryOutcome::NotApplied)
            }
            ProjectionOutcome::Applied(receipt) => {
                validate_receipt(&pending, &receipt)?;
                ensure!(
                    pending
                        .receipt
                        .as_ref()
                        .is_none_or(|saved| saved == &receipt)
                        && expected_receipt.is_none_or(|expected| expected == &receipt),
                    "native mutation receipt changed during recovery"
                );
                ensure!(
                    controller.inspect()? == receipt.after,
                    "native projection moved since its receipt"
                );
                let Some(operation) = operation else {
                    return Ok(RecoveryOutcome::OperationRequired(receipt));
                };
                let view = operation.view().await?;
                let id = view
                    .get_wc_commit_id(WorkspaceName::new(&state.workspace))
                    .context("recovery operation has no matching workspace")?;
                let tree = store.get_commit_async(id).await?.tree();
                ensure!(
                    NativeTree::from_merged_tree(&tree)? == receipt.after.tree,
                    "recovery operation does not describe the authoritative projected tree"
                );
                state.operation = operation.id().hex();
                state.projection = receipt.after.clone();
                state.pending = None;
                state.unfinished = None;
                state.acknowledgement = Some(Acknowledgement {
                    operation: state.operation.clone(),
                    state: state.projection.clone(),
                    receipt: Some(receipt.clone()),
                });
                state.save(&directory, &binding)?;
                controller
                    .acknowledge(&state.projection, operation.id(), Some(&receipt))
                    .await?;
                state.acknowledgement = None;
                state.save(&directory, &binding)?;
                Ok(RecoveryOutcome::Recovered)
            }
        }
    }
}
#[derive(Clone, Debug)]
pub enum RecoveryOutcome {
    NoPending,
    NotApplied,
    OperationRequired(ProjectionReceipt),
    Recovered,
}

/// Read-only persisted roots for retention integration. Pending targets and
/// results are additional roots, not permission to mark a workspace Clean.
#[derive(Clone, Debug)]
pub struct RetainedState {
    pub operation: OperationId,
    pub workspace: WorkspaceNameBuf,
    pub trees: Vec<NativeTree>,
    pub pending: bool,
    /// Exact bounded persisted input, retained for full-state freshness checks.
    /// Never include these private administrative bytes in public audit output.
    pub persisted_bytes: Arc<[u8]>,
    pub state_fingerprint: [u8; 64],
    pub encoded_bytes: usize,
}
pub fn read_retained_state(
    root: &Path,
    state_path: &Path,
    repository: &str,
) -> Result<RetainedState> {
    read_retained_state_bounded(root, state_path, repository, MAX_STATE_BYTES)
}

/// Read the actual private state under the caller's byte cap BEFORE decoding
/// JSON or constructing the retained roots. The implementation's absolute
/// format cap still applies if the caller supplies a larger limit.
pub fn read_retained_state_bounded(
    root: &Path,
    state_path: &Path,
    repository: &str,
    max_bytes: usize,
) -> Result<RetainedState> {
    ensure!(
        max_bytes > 0,
        "native retained-state read limit must be positive"
    );
    let binding = binding(root, state_path, repository)?;
    let directory = PrivateDir::open_existing(&binding.state_path)?;
    let bytes = directory.read(STATE, max_bytes.min(MAX_STATE_BYTES))?;
    let state: State = serde_json::from_slice(&bytes)?;
    state.validate(&binding)?;
    let encoded_bytes = bytes.len();
    let state_fingerprint = jj_lib::content_hash::blake2b_hash(bytes.as_slice()).into();
    let persisted_bytes = Arc::from(bytes.into_boxed_slice());
    let mut trees = vec![state.projection.tree.clone()];
    if let Some(receipt) = &state.unfinished {
        trees.push(receipt.before.tree.clone());
        trees.push(receipt.after.tree.clone());
    }
    if let Some(pending) = &state.pending {
        match &pending.action {
            Action::Snapshot => {}
            Action::CheckOut { tree } | Action::Reset { tree, .. } => trees.push(tree.clone()),
        }
        if let Some(receipt) = &pending.receipt {
            trees.push(receipt.after.tree.clone());
        }
    }
    if let Some(acknowledgement) = &state.acknowledgement {
        trees.push(acknowledgement.state.tree.clone());
        if let Some(receipt) = &acknowledgement.receipt {
            trees.push(receipt.before.tree.clone());
            trees.push(receipt.after.tree.clone());
        }
    }
    Ok(RetainedState {
        operation: OperationId::try_from_hex(&state.operation).unwrap(),
        workspace: state.workspace.into(),
        trees,
        pending: state.pending.is_some()
            || state.unfinished.is_some()
            || state.acknowledgement.is_some(),
        persisted_bytes,
        state_fingerprint,
        encoded_bytes,
    })
}

impl WorkingCopyFactory for NativeVfsWorkingCopyFactory {
    fn init_working_copy(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        state_path: PathBuf,
        operation: OperationId,
        workspace: WorkspaceNameBuf,
        _: &UserSettings,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        self.initialize_fresh(store, root, state_path, operation, workspace, None)
            .map_err(state_error)
    }
    fn load_working_copy(
        &self,
        store: Arc<Store>,
        root: PathBuf,
        state_path: PathBuf,
        _: &UserSettings,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let load = || -> Result<Box<dyn WorkingCopy>> {
            let (binding, directory, controller) = self.connect(&root, &state_path, &store)?;
            let state = State::read(&directory, &binding)?;
            // Recovery must remain reachable after an uncertain checkout.
            if state.pending.is_none() {
                ensure!(
                    controller.inspect()? == state.projection,
                    "native projection baseline changed outside jj"
                );
            }
            Ok(Box::new(NativeWorkingCopy::new(
                store.clone(),
                binding,
                directory,
                controller,
                state,
            )?))
        };
        load().map_err(state_error)
    }
}

struct NativeWorkingCopy {
    store: Arc<Store>,
    binding: ProjectionBinding,
    directory: Arc<PrivateDir>,
    controller: Arc<dyn NativeProjectionController>,
    state: State,
    operation: OperationId,
    workspace: WorkspaceNameBuf,
    tree: MergedTree,
    sparse: Vec<RepoPathBuf>,
}
impl NativeWorkingCopy {
    fn new(
        store: Arc<Store>,
        binding: ProjectionBinding,
        directory: Arc<PrivateDir>,
        controller: Arc<dyn NativeProjectionController>,
        state: State,
    ) -> Result<Self> {
        state.validate(&binding)?;
        let tree = state.projection.tree.merged(store.clone())?;
        Ok(Self {
            store,
            binding,
            directory,
            controller,
            operation: OperationId::try_from_hex(&state.operation).unwrap(),
            workspace: state.workspace.clone().into(),
            state,
            tree,
            sparse: vec![RepoPathBuf::root()],
        })
    }
}
#[async_trait(?Send)]
impl WorkingCopy for NativeWorkingCopy {
    fn name(&self) -> &str {
        NAME
    }
    fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace
    }
    fn operation_id(&self) -> &OperationId {
        &self.operation
    }
    fn tree(&self) -> std::result::Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }
    fn sparse_patterns(&self) -> std::result::Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse)
    }
    async fn start_mutation(
        &self,
    ) -> std::result::Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError> {
        let lock = self.directory.open_lock(LOCK).map_err(state_error)?;
        lock.lock_exclusive().map_err(state_error)?;
        let mut state = State::read(&self.directory, &self.binding).map_err(state_error)?;
        ensure_same_state(&state, &self.state).map_err(state_error)?;
        if let Some(ack) = &state.acknowledgement {
            self.controller
                .acknowledge(&ack.state, &self.operation, ack.receipt.as_ref())
                .await
                .map_err(state_error)?;
            state.acknowledgement = None;
            state
                .save(&self.directory, &self.binding)
                .map_err(state_error)?;
        }
        if state.pending.is_some() || state.unfinished.is_some() {
            return Err(state_error(anyhow::anyhow!(
                "native mutation requires explicit receipt/operation recovery; no working-copy reset was performed"
            )));
        }
        let mutation = self
            .controller
            .begin(&state.projection)
            .await
            .map_err(state_error)?;
        let wc = Self::new(
            self.store.clone(),
            self.binding.clone(),
            self.directory.clone(),
            self.controller.clone(),
            state,
        )
        .map_err(state_error)?;
        Ok(Box::new(LockedNativeWorkingCopy {
            old_operation: self.operation.clone(),
            old_tree: self.tree.clone(),
            wc,
            mutation,
            last_receipt: None,
            poisoned: false,
            _lock: lock,
        }))
    }
}
fn ensure_same_state(current: &State, loaded: &State) -> Result<()> {
    ensure!(
        current == loaded,
        "native working copy changed concurrently; reload before retrying"
    );
    Ok(())
}

struct LockedNativeWorkingCopy {
    old_operation: OperationId,
    old_tree: MergedTree,
    wc: NativeWorkingCopy,
    mutation: Box<dyn NativeProjectionMutation>,
    last_receipt: Option<ProjectionReceipt>,
    poisoned: bool,
    _lock: File,
}
impl LockedNativeWorkingCopy {
    fn prepare(&mut self, action: Action) -> Result<String> {
        ensure!(
            !self.poisoned && self.wc.state.pending.is_none(),
            "native mutation needs recovery"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let mut next = self.wc.state.clone();
        next.pending = Some(Pending {
            request_id: id.clone(),
            before: next.projection.clone(),
            action,
            receipt: None,
        });
        if let Err(error) = next.save(&self.wc.directory, &self.wc.binding) {
            self.poisoned = true;
            return Err(error);
        }
        self.wc.state = next;
        Ok(id)
    }
    fn reject(&mut self, failure: ProjectionFailure) -> anyhow::Error {
        match failure {
            ProjectionFailure::Rejected(error) => {
                let mut next = self.wc.state.clone();
                next.pending = None;
                if next.unfinished.is_none() {
                    // An ordinary failed command may never call finish(). Keep
                    // a durable no-op ACK so the next loaded mutation retires
                    // the host's sealed rejection before admitting more work.
                    // This synchronous error path cannot ACK through the held
                    // daemon session; require reload instead of reusing this
                    // LockedWorkingCopy with an outstanding acknowledgement.
                    next.acknowledgement = Some(Acknowledgement {
                        operation: next.operation.clone(),
                        state: next.projection.clone(),
                        receipt: None,
                    });
                    self.poisoned = true;
                }
                if let Err(save) = next.save(&self.wc.directory, &self.wc.binding) {
                    self.poisoned = true;
                    return save
                        .context("failed to persist nonapplication; native intent retained");
                }
                self.wc.state = next;
                error
            }
            ProjectionFailure::Ambiguous(error) => {
                self.poisoned = true;
                error.context("native mutation outcome uncertain; durable intent retained")
            }
        }
    }
    fn accept(&mut self, receipt: ProjectionReceipt) -> Result<()> {
        // Persist the authoritative receipt BEFORE replacing the pending intent.
        // A crash here is deliberately recoverable, not an implicit jj-op guess.
        let pending = self
            .wc
            .state
            .pending
            .as_ref()
            .context("native mutation has no durable intent")?;
        if let Err(error) = validate_receipt(pending, &receipt) {
            self.poisoned = true;
            return Err(error);
        }
        let mut next = self.wc.state.clone();
        next.pending.as_mut().unwrap().receipt = Some(receipt.clone());
        if let Err(error) = next.save(&self.wc.directory, &self.wc.binding) {
            self.poisoned = true;
            return Err(error);
        }
        self.wc.state = next;
        self.wc.tree = receipt.after.tree.merged(self.wc.store.clone())?;
        self.last_receipt = Some(receipt);
        Ok(())
    }
    fn before_next(&mut self) -> Result<()> {
        ensure!(!self.poisoned, "native mutation needs recovery");
        if let Some(pending) = &self.wc.state.pending {
            let receipt = pending
                .receipt
                .as_ref()
                .context("native mutation outcome is uncertain")?;
            // A later request's durable before state retains the last result;
            // the jj operation still only advances in finish().
            self.wc.state.projection = receipt.after.clone();
            self.wc.state.unfinished = Some(receipt.clone());
            self.wc.state.pending = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::{
        backend::{Backend, BackendInitError},
        config::{ConfigLayer, ConfigSource, StackedConfig},
        gitignore::GitIgnoreFile,
        matchers::{EverythingMatcher, NothingMatcher},
        repo::Repo,
        signing::Signer,
        workspace::Workspace,
    };
    use std::{collections::HashMap, sync::Mutex};

    #[derive(Clone, Copy, Default)]
    enum Failure {
        #[default]
        None,
        Reject,
        LoseAppliedReply,
        LoseUnappliedReply,
    }
    struct FakeState {
        mounted: bool,
        current: ProjectionState,
        next_tree: Option<NativeTree>,
        failure: Failure,
        outcomes: HashMap<String, ProjectionOutcome>,
        calls: usize,
        acknowledgements: usize,
        sessions: usize,
        lose_ack: bool,
    }
    struct FakeController(Arc<Mutex<FakeState>>);
    struct FakeConnector(Arc<FakeController>);
    impl ProjectionConnector for FakeConnector {
        fn connect(&self, _: &ProjectionBinding) -> Result<Arc<dyn NativeProjectionController>> {
            ensure!(
                self.0.0.lock().unwrap().mounted,
                "native provider is not mounted"
            );
            Ok(self.0.clone())
        }
    }
    struct FakeMutation(Arc<Mutex<FakeState>>);
    impl Drop for FakeMutation {
        fn drop(&mut self) {
            self.0.lock().unwrap().sessions -= 1;
        }
    }
    fn acknowledge(state: &mut FakeState, expected: &ProjectionState) -> Result<()> {
        ensure!(
            &state.current == expected,
            "acknowledgement baseline mismatch"
        );
        if std::mem::take(&mut state.lose_ack) {
            bail!("acknowledgement reply lost");
        }
        state.acknowledgements += 1;
        Ok(())
    }
    #[async_trait]
    impl NativeProjectionController for FakeController {
        fn inspect(&self) -> Result<ProjectionState> {
            let state = self.0.lock().unwrap();
            ensure!(state.mounted, "native provider is not mounted");
            Ok(state.current.clone())
        }
        async fn begin(
            &self,
            expected: &ProjectionState,
        ) -> Result<Box<dyn NativeProjectionMutation>> {
            let mut state = self.0.lock().unwrap();
            ensure!(
                state.mounted && &state.current == expected,
                "projection baseline changed"
            );
            state.sessions += 1;
            Ok(Box::new(FakeMutation(self.0.clone())))
        }
        async fn receipt(&self, request: &str) -> Result<ProjectionOutcome> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .outcomes
                .get(request)
                .cloned()
                .unwrap_or(ProjectionOutcome::Unknown))
        }
        async fn acknowledge(
            &self,
            state: &ProjectionState,
            _: &OperationId,
            _: Option<&ProjectionReceipt>,
        ) -> Result<()> {
            acknowledge(&mut self.0.lock().unwrap(), state)
        }
    }
    impl FakeMutation {
        fn apply(
            &mut self,
            id: &str,
            target: Option<&NativeTree>,
        ) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
            let mut state = self.0.lock().unwrap();
            state.calls += 1;
            let failure = std::mem::take(&mut state.failure);
            if matches!(failure, Failure::Reject) {
                state
                    .outcomes
                    .insert(id.into(), ProjectionOutcome::NotApplied);
                return Err(ProjectionFailure::Rejected(anyhow::anyhow!(
                    "unsupported native projection"
                )));
            }
            if matches!(failure, Failure::LoseUnappliedReply) {
                state.outcomes.insert(id.into(), ProjectionOutcome::Unknown);
                return Err(ProjectionFailure::Ambiguous(anyhow::anyhow!(
                    "connection interrupted"
                )));
            }
            let before = state.current.clone();
            let next = target
                .cloned()
                .or_else(|| state.next_tree.take())
                .unwrap_or_else(|| before.tree.clone());
            let after = ProjectionState {
                generation: before.generation + 1,
                tree: next,
                ..before.clone()
            };
            let receipt = ProjectionReceipt {
                request_id: id.into(),
                before,
                after: after.clone(),
            };
            state.current = after;
            state
                .outcomes
                .insert(id.into(), ProjectionOutcome::Applied(receipt.clone()));
            if matches!(failure, Failure::LoseAppliedReply) {
                return Err(ProjectionFailure::Ambiguous(anyhow::anyhow!(
                    "applied reply lost"
                )));
            }
            Ok(receipt)
        }
    }
    #[async_trait]
    impl NativeProjectionMutation for FakeMutation {
        async fn snapshot(
            &mut self,
            id: &str,
            _: &SnapshotOptions<'_>,
        ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
            self.apply(id, None)
                .map(|receipt| (receipt, SnapshotStats::default()))
        }
        async fn check_out(
            &mut self,
            id: &str,
            tree: &NativeTree,
        ) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure> {
            self.apply(id, Some(tree))
                .map(|receipt| (receipt, CheckoutStats::default()))
        }
        async fn reset(
            &mut self,
            id: &str,
            tree: &NativeTree,
            _: bool,
        ) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
            self.apply(id, Some(tree))
        }
        async fn finish(
            &mut self,
            state: &ProjectionState,
            _: &OperationId,
            _: Option<&ProjectionReceipt>,
        ) -> Result<()> {
            acknowledge(&mut self.0.lock().unwrap(), state)
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        root: PathBuf,
        state_path: PathBuf,
        settings: UserSettings,
        store: Arc<Store>,
        operation: Operation,
        controller: Arc<FakeController>,
        factory: NativeVfsWorkingCopyFactory,
    }
    impl Fixture {
        async fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("projection");
            std::fs::create_dir(&root).unwrap();
            let state_path = directory.path().join("native-state");
            let _private = PrivateDir::create(&state_path).unwrap();
            let metadata_fixture = directory.path().join("operation-fixture");
            std::fs::create_dir(&metadata_fixture).unwrap();
            let mut config = StackedConfig::with_defaults();
            config.add_layer(ConfigLayer::parse(ConfigSource::User,
                "[user]\nname='Native Fixture'\nemail='fixture@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n").unwrap());
            let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
            // This is a lower-level operation-store fixture, not a mounted
            // workflow or a supported materialized Tinytree user mode.
            let initializer =
                |_: &UserSettings,
                 path: &Path|
                 -> std::result::Result<Box<dyn Backend>, BackendInitError> {
                    Ok(Box::new(jj_native_backend::NativeBackend::init(
                        path,
                        jj_native_backend::Config::default(),
                    )?))
                };
            let (_, repo) = Workspace::init_with_backend(
                &settings,
                &metadata_fixture,
                &initializer,
                Signer::new(None, vec![]),
            )
            .await
            .unwrap();
            let current = ProjectionState {
                repository: "fixture".into(),
                generation: 1,
                tree: NativeTree {
                    terms: vec![repo.store().empty_tree_id().as_bytes().try_into().unwrap()],
                    labels: vec![],
                },
            };
            let controller = Arc::new(FakeController(Arc::new(Mutex::new(FakeState {
                mounted: true,
                current,
                next_tree: None,
                failure: Failure::None,
                outcomes: HashMap::new(),
                calls: 0,
                acknowledgements: 0,
                sessions: 0,
                lose_ack: false,
            }))));
            let factory = NativeVfsWorkingCopyFactory::new(
                "fixture".into(),
                Arc::new(FakeConnector(controller.clone())),
            );
            Self {
                root,
                state_path,
                settings,
                store: repo.store().clone(),
                operation: repo.operation().clone(),
                controller,
                factory,
                _directory: directory,
            }
        }
        fn init(&self) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
            self.factory.init_working_copy(
                self.store.clone(),
                self.root.clone(),
                self.state_path.clone(),
                self.operation.id().clone(),
                WorkspaceName::DEFAULT.to_owned(),
                &self.settings,
            )
        }
        fn load(&self) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
            self.factory.load_working_copy(
                self.store.clone(),
                self.root.clone(),
                self.state_path.clone(),
                &self.settings,
            )
        }
        fn persisted(&self) -> Vec<u8> {
            std::fs::read(self.state_path.join(STATE)).unwrap()
        }
    }
    fn options() -> SnapshotOptions<'static> {
        SnapshotOptions {
            base_ignores: GitIgnoreFile::empty(),
            progress: None,
            start_tracking_matcher: &EverythingMatcher,
            force_tracking_matcher: &NothingMatcher,
            max_new_file_size: 1024 * 1024,
        }
    }

    #[test]
    fn missing_mount_never_creates_state_or_selects_a_materialized_working_copy() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            fixture.controller.0.lock().unwrap().mounted = false;
            assert!(fixture.init().is_err());
            let tree = fixture.controller.0.lock().unwrap().current.tree.clone();
            assert!(
                fixture
                    .factory
                    .restore_fresh_working_copy(
                        fixture.store.clone(),
                        fixture.root.clone(),
                        fixture.state_path.clone(),
                        fixture.operation.id().clone(),
                        WorkspaceName::DEFAULT.to_owned(),
                        &tree,
                    )
                    .is_err()
            );
            assert!(!fixture.state_path.join(STATE).exists());
            assert!(!fixture.state_path.join(LOCK).exists());
            assert!(!fixture.root.join(".jj").exists());
            assert!(std::fs::read_dir(&fixture.root).unwrap().next().is_none());
        });
    }

    #[test]
    fn fresh_restore_preserves_operation_workspace_and_exact_tree_without_mutation() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let tree = NativeTree {
                terms: vec![[21; 32], [22; 32], [23; 32]],
                labels: vec!["our side".into(), "base".into(), "their side".into()],
            };
            fixture.controller.0.lock().unwrap().current = ProjectionState {
                repository: "fixture".into(),
                generation: 73,
                tree: tree.clone(),
            };
            let operation = fixture.operation.id().clone();
            let workspace: WorkspaceNameBuf = "restored-agent".into();
            let wrong = NativeTree {
                terms: vec![[24; 32]],
                labels: vec![],
            };
            assert!(
                fixture
                    .factory
                    .restore_fresh_working_copy(
                        fixture.store.clone(),
                        fixture.root.clone(),
                        fixture.state_path.clone(),
                        operation.clone(),
                        workspace.clone(),
                        &wrong
                    )
                    .is_err()
            );
            assert!(!fixture.state_path.join(STATE).exists());
            let sentinel = fixture.state_path.join("foreign-state");
            std::fs::write(&sentinel, b"preserve").unwrap();
            assert!(
                fixture
                    .factory
                    .restore_fresh_working_copy(
                        fixture.store.clone(),
                        fixture.root.clone(),
                        fixture.state_path.clone(),
                        operation.clone(),
                        workspace.clone(),
                        &tree
                    )
                    .is_err()
            );
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"preserve");
            assert!(!fixture.state_path.join(STATE).exists());
            std::fs::remove_file(&sentinel).unwrap(); // Explicit fixture setup, not API recovery.
            let restored = fixture
                .factory
                .restore_fresh_working_copy(
                    fixture.store.clone(),
                    fixture.root.clone(),
                    fixture.state_path.clone(),
                    operation.clone(),
                    workspace.clone(),
                    &tree,
                )
                .unwrap();
            assert_eq!(restored.operation_id(), &operation);
            assert_eq!(restored.workspace_name().as_str(), workspace.as_str());
            assert_eq!(
                NativeTree::from_merged_tree(restored.tree().unwrap()).unwrap(),
                tree
            );
            let retained =
                read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(retained.operation, operation);
            assert_eq!(retained.workspace, workspace);
            assert!(!retained.pending);
            assert_eq!(retained.trees, vec![tree.clone()]);
            let bytes = fixture.persisted();
            let state: State = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(state.root, fixture.root.canonicalize().unwrap());
            assert_eq!(
                state.projection.generation, 73,
                "use the new destination epoch, not a source epoch"
            );
            assert!(
                fixture
                    .factory
                    .restore_fresh_working_copy(
                        fixture.store.clone(),
                        fixture.root.clone(),
                        fixture.state_path.clone(),
                        operation.clone(),
                        workspace.clone(),
                        &tree
                    )
                    .is_err()
            );
            assert_eq!(fixture.persisted(), bytes);
            let reopened = fixture.load().unwrap();
            assert_eq!(reopened.operation_id(), &operation);
            assert_eq!(reopened.workspace_name().as_str(), workspace.as_str());
            let controller = fixture.controller.0.lock().unwrap();
            assert_eq!(
                (
                    controller.calls,
                    controller.sessions,
                    controller.acknowledgements
                ),
                (0, 0, 0)
            );
            assert!(
                std::fs::read_dir(&fixture.root).unwrap().next().is_none(),
                "metadata initialization must not materialize user files"
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn exact_restored_heads_are_plural_durable_and_fresh_only() {
        use jj_lib::simple_op_heads_store::SimpleOpHeadsStore;
        let directory = tempfile::tempdir().unwrap();
        let heads = vec![
            OperationId::from_bytes(&[1; 64]),
            OperationId::from_bytes(&[2; 64]),
        ];
        for (name, values, cap) in [
            ("empty", vec![], 2),
            ("duplicate", vec![heads[0].clone(), heads[0].clone()], 2),
            ("unsorted", vec![heads[1].clone(), heads[0].clone()], 2),
            ("short-identity", vec![OperationId::from_hex("01")], 2),
            ("over-budget", heads.clone(), 1),
        ] {
            let path = directory.path().join(name);
            std::fs::create_dir(&path).unwrap();
            assert!(
                SimpleOpHeadsStore::init_cloud_restore(&path, &values, cap).is_err(),
                "{name}"
            );
            assert!(
                std::fs::read_dir(&path).unwrap().next().is_none(),
                "invalid input must precede marker creation: {name}"
            );
        }
        let path = directory.path().join("exact-heads");
        std::fs::create_dir(&path).unwrap();
        let restored = SimpleOpHeadsStore::init_cloud_restore(&path, &heads, 2).unwrap();
        assert_eq!(restored.get_op_heads_bounded(2).unwrap(), heads);
        assert_eq!(
            SimpleOpHeadsStore::load(&path)
                .get_op_heads_bounded(2)
                .unwrap(),
            heads
        );
        assert!(
            SimpleOpHeadsStore::init_cloud_restore(&path, &heads, 2).is_err(),
            "a completed directory is not fresh or automatically adopted"
        );
        assert_eq!(restored.get_op_heads_bounded(2).unwrap(), heads);
        assert_eq!(
            std::fs::read_dir(&path).unwrap().count(),
            1,
            "only marker metadata is installed, not an operation or checkout"
        );
        let foreign = directory.path().join("foreign");
        std::fs::create_dir(&foreign).unwrap();
        std::fs::write(foreign.join("sentinel"), b"preserve").unwrap();
        assert!(SimpleOpHeadsStore::init_cloud_restore(&foreign, &heads, 2).is_err());
        assert_eq!(
            std::fs::read(foreign.join("sentinel")).unwrap(),
            b"preserve"
        );
        assert!(!foreign.join("heads").exists());
        let target = directory.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = directory.path().join("linked");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(SimpleOpHeadsStore::init_cloud_restore(&link, &heads, 2).is_err());
        assert!(std::fs::read_dir(&target).unwrap().next().is_none());
    }

    #[test]
    fn retained_state_read_cap_precedes_decode_and_freshness_covers_all_persisted_bytes() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let _wc = fixture.init().unwrap();
            let original = fixture.persisted();
            assert!(
                read_retained_state_bounded(
                    &fixture.root,
                    &fixture.state_path,
                    "fixture",
                    original.len() - 1
                )
                .is_err()
            );
            let first = read_retained_state_bounded(
                &fixture.root,
                &fixture.state_path,
                "fixture",
                original.len(),
            )
            .unwrap();
            assert_eq!(first.persisted_bytes.as_ref(), original);
            assert_eq!(first.encoded_bytes, original.len());
            let mut changed = original.clone();
            changed.push(b' ');
            PrivateDir::open_existing(&fixture.state_path)
                .unwrap()
                .replace(STATE, &changed)
                .unwrap();
            let second =
                read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(first.operation, second.operation);
            assert_eq!(first.trees, second.trees);
            assert_ne!(first.state_fingerprint, second.state_fingerprint);
            assert_ne!(first.persisted_bytes, second.persisted_bytes);
            let oversized_malformed = vec![b'!'; 129];
            PrivateDir::open_existing(&fixture.state_path)
                .unwrap()
                .replace(STATE, &oversized_malformed)
                .unwrap();
            let error =
                read_retained_state_bounded(&fixture.root, &fixture.state_path, "fixture", 128)
                    .unwrap_err();
            assert!(
                error.downcast_ref::<std::io::Error>().is_some(),
                "oversized input must fail in the bounded file read, before JSON parsing"
            );
        });
    }

    #[test]
    fn native_snapshot_preserves_all_merge_terms_and_labels_across_reload() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            assert_eq!(wc.name(), NAME);
            assert!(
                wc.downcast_ref::<jj_lib::local_working_copy::LocalWorkingCopy>()
                    .is_none()
            );
            let merged = NativeTree {
                terms: vec![[1; 32], [2; 32], [3; 32]],
                labels: vec!["left".into(), "base".into(), "right".into()],
            };
            fixture.controller.0.lock().unwrap().next_tree = Some(merged.clone());
            let mut locked = wc.start_mutation().await.unwrap();
            let (tree, _) = locked.snapshot(&options()).await.unwrap();
            assert_eq!(NativeTree::from_merged_tree(&tree).unwrap(), merged);
            let wc = locked.finish(fixture.operation.id().clone()).await.unwrap();
            assert_eq!(
                NativeTree::from_merged_tree(wc.tree().unwrap()).unwrap(),
                merged
            );
            drop(wc);
            let loaded = fixture.load().unwrap();
            assert_eq!(
                NativeTree::from_merged_tree(loaded.tree().unwrap()).unwrap(),
                merged
            );
            let roots = read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(roots.trees, vec![merged]);
            assert!(!roots.pending);
            let state = fixture.controller.0.lock().unwrap();
            assert_eq!(state.calls, 1);
            assert_eq!(state.acknowledgements, 1);
            assert_eq!(state.sessions, 0);
            // The controller API was used; no files or jj metadata were written
            // through the projection by this WorkingCopy implementation.
            assert!(std::fs::read_dir(&fixture.root).unwrap().next().is_none());
        });
    }

    #[test]
    fn authoritative_rejection_preserves_data_and_replays_a_durable_noop_ack_before_retry() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            let before = fixture.persisted();
            fixture.controller.0.lock().unwrap().failure = Failure::Reject;
            let mut locked = wc.start_mutation().await.unwrap();
            assert!(locked.snapshot(&options()).await.is_err());
            let state = read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(state.operation, *fixture.operation.id());
            assert!(
                state.pending,
                "ordinary rejection must retain its no-op ACK intent"
            );
            assert_eq!(fixture.controller.0.lock().unwrap().calls, 1);
            assert!(
                locked.snapshot(&options()).await.is_err(),
                "same locked object must reload before retrying with ACK pending"
            );
            assert_eq!(fixture.controller.0.lock().unwrap().calls, 1);
            assert!(locked.finish(fixture.operation.id().clone()).await.is_err());
            fixture.controller.0.lock().unwrap().lose_ack = true;
            assert!(fixture.load().unwrap().start_mutation().await.is_err());
            assert!(
                read_retained_state(&fixture.root, &fixture.state_path, "fixture")
                    .unwrap()
                    .pending
            );
            assert_eq!(fixture.controller.0.lock().unwrap().acknowledgements, 0);
            let wc = fixture.load().unwrap();
            let mut retried = wc.start_mutation().await.unwrap();
            assert_eq!(fixture.controller.0.lock().unwrap().acknowledgements, 1);
            assert_eq!(
                fixture.persisted(),
                before,
                "ACK replay restores the exact unchanged working-copy state"
            );
            fixture.controller.0.lock().unwrap().failure = Failure::Reject;
            assert!(retried.snapshot(&options()).await.is_err());
            drop(retried);
            assert!(fixture.load().unwrap().start_mutation().await.is_ok());
            assert_eq!(
                fixture.controller.0.lock().unwrap().acknowledgements,
                2,
                "each new sealed rejection must be acknowledged, even for the same operation/root"
            );
        });
    }

    #[test]
    fn lost_applied_reply_requires_exact_operation_recovery_without_resetting_files() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            fixture.controller.0.lock().unwrap().failure = Failure::LoseAppliedReply;
            let mut locked = wc.start_mutation().await.unwrap();
            assert!(locked.snapshot(&options()).await.is_err());
            assert!(locked.finish(fixture.operation.id().clone()).await.is_err());
            assert!(fixture.load().unwrap().start_mutation().await.is_err());
            let roots = read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert!(roots.pending);
            let receipt = match fixture
                .factory
                .recover_pending(
                    fixture.store.clone(),
                    &fixture.root,
                    &fixture.state_path,
                    None,
                    None,
                )
                .await
                .unwrap()
            {
                RecoveryOutcome::OperationRequired(receipt) => receipt,
                _ => panic!("an applied mutation requires a verified jj operation"),
            };
            assert!(matches!(
                fixture
                    .factory
                    .recover_pending(
                        fixture.store.clone(),
                        &fixture.root,
                        &fixture.state_path,
                        Some(&fixture.operation),
                        Some(&receipt)
                    )
                    .await
                    .unwrap(),
                RecoveryOutcome::Recovered
            ));
            assert!(
                !read_retained_state(&fixture.root, &fixture.state_path, "fixture")
                    .unwrap()
                    .pending
            );
            assert!(fixture.load().unwrap().start_mutation().await.is_ok());
            assert_eq!(fixture.controller.0.lock().unwrap().calls, 1);
            assert!(std::fs::read_dir(&fixture.root).unwrap().next().is_none());
        });
    }

    #[test]
    fn later_rejection_does_not_erase_an_unfinished_successful_snapshot() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            let mut locked = wc.start_mutation().await.unwrap();
            locked.snapshot(&options()).await.unwrap();
            fixture.controller.0.lock().unwrap().failure = Failure::Reject;
            assert!(locked.snapshot(&options()).await.is_err());
            drop(locked);
            assert!(fixture.load().unwrap().start_mutation().await.is_err());
            assert!(
                read_retained_state(&fixture.root, &fixture.state_path, "fixture")
                    .unwrap()
                    .pending
            );
            let outcome = fixture
                .factory
                .recover_pending(
                    fixture.store.clone(),
                    &fixture.root,
                    &fixture.state_path,
                    Some(&fixture.operation),
                    None,
                )
                .await
                .unwrap();
            assert!(matches!(outcome, RecoveryOutcome::Recovered));
        });
    }

    #[test]
    fn unknown_outcome_is_not_nonapplication_and_lost_finish_ack_is_replayable() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            fixture.controller.0.lock().unwrap().failure = Failure::LoseUnappliedReply;
            let mut locked = wc.start_mutation().await.unwrap();
            assert!(locked.snapshot(&options()).await.is_err());
            drop(locked);
            let pending_bytes = fixture.persisted();
            assert!(
                fixture
                    .factory
                    .recover_pending(
                        fixture.store.clone(),
                        &fixture.root,
                        &fixture.state_path,
                        None,
                        None
                    )
                    .await
                    .is_err()
            );
            assert_eq!(fixture.persisted(), pending_bytes);
            let request = fixture
                .controller
                .0
                .lock()
                .unwrap()
                .outcomes
                .keys()
                .next()
                .unwrap()
                .clone();
            fixture
                .controller
                .0
                .lock()
                .unwrap()
                .outcomes
                .insert(request, ProjectionOutcome::NotApplied);
            assert!(matches!(
                fixture
                    .factory
                    .recover_pending(
                        fixture.store.clone(),
                        &fixture.root,
                        &fixture.state_path,
                        None,
                        None
                    )
                    .await
                    .unwrap(),
                RecoveryOutcome::NotApplied
            ));
            let wc = fixture.load().unwrap();
            let mut locked = wc.start_mutation().await.unwrap();
            locked.snapshot(&options()).await.unwrap();
            fixture.controller.0.lock().unwrap().lose_ack = true;
            assert!(locked.finish(fixture.operation.id().clone()).await.is_err());
            assert!(
                read_retained_state(&fixture.root, &fixture.state_path, "fixture")
                    .unwrap()
                    .pending
            );
            assert!(fixture.load().unwrap().start_mutation().await.is_ok());
            assert!(
                !read_retained_state(&fixture.root, &fixture.state_path, "fixture")
                    .unwrap()
                    .pending
            );
        });
    }

    #[test]
    fn sealed_nonapplication_recovery_ack_is_durable_and_replayable_without_a_new_operation() {
        futures::executor::block_on(async {
            let fixture = Fixture::new().await;
            let wc = fixture.init().unwrap();
            let original_operation = wc.operation_id().clone();
            fixture.controller.0.lock().unwrap().failure = Failure::LoseUnappliedReply;
            let mut locked = wc.start_mutation().await.unwrap();
            assert!(locked.snapshot(&options()).await.is_err());
            drop(locked);
            {
                let mut host = fixture.controller.0.lock().unwrap();
                let id = host.outcomes.keys().next().unwrap().clone();
                host.outcomes.insert(id, ProjectionOutcome::NotApplied);
                host.lose_ack = true;
            }
            assert!(
                fixture
                    .factory
                    .recover_pending(
                        fixture.store.clone(),
                        &fixture.root,
                        &fixture.state_path,
                        None,
                        None
                    )
                    .await
                    .is_err()
            );
            let state = read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(state.operation, original_operation);
            assert!(
                state.pending,
                "lost no-op ACK must leave durable acknowledgement intent"
            );
            assert_eq!(fixture.controller.0.lock().unwrap().acknowledgements, 0);
            assert!(matches!(
                fixture
                    .factory
                    .recover_pending(
                        fixture.store.clone(),
                        &fixture.root,
                        &fixture.state_path,
                        None,
                        None
                    )
                    .await
                    .unwrap(),
                RecoveryOutcome::NoPending
            ));
            assert_eq!(fixture.controller.0.lock().unwrap().acknowledgements, 1);
            let state = read_retained_state(&fixture.root, &fixture.state_path, "fixture").unwrap();
            assert_eq!(state.operation, original_operation);
            assert!(!state.pending);
            assert!(fixture.load().unwrap().start_mutation().await.is_ok());
        });
    }
}

#[async_trait]
impl LockedWorkingCopy for LockedNativeWorkingCopy {
    fn old_operation_id(&self) -> &OperationId {
        &self.old_operation
    }
    fn old_tree(&self) -> &MergedTree {
        &self.old_tree
    }
    async fn snapshot(
        &mut self,
        options: &SnapshotOptions,
    ) -> std::result::Result<(MergedTree, SnapshotStats), SnapshotError> {
        self.before_next().map_err(snapshot_error)?;
        let id = self.prepare(Action::Snapshot).map_err(snapshot_error)?;
        match self.mutation.snapshot(&id, options).await {
            Ok((receipt, stats)) => {
                self.accept(receipt).map_err(snapshot_error)?;
                Ok((self.wc.tree.clone(), stats))
            }
            Err(error) => Err(snapshot_error(self.reject(error))),
        }
    }
    async fn check_out(
        &mut self,
        commit: &Commit,
    ) -> std::result::Result<CheckoutStats, CheckoutError> {
        self.before_next().map_err(checkout_error)?;
        let tree = NativeTree::from_merged_tree(&commit.tree()).map_err(checkout_error)?;
        if tree == self.wc.state.projection.tree {
            return Ok(CheckoutStats::default());
        }
        let id = self
            .prepare(Action::CheckOut { tree: tree.clone() })
            .map_err(checkout_error)?;
        match self.mutation.check_out(&id, &tree).await {
            Ok((receipt, stats)) => {
                self.accept(receipt).map_err(checkout_error)?;
                Ok(stats)
            }
            Err(error) => Err(checkout_error(self.reject(error))),
        }
    }
    fn rename_workspace(&mut self, workspace: WorkspaceNameBuf) {
        self.wc.workspace = workspace;
    }
    async fn reset(&mut self, commit: &Commit) -> std::result::Result<(), ResetError> {
        self.reset_inner(commit, false).await.map_err(reset_error)
    }
    async fn recover(&mut self, commit: &Commit) -> std::result::Result<(), ResetError> {
        self.reset_inner(commit, true).await.map_err(reset_error)
    }
    fn sparse_patterns(&self) -> std::result::Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.wc.sparse)
    }
    async fn set_sparse_patterns(
        &mut self,
        patterns: Vec<RepoPathBuf>,
    ) -> std::result::Result<CheckoutStats, CheckoutError> {
        if patterns != self.wc.sparse {
            return Err(checkout_error(anyhow::anyhow!(
                "native VFS projects lazily; sparse materialization is unsupported"
            )));
        }
        Ok(CheckoutStats::default())
    }
    async fn finish(
        mut self: Box<Self>,
        operation: OperationId,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        self.before_next().map_err(state_error)?;
        let mut next = self.wc.state.clone();
        next.operation = operation.hex();
        next.workspace = self.wc.workspace.as_str().into();
        next.unfinished = None;
        next.acknowledgement = Some(Acknowledgement {
            operation: next.operation.clone(),
            state: next.projection.clone(),
            receipt: self.last_receipt.clone(),
        });
        next.save(&self.wc.directory, &self.wc.binding)
            .map_err(state_error)?;
        self.wc.state = next;
        self.mutation
            .finish(
                &self.wc.state.projection,
                &operation,
                self.last_receipt.as_ref(),
            )
            .await
            .map_err(state_error)?;
        self.wc.state.acknowledgement = None;
        self.wc
            .state
            .save(&self.wc.directory, &self.wc.binding)
            .map_err(state_error)?;
        self.wc.operation = operation;
        Ok(Box::new(self.wc))
    }
}
impl LockedNativeWorkingCopy {
    async fn reset_inner(&mut self, commit: &Commit, recover: bool) -> Result<()> {
        self.before_next()?;
        let tree = NativeTree::from_merged_tree(&commit.tree())?;
        let id = self.prepare(Action::Reset {
            tree: tree.clone(),
            recover,
        })?;
        match self.mutation.reset(&id, &tree, recover).await {
            Ok(receipt) => self.accept(receipt),
            Err(error) => Err(self.reject(error)),
        }
    }
}
