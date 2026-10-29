//! Read-only retained-root inventory. This is not a GC authorization: the host
//! still needs command fencing, durable workspace registration and a checkpoint.
use jj_lib::{
    backend::TreeId,
    conflict_labels::ConflictLabels,
    local_working_copy::LocalWorkingCopy,
    merge::Merge,
    merged_tree::MergedTree,
    object_id::ObjectId as _,
    op_store::{OpStore, OperationId},
    operation::Operation,
    ref_name::{WorkspaceName, WorkspaceNameBuf},
    repo::RepoLoader,
    repo_path::RepoPathBuf,
    simple_op_heads_store::SimpleOpHeadsStore,
    simple_op_store::SimpleOpStore,
    store::Store,
    working_copy::{LockedWorkingCopy, WorkingCopy, WorkingCopyStateError},
    workspace::Workspace,
};
use jj_vfs::native::{NativeTree, RetainedState};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tinytree_model::{Kind, ObjectId, ObjectKey};

#[derive(Clone, Debug)]
pub(crate) struct CollectorLimits {
    pub max_operations: usize,
    pub max_file_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_parent_edges: usize,
    pub max_object_roots: usize,
    /// Conservative ASCII JSON upper bound, also charged for repeated input
    /// references/edges so duplicate entries cannot evade traversal-work limits.
    pub max_encoded_bytes: usize,
    pub max_elapsed_ms: u64,
}
impl Default for CollectorLimits {
    fn default() -> Self {
        Self {
            max_operations: 10_000,
            max_file_bytes: 8 * 1024 * 1024,
            max_decode_bytes: 128 * 1024 * 1024,
            max_parent_edges: 100_000,
            max_object_roots: 100_000,
            max_encoded_bytes: 8 * 1024 * 1024,
            max_elapsed_ms: 30_000,
        }
    }
}
#[derive(Debug)]
pub(crate) struct CollectError(&'static str);
impl fmt::Display for CollectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "native root collection failed: {}", self.0)
    }
}
impl std::error::Error for CollectError {}
type Result<T> = std::result::Result<T, CollectError>;

pub(crate) struct RootCheckpoint {
    pub operation_heads: Vec<OperationId>,
    pub working_copy_operation: OperationId,
    pub retained_operations: Vec<OperationId>,
    pub intent_operations: Vec<OperationId>,
    pub object_roots: Vec<ObjectKey>,
    pub operations: usize,
    pub parent_edges: usize,
    pub root_references: usize,
    pub intrinsic_commit_references: usize,
    pub encoded_bytes_upper_bound: usize,
    pub elapsed_ms: u64,
}
#[derive(Serialize)]
pub(crate) struct RootAudit {
    pub version: u32,
    pub operation_heads: Vec<String>,
    pub working_copy_operation: String,
    pub retained_operations: Vec<String>,
    pub intent_operations: Vec<String>,
    pub object_roots: Vec<AuditObject>,
    pub operations: usize,
    pub parent_edges: usize,
    pub root_references: usize,
    pub intrinsic_commit_references: usize,
    pub encoded_bytes_upper_bound: usize,
    pub elapsed_ms: u64,
}
#[derive(Serialize)]
pub(crate) struct AuditObject {
    pub kind: &'static str,
    pub id: String,
}
impl RootCheckpoint {
    pub fn audit(&self) -> RootAudit {
        RootAudit {
            version: 1,
            operation_heads: self.operation_heads.iter().map(|id| id.hex()).collect(),
            working_copy_operation: self.working_copy_operation.hex(),
            retained_operations: self.retained_operations.iter().map(|id| id.hex()).collect(),
            intent_operations: self.intent_operations.iter().map(|id| id.hex()).collect(),
            object_roots: self
                .object_roots
                .iter()
                .map(|object| AuditObject {
                    kind: match object.kind {
                        Kind::File => "file",
                        Kind::Tree => "tree",
                        Kind::Commit => "commit",
                        Kind::Symlink => "symlink",
                        Kind::CopyHistory => "copy_history",
                    },
                    id: object
                        .id
                        .0
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                })
                .collect(),
            operations: self.operations,
            parent_edges: self.parent_edges,
            root_references: self.root_references,
            intrinsic_commit_references: self.intrinsic_commit_references,
            encoded_bytes_upper_bound: self.encoded_bytes_upper_bound,
            elapsed_ms: self.elapsed_ms,
        }
    }
}
struct Budget<'a> {
    limits: &'a CollectorLimits,
    started: Instant,
    operations: usize,
    edges: usize,
    references: usize,
    intrinsic: usize,
    encoded: usize,
}
impl Budget<'_> {
    fn check_time(&self) -> Result<()> {
        if self.started.elapsed() >= Duration::from_millis(self.limits.max_elapsed_ms) {
            return Err(CollectError("elapsed budget exceeded"));
        }
        Ok(())
    }
    fn bytes(&mut self, bytes: usize) -> Result<()> {
        self.check_time()?;
        self.encoded = self
            .encoded
            .checked_add(bytes)
            .ok_or(CollectError("encoded byte budget overflow"))?;
        if self.encoded > self.limits.max_encoded_bytes {
            return Err(CollectError("encoded byte budget exceeded"));
        }
        Ok(())
    }
    fn operation(&mut self, id: &OperationId) -> Result<()> {
        self.operations = self
            .operations
            .checked_add(1)
            .ok_or(CollectError("operation budget overflow"))?;
        if self.operations > self.limits.max_operations {
            return Err(CollectError("operation budget exceeded"));
        }
        self.opaque_id(id)
    }
    fn opaque_id(&mut self, id: &OperationId) -> Result<()> {
        let bytes = id.as_bytes();
        if bytes.is_empty() || bytes.len() > 128 {
            return Err(CollectError("unsupported operation ID length"));
        }
        self.bytes(bytes.len() * 2 + 8)
    }
    fn parents(&mut self, parents: &[OperationId]) -> Result<()> {
        self.edges = self
            .edges
            .checked_add(parents.len())
            .ok_or(CollectError("parent edge budget overflow"))?;
        if self.edges > self.limits.max_parent_edges {
            return Err(CollectError("parent edge budget exceeded"));
        }
        for parent in parents {
            self.opaque_id(parent)?;
        }
        Ok(())
    }
    fn root(
        &mut self,
        roots: &mut BTreeMap<(u8, [u8; 32]), ObjectKey>,
        kind: Kind,
        bytes: &[u8],
    ) -> Result<()> {
        self.references = self
            .references
            .checked_add(1)
            .ok_or(CollectError("root reference budget overflow"))?;
        self.bytes(112)?;
        let id: [u8; 32] = bytes
            .try_into()
            .map_err(|_| CollectError("non-native commit or tree ID"))?;
        if kind == Kind::Commit && id == [0; 32] {
            // The native zero commit is intrinsic, never a stored object.
            self.intrinsic += 1;
            return Ok(());
        }
        let key = (kind as u8, id);
        if !roots.contains_key(&key) {
            if roots.len() >= self.limits.max_object_roots {
                return Err(CollectError("object root budget exceeded"));
            }
            roots.insert(
                key,
                ObjectKey {
                    kind,
                    id: ObjectId(id),
                },
            );
        }
        Ok(())
    }
}
fn decode_hex(text: &str, maximum_bytes: usize) -> Result<Vec<u8>> {
    if text.is_empty()
        || text.len() % 2 != 0
        || text.len() / 2 > maximum_bytes
        || !text.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(CollectError("invalid saved retention identity"));
    }
    (0..text.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&text[offset..offset + 2], 16)
                .map_err(|_| CollectError("invalid saved retention identity"))
        })
        .collect()
}
fn sorted_ids(mut ids: Vec<OperationId>) -> Vec<OperationId> {
    ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    ids.dedup();
    ids
}
async fn read_operation(
    store: &std::sync::Arc<dyn OpStore>,
    id: &OperationId,
    budget: &mut Budget<'_>,
) -> Result<Operation> {
    budget.operation(id)?;
    let simple = store
        .downcast_ref::<SimpleOpStore>()
        .ok_or(CollectError("unsupported operation store"))?;
    let data = simple
        .read_operation_bounded(
            id,
            budget.limits.max_file_bytes,
            budget.limits.max_decode_bytes,
        )
        .map_err(|_| CollectError("required operation is unavailable or exceeded read budget"))?;
    budget.check_time()?;
    if id != store.root_operation_id()
        && jj_lib::content_hash::blake2b_hash(&data).as_slice() != id.as_bytes()
    {
        return Err(CollectError(
            "retained operation identity does not match its content",
        ));
    }
    let operation = Operation::new(store.clone(), id.clone(), data);
    budget.parents(operation.parent_ids())?;
    Ok(operation)
}
async fn roots_from_operation(
    operation: &Operation,
    roots: &mut BTreeMap<(u8, [u8; 32]), ObjectKey>,
    budget: &mut Budget<'_>,
) -> Result<()> {
    for id in operation.all_referenced_commit_ids() {
        budget.root(roots, Kind::Commit, id.as_bytes())?;
    }
    let store = operation.op_store();
    let simple = store
        .downcast_ref::<SimpleOpStore>()
        .ok_or(CollectError("unsupported operation store"))?;
    let raw = simple
        .read_view_bounded(
            operation.view_id(),
            budget.limits.max_file_bytes,
            budget.limits.max_decode_bytes,
        )
        .map_err(|_| {
            CollectError("required retained view is unavailable or exceeded read budget")
        })?;
    let view = jj_lib::view::View::new(raw, true);
    budget.check_time()?;
    // SimpleOpStore's all-zero intrinsic view is synthesized, not hashed/stored.
    if operation.view_id().as_bytes().iter().any(|byte| *byte != 0)
        && jj_lib::content_hash::blake2b_hash(view.store_view()).as_slice()
            != operation.view_id().as_bytes()
    {
        return Err(CollectError(
            "retained view identity does not match its content",
        ));
    }
    for id in view.all_referenced_commit_ids() {
        budget.root(roots, Kind::Commit, id.as_bytes())?;
    }
    Ok(())
}

// Native JSON strings/sequence capacity and the retained exact-byte input fit
// under this conservative per-file decode allowance. Charge a fixed reserve
// before deriving the input cap, rather than decoding first and checking later.
fn native_state_limit(limits: &CollectorLimits) -> Result<usize> {
    let decode = limits
        .max_decode_bytes
        .checked_sub(4096)
        .ok_or(CollectError(
            "native working-copy decode budget is too small",
        ))?
        / 32;
    let limit = limits.max_file_bytes.min(decode).min(4 * 1024 * 1024);
    if limit == 0 {
        return Err(CollectError("native working-copy read budget is too small"));
    }
    Ok(limit)
}

#[derive(Clone)]
struct NativeLocation {
    state_path: PathBuf,
    repository: String,
    registry: Option<Arc<native_workspace_admin::Registry>>,
    registration: Option<native_workspace_admin::Binding>,
}
impl NativeLocation {
    fn read(&self, root: &Path, limits: &CollectorLimits) -> Result<RetainedState> {
        if let (Some(registry), Some(registration)) = (&self.registry, &self.registration) {
            if registry
                .lookup(root)
                .map_err(|_| CollectError("native registry binding became unavailable"))?
                != *registration
            {
                return Err(CollectError(
                    "native registry binding changed during collection",
                ));
            }
        }
        jj_vfs::native::read_retained_state_bounded(
            root,
            &self.state_path,
            &self.repository,
            native_state_limit(limits)?,
        )
        .map_err(|_| {
            CollectError(
                "persisted native working-copy state is unavailable or exceeded read budget",
            )
        })
    }
}

/// This private audit view never connects a projection controller or selects a
/// normal working copy. The first state decode is already caller-bounded.
struct ReadonlyNativeWorkingCopy {
    location: NativeLocation,
    retained: RetainedState,
    tree: MergedTree,
    sparse: Vec<RepoPathBuf>,
}
impl ReadonlyNativeWorkingCopy {
    fn new(store: Arc<Store>, location: NativeLocation, retained: RetainedState) -> Result<Self> {
        let current = retained
            .trees
            .first()
            .ok_or(CollectError("native working-copy tree is missing"))?;
        let trees: Vec<TreeId> = current
            .terms
            .iter()
            .map(|id| TreeId::new(id.to_vec()))
            .collect();
        let tree = MergedTree::new(
            store,
            Merge::from_vec(trees),
            ConflictLabels::from_vec(current.labels.clone()),
        );
        Ok(Self {
            location,
            retained,
            tree,
            sparse: vec![RepoPathBuf::root()],
        })
    }
}
impl WorkingCopy for ReadonlyNativeWorkingCopy {
    fn name(&self) -> &str {
        jj_vfs::native::NAME
    }
    fn workspace_name(&self) -> &WorkspaceName {
        &self.retained.workspace
    }
    fn operation_id(&self) -> &OperationId {
        &self.retained.operation
    }
    fn tree(&self) -> std::result::Result<&MergedTree, WorkingCopyStateError> {
        Ok(&self.tree)
    }
    fn sparse_patterns(&self) -> std::result::Result<&[RepoPathBuf], WorkingCopyStateError> {
        Ok(&self.sparse)
    }
    fn start_mutation<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<Box<dyn LockedWorkingCopy>, WorkingCopyStateError>,
                > + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async {
            Err(WorkingCopyStateError {
                message: "native root inventory is read-only".into(),
                err: std::io::Error::other("root inventory cannot mutate a working copy").into(),
            })
        })
    }
}

/// Build the production audit workspace before invoking any ordinary WorkingCopy
/// factory. Registry paths, transport scope and repository identity remain
/// trusted host registration data, never fields from the state being decoded.
pub(crate) fn load_native_workspace(
    command: &jj_cli::cli_util::CommandHelper,
    limits: &CollectorLimits,
) -> Result<Workspace> {
    let loader = command
        .workspace_loader()
        .map_err(|_| CollectError("native workspace selection is unavailable"))?;
    let registry = native_workspace_admin::Registry::open_existing(
        native_workspace_admin::Registry::default_path()
            .map_err(|_| CollectError("native registry configuration is unavailable"))?,
    )
    .map_err(|_| CollectError("native workspace registry is unavailable"))?;
    let registration = registry
        .lookup(loader.workspace_root())
        .map_err(|_| CollectError("native workspace is not registered or ready"))?;
    if registration.repo != loader.repo_path() {
        return Err(CollectError(
            "native repository selection differs from its registration",
        ));
    }
    #[cfg(not(feature = "protocol-fixture"))]
    crate::commands::ensure_binding(&registration)
        .map_err(|_| CollectError("native audit command fence is unavailable"))?;
    let location = NativeLocation {
        state_path: registration.working_copy.clone(),
        repository: registration.repository.clone(),
        registry: Some(registry),
        registration: Some(registration.clone()),
    };
    let retained = location.read(&registration.root, limits)?;
    let mut stores = jj_lib::default_backend_factories::default_backend_factories();
    crate::broker::register(&mut stores);
    let repo = RepoLoader::init_from_file_system(command.settings(), &registration.repo, &stores)
        .map_err(|_| CollectError("native audit repository is unavailable"))?;
    if repo.store().backend().name() != jj_native_backend::NativeBackend::NAME {
        return Err(CollectError("unsupported native audit object backend"));
    }
    let wc = ReadonlyNativeWorkingCopy::new(repo.store().clone(), location, retained)?;
    Workspace::new(&registration.root, registration.repo, Box::new(wc), repo)
        .map_err(|_| CollectError("native audit workspace cannot be constructed"))
}

struct WorkingCopyInventory {
    state_path: PathBuf,
    operation: OperationId,
    workspace: WorkspaceNameBuf,
    trees: Vec<NativeTree>,
    persisted_bytes: Option<Arc<[u8]>>,
    encoded_bytes: usize,
}
fn native_inventory(retained: RetainedState, state_path: PathBuf) -> WorkingCopyInventory {
    WorkingCopyInventory {
        state_path,
        operation: retained.operation,
        workspace: retained.workspace,
        trees: retained.trees,
        persisted_bytes: Some(retained.persisted_bytes),
        encoded_bytes: retained.encoded_bytes,
    }
}
fn fresh_working_copy(
    workspace: &Workspace,
    limits: &CollectorLimits,
) -> Result<WorkingCopyInventory> {
    if let Some(audit) = workspace
        .working_copy()
        .downcast_ref::<ReadonlyNativeWorkingCopy>()
    {
        return Ok(native_inventory(
            audit.location.read(workspace.workspace_root(), limits)?,
            audit.location.state_path.clone(),
        ));
    }
    if workspace.working_copy().name() == jj_vfs::native::NAME {
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path()
                .map_err(|_| CollectError("native registry configuration is unavailable"))?,
        )
        .map_err(|_| CollectError("native workspace registry is unavailable"))?;
        let registration = registry
            .lookup(workspace.workspace_root())
            .map_err(|_| CollectError("native workspace is not registered or ready"))?;
        if registration.repo != workspace.repo_path() {
            return Err(CollectError(
                "native registered repository differs from the workspace",
            ));
        }
        let location = NativeLocation {
            state_path: registration.working_copy.clone(),
            repository: registration.repository.clone(),
            registry: Some(registry),
            registration: Some(registration),
        };
        return Ok(native_inventory(
            location.read(workspace.workspace_root(), limits)?,
            location.state_path.clone(),
        ));
    }
    // Declared protocol fixtures retain their existing bounded local-WC path;
    // the production loader above never falls back to this implementation.
    let local = workspace
        .working_copy()
        .downcast_ref::<LocalWorkingCopy>()
        .ok_or(CollectError("unsupported working-copy implementation"))?
        .reload_readonly(limits.max_file_bytes, limits.max_decode_bytes)
        .map_err(|_| CollectError("persisted working-copy state is unavailable"))?;
    let tree = local
        .tree()
        .map_err(|_| CollectError("actual working-copy tree is unavailable"))?;
    let terms = tree
        .tree_ids()
        .iter()
        .map(|id| {
            id.as_bytes()
                .try_into()
                .map_err(|_| CollectError("non-native working-copy tree identity"))
        })
        .collect::<Result<_>>()?;
    Ok(WorkingCopyInventory {
        state_path: workspace.workspace_root().join(".jj/working_copy"),
        operation: local.operation_id().clone(),
        workspace: local.workspace_name().to_owned(),
        trees: vec![NativeTree {
            terms,
            labels: tree.labels().as_slice().to_vec(),
        }],
        persisted_bytes: None,
        encoded_bytes: 0,
    })
}

/// Inventory every retained operation/view plus the actual working-copy state.
/// Reads are checked before/after each API boundary; filesystem I/O itself is
/// not forcibly cancelled. No snapshot, mutation, deletion or Git API is used.
pub(crate) async fn collect(
    workspace: &Workspace,
    limits: &CollectorLimits,
) -> Result<RootCheckpoint> {
    collect_with_hook(workspace, limits, || std::future::ready(())).await
}

async fn collect_with_hook<F, Fut>(
    workspace: &Workspace,
    limits: &CollectorLimits,
    before_final_check: F,
) -> Result<RootCheckpoint>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if limits.max_operations == 0
        || limits.max_file_bytes == 0
        || limits.max_decode_bytes == 0
        || limits.max_parent_edges == 0
        || limits.max_object_roots == 0
        || limits.max_encoded_bytes == 0
        || !(1..=86_400_000).contains(&limits.max_elapsed_ms)
    {
        return Err(CollectError("invalid collection limits"));
    }
    if workspace.repo_loader().store().backend().name() != jj_native_backend::NativeBackend::NAME {
        return Err(CollectError("unsupported object backend"));
    }
    let mut budget = Budget {
        limits,
        started: Instant::now(),
        operations: 0,
        edges: 0,
        references: 0,
        intrinsic: 0,
        encoded: 0,
    };
    budget.bytes(4096)?;
    let store = workspace.repo_loader().op_store().clone();
    let simple = store
        .downcast_ref::<SimpleOpStore>()
        .ok_or(CollectError("unsupported operation store"))?;
    let retained = simple
        .retained_operation_ids(limits.max_operations)
        .map_err(|_| {
            CollectError("retained operation enumeration failed or exceeded its budget")
        })?;
    budget.check_time()?;
    if retained.len() > limits.max_operations {
        return Err(CollectError("retained operation budget exceeded"));
    }
    let head_store = workspace
        .repo_loader()
        .op_heads_store()
        .downcast_ref::<SimpleOpHeadsStore>()
        .ok_or(CollectError("unsupported operation head store"))?;
    let heads = head_store
        .get_op_heads_bounded(limits.max_operations)
        .map_err(|_| CollectError("operation heads are unavailable or exceeded budget"))?;
    budget.check_time()?;
    if heads.is_empty() || heads.len() > limits.max_operations {
        return Err(CollectError(
            "operation head budget exceeded or no head exists",
        ));
    }
    let operation_heads = sorted_ids(heads);
    for id in &operation_heads {
        budget.opaque_id(id)?;
    }
    let working_copy = fresh_working_copy(workspace, limits)?;
    let working_copy_operation = working_copy.operation.clone();
    let working_copy_name = working_copy.workspace.clone();
    let working_copy_trees = working_copy.trees.clone();
    let working_copy_bytes = working_copy.persisted_bytes;
    budget.bytes(working_copy.encoded_bytes)?;
    budget.opaque_id(&working_copy_operation)?;
    let sidecar =
        crate::remote_state::retained_roots(&working_copy.state_path, workspace.repo_path())
            .map_err(|_| {
                CollectError("native sidecar retention state is unavailable or invalid")
            })?;
    let mut intent_operations = Vec::new();
    let mut sidecar_heads = Vec::new();
    if let Some(sidecar) = &sidecar {
        budget.bytes(sidecar.encoded_bytes())?;
        for head in &sidecar.commit_heads {
            sidecar_heads.push(decode_hex(head, 32)?);
        }
        for id in &sidecar.active_operations {
            let id = OperationId::from_bytes(&decode_hex(id, 128)?);
            budget.opaque_id(&id)?;
            intent_operations.push(id);
        }
    }
    let forge_sidecar = crate::forge_source::retained_roots(&working_copy.state_path)
        .map_err(|_| CollectError("forge source retention state is unavailable or invalid"))?;
    if let Some(saved) = &forge_sidecar {
        budget.bytes(saved.encoded_bytes())?;
        for head in saved.state.commit_heads() {
            sidecar_heads.push(decode_hex(head, 32)?);
        }
        for id in saved.state.operations() {
            let id = OperationId::from_bytes(&decode_hex(id, 128)?);
            budget.opaque_id(&id)?;
            intent_operations.push(id);
        }
    }
    let intent_operations = sorted_ids(intent_operations);
    let mut roots = BTreeMap::new();
    for id in sidecar_heads {
        budget.root(&mut roots, Kind::Commit, &id)?;
    }
    for tree in &working_copy_trees {
        for label in &tree.labels {
            budget.bytes(
                label
                    .len()
                    .checked_mul(6)
                    .ok_or(CollectError("working-copy label budget overflow"))?,
            )?;
        }
        for id in &tree.terms {
            budget.root(&mut roots, Kind::Tree, id)?;
        }
    }

    enum Visit {
        Enter(OperationId),
        Exit(OperationId),
    }
    let mut states: HashMap<OperationId, u8> = HashMap::new();
    let mut pending = operation_heads
        .iter()
        .cloned()
        .map(Visit::Enter)
        .collect::<Vec<_>>();
    pending.push(Visit::Enter(working_copy_operation.clone()));
    pending.extend(intent_operations.iter().cloned().map(Visit::Enter));
    while let Some(visit) = pending.pop() {
        budget.check_time()?;
        match visit {
            Visit::Exit(id) => {
                states.insert(id, 2);
            }
            Visit::Enter(id) => {
                match states.get(&id) {
                    Some(2) => continue,
                    Some(1) => return Err(CollectError("cycle in required operation ancestry")),
                    _ => {}
                }
                let operation = read_operation(&store, &id, &mut budget).await?;
                roots_from_operation(&operation, &mut roots, &mut budget).await?;
                states.insert(id.clone(), 1);
                pending.push(Visit::Exit(id));
                for parent in operation.parent_ids().iter().rev() {
                    pending.push(Visit::Enter(parent.clone()));
                }
            }
        }
    }
    // Young/orphan operation files may outlive the reachable operation DAG.
    // Their own views/predecessors remain roots; expired orphan parents need not
    // exist, unlike ancestors required by a current head or actual working copy.
    for id in &retained {
        if id == store.root_operation_id() {
            return Err(CollectError(
                "intrinsic operation unexpectedly has a retained file",
            ));
        }
        if states.contains_key(id) {
            continue;
        }
        let operation = read_operation(&store, id, &mut budget).await?;
        roots_from_operation(&operation, &mut roots, &mut budget).await?;
        states.insert(id.clone(), 2);
    }
    let retained_operations = sorted_ids(states.into_keys().collect());
    before_final_check().await;
    let after_wc = fresh_working_copy(workspace, limits)?;
    budget.bytes(after_wc.encoded_bytes)?;
    if after_wc.operation != working_copy_operation
        || after_wc.state_path != working_copy.state_path
        || after_wc.workspace != working_copy_name
        || after_wc.trees != working_copy_trees
        || after_wc.persisted_bytes != working_copy_bytes
    {
        return Err(CollectError(
            "persisted working-copy state changed during collection",
        ));
    }
    // Detect visible concurrent graph changes. This is additional conservative
    // validation, not a substitute for the future broker command/GC fence.
    let after = head_store
        .get_op_heads_bounded(limits.max_operations)
        .map_err(|_| CollectError("operation heads changed or became unavailable"))?;
    if sorted_ids(after) != operation_heads {
        return Err(CollectError("operation heads changed during collection"));
    }
    if simple
        .retained_operation_ids(limits.max_operations)
        .map_err(|_| CollectError("retained operations changed or exceeded budget"))?
        != retained
    {
        return Err(CollectError(
            "retained operations changed during collection",
        ));
    }
    if crate::remote_state::retained_roots(&after_wc.state_path, workspace.repo_path())
        .map_err(|_| CollectError("native sidecar changed or became unavailable"))?
        != sidecar
    {
        return Err(CollectError("native sidecar changed during collection"));
    }
    if crate::forge_source::retained_roots(&after_wc.state_path)
        .map_err(|_| CollectError("forge source retention state changed or became unavailable"))?
        != forge_sidecar
    {
        return Err(CollectError(
            "forge source retention state changed during collection",
        ));
    }
    budget.check_time()?;
    Ok(RootCheckpoint {
        operation_heads,
        working_copy_operation,
        retained_operations,
        intent_operations,
        object_roots: roots.into_values().collect(),
        operations: budget.operations,
        parent_edges: budget.edges,
        root_references: budget.references,
        intrinsic_commit_references: budget.intrinsic,
        encoded_bytes_upper_bound: budget.encoded,
        elapsed_ms: budget.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::{
        backend::{Backend, BackendInitError},
        config::{ConfigLayer, ConfigSource, StackedConfig},
        settings::UserSettings,
        signing::Signer,
    };

    #[test]
    fn native_audit_keeps_pending_merge_terms_and_detects_non_tree_state_changes() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let fixture_root = directory.path().join("operation-fixture");
            std::fs::create_dir(&fixture_root).unwrap();
            let mut config = StackedConfig::with_defaults();
            config.add_layer(ConfigLayer::parse(ConfigSource::User,
                "[user]\nname='Native Fixture'\nemail='fixture@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n").unwrap());
            let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
            let initializer =
                |_: &UserSettings,
                 path: &Path|
                 -> std::result::Result<Box<dyn Backend>, BackendInitError> {
                    Ok(Box::new(jj_native_backend::NativeBackend::init(
                        path,
                        Default::default(),
                    )?))
                };
            let (operation_workspace, _) = Workspace::init_with_backend(
                &settings,
                &fixture_root,
                &initializer,
                Signer::new(None, vec![]),
            )
            .await
            .unwrap();
            let root = directory.path().join("projection");
            std::fs::create_dir(&root).unwrap();
            let state_path = directory.path().join("native-state");
            let state_dir = private_fs::PrivateDir::create(&state_path).unwrap();
            let root = root.canonicalize().unwrap();
            let store = operation_workspace.repo_loader().store().clone();
            let empty: [u8; 32] = store.empty_tree_id().as_bytes().try_into().unwrap();
            let operation = operation_workspace.working_copy().operation_id().hex();
            let before = serde_json::json!({"repository":"fixture", "generation":1,
                "tree":{"terms":[empty], "labels":[]}});
            let pending_terms = vec![[11u8; 32], [12u8; 32], [13u8; 32]];
            let state = serde_json::json!({"version":1,"root":root,"repository":"fixture",
                "operation":operation,"workspace":WorkspaceName::DEFAULT.as_str(),"projection":before,
                "pending":{"request_id":"3345c9ee-95f8-48ca-8737-c73e686dd098","before":before,
                    "action":{"kind":"CheckOut","tree":{"terms":pending_terms,"labels":["left","base","right"]}},"receipt":null},
                "unfinished":null,"acknowledgement":null});
            let original = serde_json::to_vec(&state).unwrap();
            state_dir.replace("native-vfs.json", &original).unwrap();
            let location = NativeLocation {
                state_path: state_dir.path().to_owned(),
                repository: "fixture".into(),
                registry: None,
                registration: None,
            };
            let retained = location.read(&root, &CollectorLimits::default()).unwrap();
            assert!(retained.pending);
            let readonly =
                ReadonlyNativeWorkingCopy::new(store, location.clone(), retained).unwrap();
            assert!(readonly.start_mutation().await.is_err());
            let workspace = Workspace::new(
                &root,
                operation_workspace.repo_path().to_owned(),
                Box::new(readonly),
                operation_workspace.repo_loader().clone(),
            )
            .unwrap();
            let checkpoint = collect(&workspace, &CollectorLimits::default())
                .await
                .unwrap();
            for id in [empty, [11; 32], [12; 32], [13; 32]] {
                assert!(
                    checkpoint
                        .object_roots
                        .iter()
                        .any(|key| key.kind == Kind::Tree && key.id.0 == id)
                );
            }
            let too_small = CollectorLimits {
                max_file_bytes: original.len() - 1,
                ..CollectorLimits::default()
            };
            assert!(collect(&workspace, &too_small).await.is_err());
            let decode_limited = CollectorLimits {
                max_decode_bytes: 4096 + original.len() * 31,
                ..CollectorLimits::default()
            };
            assert!(location.read(&root, &decode_limited).is_err());
            let error = collect_with_hook(&workspace, &CollectorLimits::default(), || async {
                // Same operation, tree terms, labels and recovery flags. Only
                // exact persisted bytes change; cached/typed-only checks miss it.
                let mut changed = original.clone();
                changed.push(b' ');
                state_dir.replace("native-vfs.json", &changed).unwrap();
            })
            .await
            .err()
            .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("persisted working-copy state changed")
            );
            assert!(state_dir.path().join("native-vfs.json").is_file());
        });
    }

    #[test]
    fn persisted_working_copy_change_without_new_operation_rejects_inventory() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let mut config = StackedConfig::with_defaults();
            config.add_layer(ConfigLayer::parse(ConfigSource::User,
                "[user]\nname='Native Fixture'\nemail='fixture@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n").unwrap());
            let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
            let initializer =
                |_: &UserSettings,
                 path: &std::path::Path|
                 -> std::result::Result<Box<dyn Backend>, BackendInitError> {
                    Ok(Box::new(jj_native_backend::NativeBackend::init(
                        path,
                        jj_native_backend::Config::default(),
                    )?))
                };
            let (workspace, _) = Workspace::init_with_backend(
                &settings,
                directory.path(),
                &initializer,
                Signer::new(None, vec![]),
            )
            .await
            .unwrap();
            let original = collect(&workspace, &CollectorLimits::default())
                .await
                .unwrap();
            let root = workspace
                .repo_loader()
                .op_store()
                .root_operation_id()
                .clone();
            assert_ne!(original.working_copy_operation, root);
            let result = collect_with_hook(&workspace, &CollectorLimits::default(), || async {
                workspace
                    .working_copy()
                    .start_mutation()
                    .await
                    .unwrap()
                    .finish(root)
                    .await
                    .unwrap();
            })
            .await;
            let error = match result {
                Ok(_) => panic!("changed persisted working copy must reject the entire audit"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("persisted working-copy state changed")
            );
            // The mutation only finishes an existing operation. The other
            // inventory guards cannot detect it by new operation/head files.
            let after = collect(&workspace, &CollectorLimits::default())
                .await
                .unwrap();
            assert_eq!(after.operation_heads, original.operation_heads);
            assert_eq!(after.retained_operations, original.retained_operations);
            assert_ne!(
                after.working_copy_operation,
                original.working_copy_operation
            );
            // Cached workspace state is intentionally stale; the audit uses
            // fresh persisted state rather than accepting this old accessor.
            assert_eq!(
                workspace.working_copy().operation_id(),
                &original.working_copy_operation
            );
            let state_path = workspace
                .working_copy()
                .downcast_ref::<LocalWorkingCopy>()
                .unwrap()
                .state_path()
                .join("tree_state");
            std::fs::remove_file(&state_path).unwrap();
            assert!(
                collect(&workspace, &CollectorLimits::default())
                    .await
                    .is_err()
            );
            assert!(
                !state_path.exists(),
                "read-only audit must not reinitialize missing WC state"
            );
        });
    }
}
