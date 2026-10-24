//! Captured broker queries. Server membership is authenticated, not a hash proof.
use crate::{Inner, backend, io, ledger::Authority};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use jj_native_backend::NativeObjectIo;
use native_broker::{
    BeginQuery, ForkQueryScope, NativeQueryIo, OpenCloudQueryScope, OpenQueryScope, QueryHandle,
    QueryOwner, QueryPage, QueryReply, QueryResources, QueryScope, ScopedPathQuery,
    query_protocol as q,
};
use prost::Message;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tinytree_client::{
    CheckedGraphPage, CheckedGraphQuery, CheckedIdsQuery, CheckedScopeHandle, NativeClient,
};
use tinytree_model as m;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct RemoteQueryIo {
    inner: Arc<Inner>,
    registry: Arc<Mutex<Registry>>,
    memory: Arc<Semaphore>,
    operations: Arc<Semaphore>,
    restore_bound: Option<Arc<crate::restore_io::RestoreBound>>,
}
impl std::fmt::Debug for RemoteQueryIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteQueryIo").finish_non_exhaustive()
    }
}
#[derive(Default)]
struct Registry {
    scopes: HashMap<[u8; 32], ScopeRecord>,
    queries: HashMap<[u8; 32], QueryRecord>,
    replays: HashMap<(u8, [u8; 32]), Replay>,
    _memory: Option<OwnedSemaphorePermit>,
}
#[derive(Default)]
struct Retired {
    queries: Vec<QueryRecord>,
    scopes: Vec<ScopeRecord>,
    replays: Vec<Replay>,
}
impl Retired {
    fn is_empty(&self) -> bool {
        self.queries.is_empty() && self.scopes.is_empty() && self.replays.is_empty()
    }
}
struct RetirementOwner {
    _inner: Arc<Inner>,
    _ticket: io::WorkTicket,
}

struct Replay {
    fingerprint: [u8; 32],
    handle: [u8; 32],
    error: Option<tonic::Code>,
    expires: i64,
    _memory: OwnedSemaphorePermit,
}
struct ScopeRecord {
    captured: Option<Arc<CapturedScope>>,
    expires: i64,
    closed: bool,
    close_acked: bool,
    query_identity_version: u32,
    cloud_source_digest: Option<[u8; 32]>,
}
struct CapturedScope {
    client: NativeClient,
    checked: CheckedScopeHandle,
    authority: Authority,
    overlays: BTreeMap<m::CommitId, Arc<Vec<u8>>>,
    _parent: Option<Arc<CapturedScope>>,
    _memory: OwnedSemaphorePermit,
}
impl CapturedScope {
    fn source_digest(&self) -> Option<[u8; 32]> {
        match &self.authority {
            Authority::CloudQuery(source) => Some(source.digest()),
            _ => None,
        }
    }
}
enum CheckedQuery {
    Ids(CheckedIdsQuery),
    Graph(CheckedGraphQuery),
}
struct QueryRecord {
    scope_handle: [u8; 32],
    query_identity_version: u32,
    begin_key: [u8; 32],
    close_acked: bool,
    scope: Option<Arc<CapturedScope>>,
    client: NativeClient,
    checked: Option<CheckedQuery>,
    pending: Option<Arc<CheckedGraphPage>>,
    pending_ids: Option<tinytree_client::CheckedIdsPage>,
    replay: Option<Arc<PageState>>,
    next: u64,
    expires: i64,
    closed: bool,
    _memory: OwnedSemaphorePermit,
}
struct PageState {
    value: QueryPage,
    _checked: Option<Arc<CheckedGraphPage>>,
    _scope: Arc<CapturedScope>,
    _memory: OwnedSemaphorePermit,
}
struct PhysicalOperation {
    deadline: tokio::time::Instant,
    cleanup: bool,
    _provider: Arc<Inner>,
    _owner: QueryOwner,
    _lease: OwnedSemaphorePermit,
    _ticket: io::WorkTicket,
}
impl PhysicalOperation {
    fn client(self: &Arc<Self>, client: &NativeClient) -> NativeClient {
        let client = client.clone().with_operation_deadline(self.deadline);
        let client = if self.cleanup {
            client.with_cleanup_deadline(self.deadline)
        } else {
            client
        };
        client.with_rpc_lifetime_guard(self.clone())
    }
}
fn now() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    )?)
}
fn status(error: anyhow::Error) -> tonic::Status {
    let code = error
        .downcast_ref::<tonic::Status>()
        .map_or(tonic::Code::FailedPrecondition, tonic::Status::code);
    tonic::Status::new(code, "native repository query operation failed")
}
fn random() -> [u8; 32] {
    let mut id = [0; 32];
    id[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id
}
fn wire_commit(id: m::CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn commit(id: q::CommitId) -> Result<m::CommitId> {
    Ok(m::CommitId(id.value.try_into().map_err(|_| {
        tonic::Status::invalid_argument("invalid native query commit ID")
    })?))
}
fn hash(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"native-broker-query-intent-v1\0");
    for p in parts {
        h.update(&(p.len() as u64).to_be_bytes());
        h.update(p);
    }
    *h.finalize().as_bytes()
}
impl Registry {
    fn expire(&mut self, time: i64) -> Retired {
        let mut retired = Retired::default();
        let expired = self
            .queries
            .iter()
            .filter_map(|(id, q)| (q.expires <= time).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            retired.queries.push(self.queries.remove(&id).unwrap());
        }
        let expired = self
            .scopes
            .iter()
            .filter_map(|(id, q)| (q.expires <= time).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            retired.scopes.push(self.scopes.remove(&id).unwrap());
        }
        let expired = self
            .replays
            .iter()
            .filter_map(|(id, q)| (q.expires <= time).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            retired.replays.push(self.replays.remove(&id).unwrap());
        }
        retired
    }
    fn scope(&self, id: [u8; 32]) -> Result<Arc<CapturedScope>> {
        let record = self
            .scopes
            .get(&id)
            .context("native query scope unavailable")?;
        ensure!(
            !record.closed && record.expires > now()?,
            "native query scope closed or expired"
        );
        record
            .captured
            .clone()
            .context("native query scope unavailable")
    }
    fn query_identity(&self, handle: [u8; 32], id: &[u8; 32]) -> Result<u32> {
        let scope = self
            .scopes
            .get(&handle)
            .context("native query scope unavailable")?;
        let value = QueryScope {
            cloud_source_digest: None,
            handle,
            expires_at_unix_seconds: scope.expires,
            query_identity_version: scope.query_identity_version,
        };
        value.validate_query_id(id)?;
        Ok(scope.query_identity_version)
    }
    fn retire_closed_queries(&mut self, scope_handle: [u8; 32]) -> Result<Retired> {
        let Some(scope) = self.scopes.get(&scope_handle) else {
            return Ok(Retired::default());
        };
        if scope.query_identity_version != 2 || !scope.close_acked {
            return Ok(Retired::default());
        }
        let ids: Vec<_> = self
            .queries
            .iter()
            .filter(|(_, query)| {
                query.scope_handle == scope_handle
                    && query.query_identity_version == 2
                    && query.closed
                    && query.close_acked
            })
            .map(|(handle, query)| (*handle, query.begin_key))
            .collect();
        // Verify all pairings before mutation: a retirement must never remove
        // another creation intent or weaken its replay fingerprint.
        for (handle, key) in &ids {
            ensure!(
                self.replays
                    .get(&(3, *key))
                    .is_some_and(|r| r.handle == *handle),
                "native query replay pairing changed"
            );
        }
        let mut retired = Retired::default();
        for (handle, key) in ids {
            retired
                .queries
                .push(self.queries.remove(&handle).expect("checked query"));
            retired
                .replays
                .push(self.replays.remove(&(3, key)).expect("checked replay"));
        }
        Ok(retired)
    }
    fn replay(&self, kind: u8, key: [u8; 32], fingerprint: [u8; 32]) -> Result<Option<[u8; 32]>> {
        let Some(r) = self.replays.get(&(kind, key)) else {
            return Ok(None);
        };
        ensure!(
            r.fingerprint == fingerprint,
            tonic::Status::already_exists("query replay key has another intent")
        );
        if let Some(code) = r.error {
            return Err(tonic::Status::new(
                code,
                "native query creation outcome unavailable; preserve the original intent",
            )
            .into());
        }
        Ok(Some(r.handle))
    }
}
impl RemoteQueryIo {
    pub(crate) fn new(inner: Arc<Inner>) -> Result<Self> {
        let memory = Arc::new(Semaphore::new(inner.config.query.max_registry_bytes));
        let base = memory
            .clone()
            .try_acquire_many_owned(u32::try_from(inner.config.query.registry_overhead())?)?;
        let registry = Arc::new(Mutex::new(Registry {
            _memory: Some(base),
            ..Default::default()
        }));
        let operations = Arc::new(Semaphore::new(1));
        let weak_registry = Arc::downgrade(&registry);
        let weak_operations = Arc::downgrade(&operations);
        let weak_inner = Arc::downgrade(&inner);
        let interval = std::time::Duration::from_millis(inner.config.query.expiry_interval_ms);
        io::background(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(operations) = weak_operations.upgrade() else {
                    return;
                };
                let Ok(lease) = operations.try_acquire_owned() else {
                    continue;
                };
                let Some(registry) = weak_registry.upgrade() else {
                    return;
                };
                let retired = {
                    let mut registry = registry.lock().await;
                    match now() {
                        Ok(time) => registry.expire(time),
                        Err(_) => continue,
                    }
                };
                drop(registry);
                if retired.is_empty() {
                    drop(lease);
                    continue;
                }
                let Some(inner) = weak_inner.upgrade() else {
                    return;
                };
                let owner = Arc::new(RetirementOwner {
                    _ticket: inner.metadata_work.enter(),
                    _inner: inner,
                });
                // Removed states keep their own quotas until blocking teardown.
                let _ = io::blocking_owned(owner, move || {
                    drop(retired);
                    drop(lease);
                    Ok(())
                })
                .await;
            }
        })?;
        Ok(Self {
            inner,
            registry,
            memory,
            operations,
            restore_bound: None,
        })
    }
    pub(crate) fn shares_inner(&self, objects: &crate::RemoteObjectIo) -> bool {
        Arc::ptr_eq(&self.inner, &objects.inner)
    }
    pub(crate) fn for_restore(&self, bound: Arc<crate::restore_io::RestoreBound>) -> Self {
        let mut this = self.clone();
        this.restore_bound = Some(bound);
        this
    }
    async fn clean_expired(&self) -> Result<()> {
        let retired = {
            let mut registry = self.registry.lock().await;
            registry.expire(now()?)
        };
        if retired.is_empty() {
            return Ok(());
        }
        let owner = Arc::new(RetirementOwner {
            _ticket: self.inner.metadata_work.enter(),
            _inner: self.inner.clone(),
        });
        io::blocking_owned(owner, move || {
            drop(retired);
            Ok(())
        })
        .await
    }
    async fn drop_retired(&self, retired: Retired, owner: QueryOwner) -> Result<()> {
        if !retired.is_empty() {
            io::blocking_owned(owner, move || {
                drop(retired);
                Ok(())
            })
            .await?;
        }
        Ok(())
    }
    fn charge(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        let bytes = u32::try_from(bytes).map_err(|_| {
            tonic::Status::resource_exhausted("native query registry bytes exceeded")
        })?;
        self.memory
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| {
                tonic::Status::resource_exhausted("native query registry bytes exhausted").into()
            })
    }
    async fn run<T: Send + 'static, F: Future<Output = Result<T>> + Send + 'static>(
        &self,
        owner: QueryOwner,
        make: impl FnOnce(Self, Arc<PhysicalOperation>) -> F + Send + 'static,
    ) -> std::result::Result<T, tonic::Status> {
        self.run_mode(owner, false, make).await
    }
    async fn run_mode<T: Send + 'static, F: Future<Output = Result<T>> + Send + 'static>(
        &self,
        owner: QueryOwner,
        cleanup: bool,
        make: impl FnOnce(Self, Arc<PhysicalOperation>) -> F + Send + 'static,
    ) -> std::result::Result<T, tonic::Status> {
        if let Some(bound) = &self.restore_bound {
            let deadline = if cleanup {
                tokio::time::Instant::now() + bound.cleanup
            } else {
                bound.deadline
            };
            let owner: QueryOwner = Arc::new((owner, bound.owner.clone()));
            io::operation_context_scope(
                deadline,
                owner.clone(),
                self.run_inner(owner, cleanup, make),
            )
            .await
        } else {
            self.run_inner(owner, false, make).await
        }
    }
    async fn run_inner<T: Send + 'static, F: Future<Output = Result<T>> + Send + 'static>(
        &self,
        owner: QueryOwner,
        cleanup: bool,
        make: impl FnOnce(Self, Arc<PhysicalOperation>) -> F + Send + 'static,
    ) -> std::result::Result<T, tonic::Status> {
        let deadline = self.inner.deadline();
        let lease = tokio::time::timeout_at(deadline, self.operations.clone().acquire_owned())
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("native query admission deadline exceeded")
            })?
            .map_err(|_| tonic::Status::unavailable("native query adapter draining"))?;
        let operation = Arc::new(PhysicalOperation {
            deadline,
            cleanup,
            _provider: self.inner.clone(),
            _owner: owner,
            _lease: lease,
            _ticket: self.inner.metadata_work.enter(),
        });
        let this = self.clone();
        io::owned(async move {
            let result = tokio::time::timeout_at(deadline, make(this, operation.clone()))
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded("native query operation deadline exceeded")
                })?;
            drop(operation);
            result
        })
        .await
        .map_err(status)
    }
    async fn online(&self, scope: &CapturedScope) -> Result<()> {
        let _ = self.inner.connector.get_bare().await?;
        self.inner.ledger.validate_authority(&scope.authority)
    }
    async fn overlay(
        &self,
        client: &NativeClient,
        raw: Vec<Vec<u8>>,
    ) -> Result<(
        Vec<tinytree_client::VerifiedMetadata>,
        BTreeMap<m::CommitId, Arc<Vec<u8>>>,
    )> {
        ensure!(
            raw.len() <= self.inner.config.closure.max_objects,
            "native query overlay object limit exceeded"
        );
        let mut total = 0usize;
        let mut verified = Vec::new();
        let mut index = BTreeMap::new();
        for bytes in raw {
            total = total
                .checked_add(bytes.len())
                .context("native query overlay byte overflow")?;
            ensure!(
                total <= self.inner.config.closure.max_metadata_bytes,
                "native query overlay bytes exceeded"
            );
            let object = m::decode(&bytes, self.inner.model_limits())?;
            let m::Object::Commit(_) = object else {
                anyhow::bail!("native query overlay requires commits")
            };
            let encoded = m::encode(&object, self.inner.model_limits())?;
            ensure!(
                encoded.bytes == bytes,
                "native query overlay is not canonical"
            );
            let id = m::CommitId(encoded.descriptor.key.id.0);
            ensure!(!index.contains_key(&id), "duplicate native query overlay");
            let item = client
                .verify_metadata(encoded.descriptor, bytes.clone())
                .await?;
            index.insert(id, Arc::new(bytes));
            verified.push(item);
        }
        Ok((verified, index))
    }
    fn replay_reservation(
        &self,
        registry: &mut Registry,
        kind: u8,
        key: [u8; 32],
        fingerprint: [u8; 32],
        expires: i64,
    ) -> Result<[u8; 32]> {
        ensure!(
            registry.replays.len() < self.inner.config.query.max_replays,
            "native query replay count exceeded"
        );
        let charge = self.charge(4096)?;
        let handle = random();
        registry.replays.insert(
            (kind, key),
            Replay {
                fingerprint,
                handle,
                error: Some(tonic::Code::Unavailable),
                expires,
                _memory: charge,
            },
        );
        Ok(handle)
    }
    async fn install(
        &self,
        scope: &Arc<CapturedScope>,
        page: &CheckedGraphPage,
        owner: QueryOwner,
    ) -> Result<()> {
        let mut nodes = Vec::new();
        for row in page.rows() {
            if row.commit == m::CommitId::ROOT || scope.overlays.contains_key(&row.commit) {
                continue;
            }
            if let Some(canonical) = &row.canonical {
                let descriptor = canonical.descriptor();
                let object = canonical.object().clone();
                ensure!(
                    descriptor.key.kind == m::Kind::Commit && descriptor.key.id.0 == row.commit.0,
                    "native canonical row identity mismatch"
                );
                let m::Object::Commit(commit) = &object else {
                    anyhow::bail!("native canonical row is not a commit")
                };
                ensure!(
                    commit.change_id == row.change,
                    "native canonical row ChangeId mismatch"
                );
                nodes.push((descriptor, object));
            }
        }
        if nodes.is_empty() {
            return Ok(());
        }
        let _metadata = self.inner.metadata.clone().lock_owned().await;
        let ledger = self.inner.ledger.clone();
        let authority = scope.authority.clone();
        let plan = nodes.clone();
        let digest = *scope.checked.content_digest();
        let epoch = *scope.checked.index_epoch();
        io::blocking_owned(owner.clone(), move || {
            if matches!(authority, Authority::CloudQuery(_)) {
                ledger.admit_captured_page(&authority, &plan, digest, epoch)
            } else {
                ledger.check_captured_page(&authority, &plan, digest, epoch)
            }
        })
        .await?;
        // This private fetched-data path installs canonical objects, not authored
        // operation/index roots. Only the following explicit witness grants fetches.
        for (expected, object) in &nodes {
            let actual = self
                .inner
                .local
                .write_metadata_with_owner(object.clone(), owner.clone())
                .await
                .map_err(backend)?;
            ensure!(
                actual == *expected,
                "native fetched metadata installation changed identity"
            );
        }
        let ledger = self.inner.ledger.clone();
        let authority = scope.authority.clone();
        if matches!(authority, Authority::CloudQuery(_)) {
            return Ok(());
        }
        io::blocking_owned(owner, move || {
            ledger.admit_captured_page(&authority, &nodes, digest, epoch)
        })
        .await
    }
}
fn scope_fingerprint(roots: &[m::CommitId], overlay: &[Vec<u8>]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"native-broker-query-scope-v1\0");
    h.update(&(roots.len() as u64).to_be_bytes());
    for root in roots {
        h.update(&root.0);
    }
    h.update(&(overlay.len() as u64).to_be_bytes());
    for bytes in overlay {
        h.update(&(bytes.len() as u64).to_be_bytes());
        h.update(bytes);
    }
    *h.finalize().as_bytes()
}

impl RemoteQueryIo {
    fn input_bounds(&self, roots: &[m::CommitId], overlay: &[Vec<u8>]) -> Result<()> {
        ensure!(
            roots.len() <= self.inner.config.max_admitted_roots
                && overlay.len() <= self.inner.config.closure.max_objects,
            "native query input count exceeded"
        );
        let total = overlay.iter().try_fold(0usize, |sum, b| {
            sum.checked_add(b.len())
                .context("native query overlay bytes overflow")
        })?;
        ensure!(
            total <= self.inner.config.closure.max_metadata_bytes,
            "native query overlay byte limit exceeded"
        );
        Ok(())
    }
    fn expression_bounds(&self, expression: &q::GraphExpr) -> Result<()> {
        let mut pending = vec![(expression, 1usize)];
        let mut count = 0usize;
        let mut literals = 0usize;
        while let Some((node, depth)) = pending.pop() {
            count += 1;
            ensure!(
                depth <= 64
                    && count <= self.inner.config.remote.limits.max_batch_items
                    && node.version == 1,
                "native query AST limit exceeded"
            );
            use q::graph_expr::Expression as E;
            match node
                .expression
                .as_ref()
                .context("native query AST expression missing")?
            {
                E::Commits(c) => {
                    literals = literals
                        .checked_add(c.commits.len())
                        .context("native query literal overflow")?;
                    ensure!(
                        literals <= self.inner.config.remote.limits.max_batch_items,
                        "native query literals exceeded"
                    );
                }
                E::Ancestors(v) => {
                    if let Some(c) = v.heads.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
                E::Range(v) => {
                    if let Some(c) = v.roots.as_deref() {
                        pending.push((c, depth + 1));
                    }
                    if let Some(c) = v.heads.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
                E::DagRange(v) => {
                    if let Some(c) = v.roots.as_deref() {
                        pending.push((c, depth + 1));
                    }
                    if let Some(c) = v.heads.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
                E::Heads(v) | E::Roots(v) | E::HasConflict(v) => {
                    if let Some(c) = v.input.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
                E::Union(v) | E::Intersection(v) => {
                    ensure!(
                        v.inputs.len() <= self.inner.config.remote.limits.max_batch_items,
                        "native query set count exceeded"
                    );
                    for c in &v.inputs {
                        pending.push((c, depth + 1));
                    }
                }
                E::Difference(v) => {
                    if let Some(c) = v.left.as_deref() {
                        pending.push((c, depth + 1));
                    }
                    if let Some(c) = v.right.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
                E::Latest(v) => {
                    if let Some(c) = v.candidates.as_deref() {
                        pending.push((c, depth + 1));
                    }
                }
            }
            ensure!(
                pending.len() <= self.inner.config.remote.limits.max_batch_items,
                "native query AST frontier exceeded"
            );
        }
        Ok(())
    }
    async fn captured(&self, handle: [u8; 32]) -> Result<Arc<CapturedScope>> {
        self.clean_expired().await?;
        self.registry.lock().await.scope(handle)
    }
    async fn scope_request<T, F, Fut>(
        &self,
        scope: [u8; 32],
        owner: QueryOwner,
        call: F,
    ) -> std::result::Result<QueryReply<T>, tonic::Status>
    where
        T: Message + Send + 'static,
        F: FnOnce(Arc<CapturedScope>, NativeClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.run(owner, move |this, operation| async move {
            let scope = this.captured(scope).await?;
            this.online(&scope).await?;
            let rpc_client = operation.client(&scope.client);
            let value = call(scope.clone(), rpc_client).await?;
            // Tiny nested protobuf values can allocate much more than their
            // encoded length. Returned facts/prefix payloads need independent
            // ownership after the physical request admission is released.
            let bytes = value
                .encoded_len()
                .checked_mul(128)
                .and_then(|bytes| bytes.checked_add(4096))
                .context("native query reply allocation bound overflow")?;
            let retained = this.charge(bytes)?;
            Ok(QueryReply {
                value,
                owner: Arc::new((scope, retained)),
            })
        })
        .await
    }
}
#[async_trait]
impl NativeQueryIo for RemoteQueryIo {
    fn resources(&self) -> QueryResources {
        let budget = self.inner.config.control_work_budget();
        QueryResources {
            memory_bytes: budget.memory_bytes,
            temporary_bytes: budget.temporary_bytes,
            persistent_memory_bytes: (self.inner.config.query.max_registry_bytes as u64)
                .saturating_add(self.inner.config.remote.limits.max_buffered_bytes as u64),
        }
    }
    async fn open_cloud_scope(
        &self,
        input: OpenCloudQueryScope,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<QueryScope>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            this.input_bounds(&input.source.expected_anchors, &input.overlay)?;
            ensure!(
                input.source.index_root_ids.len() <= 256,
                tonic::Status::resource_exhausted("snapshot selection count exceeded")
            );
            ensure!(
                !input.source.index_root_ids.is_empty()
                    && input.source.index_root_ids.len() <= 256
                    && input.source.index_root_ids.windows(2).all(|p| p[0] < p[1])
                    && !input.source.expected_anchors.is_empty()
                    && input
                        .source
                        .expected_anchors
                        .windows(2)
                        .all(|p| p[0] < p[1])
                    && input.source.provider_identity != [0; 32]
                    && input.source.generation > 0
                    && input.source.manifest_size_bytes > 0
                    && input.source.manifest_size_bytes
                        <= this.inner.config.remote.cloud.max_manifest_bytes as u64,
                tonic::Status::invalid_argument("invalid selected snapshot query source")
            );
            let source_digest = input.source.digest();
            let overlay_digest = scope_fingerprint(&[], &input.overlay);
            let fingerprint = hash(&[
                b"native-cloud-open-scope-v1\0",
                &source_digest,
                &overlay_digest,
            ]);
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            if let Some(handle) = registry.replay(4, input.replay_key, fingerprint)? {
                let scope = registry.scope(handle)?;
                ensure!(
                    scope.source_digest() == Some(source_digest),
                    "cloud scope replay source changed"
                );
                return Ok(QueryReply {
                    value: QueryScope {
                        handle,
                        expires_at_unix_seconds: scope.checked.expires_at_unix_seconds(),
                        query_identity_version: 2,
                        cloud_source_digest: scope.source_digest(),
                    },
                    owner: scope,
                });
            }
            ensure!(
                registry
                    .scopes
                    .values()
                    .filter(|scope| !scope.closed)
                    .count()
                    < this.inner.config.query.max_scopes,
                "native query scope count exceeded"
            );
            let expires = now()?
                .saturating_add(86400)
                .saturating_add((this.inner.config.operation_timeout_ms / 1000 + 1) as i64);
            let handle =
                this.replay_reservation(&mut registry, 4, input.replay_key, fingerprint, expires)?;
            let result: Result<Arc<CapturedScope>> = async {
                let bytes = input.overlay.iter().try_fold(0usize, |sum, bytes| {
                    sum.checked_add(bytes.len())
                        .context("snapshot overlay byte overflow")
                })?;
                let charge = this.charge(
                    8192usize
                        .saturating_add(bytes.saturating_mul(8))
                        .saturating_add(input.overlay.len().saturating_mul(512))
                        .saturating_add(
                            input
                                .source
                                .expected_anchors
                                .len()
                                .saturating_add(input.source.index_root_ids.len())
                                .saturating_mul(1024),
                        ),
                )?;
                let client = this.inner.connector.get_bare().await?;
                let rpc = operation.client(&client);
                let proof = Arc::new(
                    rpc.cloud_retained_version_selected(
                        tinytree_client::RetainedVersionSelector {
                            workspace_id: input.source.workspace_id,
                            generation: input.source.generation,
                            manifest_id: tinytree_client::CloudManifestId(input.source.manifest_id),
                        },
                        tinytree_client::CloudRawDigest {
                            blake3: input.source.manifest_blake3,
                            size_bytes: input.source.manifest_size_bytes,
                        },
                    )
                    .await?,
                );
                let (overlay, overlays) = this.overlay(&rpc, input.overlay).await?;
                let checked = rpc
                    .open_cloud_query_scope(
                        &proof,
                        input.source.index_root_ids.clone(),
                        input.source.expected_anchors.clone(),
                        overlay,
                    )
                    .await?;
                ensure!(
                    checked.query_identity_version() == 2,
                    "snapshot identity negotiation failed"
                );
                let ledger = this.inner.ledger.clone();
                let captured = proof.clone();
                let authority = io::blocking_owned(operation.clone(), move || {
                    ledger.cloud_query_authority(input.source, &captured)
                })
                .await?;
                Ok(Arc::new(CapturedScope {
                    client,
                    checked,
                    authority: Authority::CloudQuery(authority),
                    overlays,
                    _parent: None,
                    _memory: charge,
                }))
            }
            .await;
            match result {
                Ok(scope) => {
                    let expires = scope.checked.expires_at_unix_seconds();
                    let replay = registry.replays.get_mut(&(4, input.replay_key)).unwrap();
                    replay.error = None;
                    replay.expires = expires;
                    registry.scopes.insert(
                        handle,
                        ScopeRecord {
                            captured: Some(scope.clone()),
                            expires,
                            closed: false,
                            close_acked: false,
                            query_identity_version: 2,
                            cloud_source_digest: scope.source_digest(),
                        },
                    );
                    Ok(QueryReply {
                        value: QueryScope {
                            handle,
                            expires_at_unix_seconds: expires,
                            query_identity_version: 2,
                            cloud_source_digest: scope.source_digest(),
                        },
                        owner: scope,
                    })
                }
                Err(error) => {
                    registry
                        .replays
                        .get_mut(&(4, input.replay_key))
                        .unwrap()
                        .error = Some(
                        error
                            .downcast_ref::<tonic::Status>()
                            .map_or(tonic::Code::FailedPrecondition, tonic::Status::code),
                    );
                    Err(error)
                }
            }
        })
        .await
    }
    async fn open_scope(
        &self,
        input: OpenQueryScope,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<QueryScope>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            this.input_bounds(&input.roots, &input.overlay)?;
            let version =
                native_broker::normalize_query_identity_version(input.query_identity_version)?;
            let fingerprint = scope_fingerprint(&input.roots, &input.overlay);
            let fingerprint = if version == 2 {
                hash(&[b"native-open-scope-identity-v2\0", &fingerprint])
            } else {
                fingerprint
            };
            let time = now()?;
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            if let Some(handle) = registry.replay(1, input.replay_key, fingerprint)? {
                let scope = registry.scope(handle)?;
                return Ok(QueryReply {
                    value: QueryScope {
                        cloud_source_digest: scope.source_digest(),
                        handle,
                        expires_at_unix_seconds: scope.checked.expires_at_unix_seconds(),
                        query_identity_version: scope.checked.query_identity_version(),
                    },
                    owner: scope,
                });
            }
            ensure!(
                registry.scopes.values().filter(|s| !s.closed).count()
                    < this.inner.config.query.max_scopes,
                "native query scope count exceeded"
            );
            let expires = time
                .saturating_add(86400)
                .saturating_add((this.inner.config.operation_timeout_ms / 1000 + 1) as i64);
            let handle =
                this.replay_reservation(&mut registry, 1, input.replay_key, fingerprint, expires)?;
            let result: Result<Arc<CapturedScope>> = async {
                let raw_bytes = input.overlay.iter().try_fold(0usize, |sum, b| {
                    sum.checked_add(b.len())
                        .context("query overlay byte overflow")
                })?;
                let charge = this.charge(
                    8192usize
                        .saturating_add(raw_bytes.saturating_mul(8))
                        .saturating_add(input.overlay.len().saturating_mul(512))
                        .saturating_add(input.roots.len().saturating_mul(1024)),
                )?;
                let ledger = this.inner.ledger.clone();
                let roots = input.roots.clone();
                let authority =
                    io::blocking_owned(operation.clone(), move || ledger.query_authority(&roots))
                        .await?;
                let client = this.inner.connector.get_bare().await?;
                let rpc_client = operation.client(&client);
                let (overlay, overlays) = this.overlay(&rpc_client, input.overlay).await?;
                let checked = if version == 2 {
                    rpc_client
                        .open_query_scope_with_overlay_v2(input.roots, overlay)
                        .await?
                } else {
                    rpc_client
                        .open_query_scope_with_overlay(input.roots, overlay)
                        .await?
                };
                ensure!(
                    checked.query_identity_version() == version,
                    tonic::Status::unimplemented("native query identity negotiation failed")
                );
                Ok(Arc::new(CapturedScope {
                    client,
                    checked,
                    authority: Authority::Query(authority),
                    overlays,
                    _parent: None,
                    _memory: charge,
                }))
            }
            .await;
            match result {
                Ok(scope) => {
                    let expires = scope.checked.expires_at_unix_seconds();
                    let replay = registry.replays.get_mut(&(1, input.replay_key)).unwrap();
                    replay.error = None;
                    replay.expires = expires;
                    registry.scopes.insert(
                        handle,
                        ScopeRecord {
                            captured: Some(scope.clone()),
                            expires,
                            closed: false,
                            close_acked: false,
                            query_identity_version: scope.checked.query_identity_version(),
                            cloud_source_digest: scope.source_digest(),
                        },
                    );
                    Ok(QueryReply {
                        value: QueryScope {
                            cloud_source_digest: scope.source_digest(),
                            handle,
                            expires_at_unix_seconds: expires,
                            query_identity_version: scope.checked.query_identity_version(),
                        },
                        owner: scope,
                    })
                }
                Err(error) => {
                    registry
                        .replays
                        .get_mut(&(1, input.replay_key))
                        .unwrap()
                        .error = Some(
                        error
                            .downcast_ref::<tonic::Status>()
                            .map_or(tonic::Code::FailedPrecondition, tonic::Status::code),
                    );
                    Err(error)
                }
            }
        })
        .await
    }
    async fn fork_scope(
        &self,
        input: ForkQueryScope,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<QueryScope>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            this.input_bounds(&[], &input.overlay)?;
            let overlay_hash = scope_fingerprint(&[], &input.overlay);
            let fingerprint = if let Some(source) = input.expected_cloud_source_digest {
                hash(&[&input.parent, &overlay_hash, &source])
            } else {
                hash(&[&input.parent, &overlay_hash])
            };
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            let source = registry
                .scopes
                .get(&input.parent)
                .and_then(|scope| scope.cloud_source_digest);
            if source != input.expected_cloud_source_digest {
                if source.is_none() {
                    anyhow::bail!(tonic::Status::unimplemented(
                        "ordinary scope cannot accept a cloud source"
                    ));
                }
                anyhow::bail!(tonic::Status::failed_precondition(
                    "snapshot fork source changed"
                ));
            }
            if let Some(handle) = registry.replay(2, input.replay_key, fingerprint)? {
                let scope = registry.scope(handle)?;
                return Ok(QueryReply {
                    value: QueryScope {
                        cloud_source_digest: scope.source_digest(),
                        handle,
                        expires_at_unix_seconds: scope.checked.expires_at_unix_seconds(),
                        query_identity_version: scope.checked.query_identity_version(),
                    },
                    owner: scope,
                });
            }
            let parent = registry.scope(input.parent)?;
            this.online(&parent).await?;
            ensure!(
                registry.scopes.values().filter(|s| !s.closed).count()
                    < this.inner.config.query.max_scopes,
                "native query scope count exceeded"
            );
            let handle = this.replay_reservation(
                &mut registry,
                2,
                input.replay_key,
                fingerprint,
                parent.checked.expires_at_unix_seconds(),
            )?;
            let result: Result<Arc<CapturedScope>> = async {
                let bytes = input.overlay.iter().try_fold(0usize, |n, b| {
                    n.checked_add(b.len())
                        .context("query overlay bytes overflow")
                })?;
                let charge = this.charge(
                    8192usize
                        .saturating_add(bytes.saturating_mul(8))
                        .saturating_add(
                            (parent.overlays.len() + input.overlay.len()).saturating_mul(512),
                        ),
                )?;
                let rpc_client = operation.client(&parent.client);
                let (overlay, added) = this.overlay(&rpc_client, input.overlay).await?;
                let checked = rpc_client
                    .fork_query_scope(&parent.checked, overlay)
                    .await?;
                let mut overlays = parent.overlays.clone();
                for (id, bytes) in added {
                    if let Some(old) = overlays.insert(id, bytes.clone()) {
                        ensure!(
                            *old == *bytes,
                            "native private query overlay changed identity"
                        );
                    }
                }
                ensure!(
                    overlays.len() <= this.inner.config.closure.max_objects,
                    "native private query overlay object limit exceeded"
                );
                Ok(Arc::new(CapturedScope {
                    client: parent.client.clone(),
                    checked,
                    authority: parent.authority.clone(),
                    overlays,
                    _parent: Some(parent),
                    _memory: charge,
                }))
            }
            .await;
            match result {
                Ok(scope) => {
                    let expires = scope.checked.expires_at_unix_seconds();
                    let r = registry.replays.get_mut(&(2, input.replay_key)).unwrap();
                    r.error = None;
                    r.expires = expires;
                    registry.scopes.insert(
                        handle,
                        ScopeRecord {
                            captured: Some(scope.clone()),
                            expires,
                            closed: false,
                            close_acked: false,
                            query_identity_version: scope.checked.query_identity_version(),
                            cloud_source_digest: scope.source_digest(),
                        },
                    );
                    Ok(QueryReply {
                        value: QueryScope {
                            cloud_source_digest: scope.source_digest(),
                            handle,
                            expires_at_unix_seconds: expires,
                            query_identity_version: scope.checked.query_identity_version(),
                        },
                        owner: scope,
                    })
                }
                Err(error) => {
                    registry
                        .replays
                        .get_mut(&(2, input.replay_key))
                        .unwrap()
                        .error = Some(
                        error
                            .downcast_ref::<tonic::Status>()
                            .map_or(tonic::Code::FailedPrecondition, tonic::Status::code),
                    );
                    Err(error)
                }
            }
        })
        .await
    }
    async fn facts(
        &self,
        scope: [u8; 32],
        facts: Vec<q::GraphFact>,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::GraphFacts>, tonic::Status> {
        let limits = &self.inner.config.remote.limits;
        if facts.len() > limits.max_batch_items {
            return Err(tonic::Status::resource_exhausted(
                "native query fact count exceeded",
            ));
        }
        let mut bytes = 0usize;
        for fact in &facts {
            if let Some(q::graph_fact::Fact::Membership(value)) = &fact.fact {
                let expression = value.expression.as_ref().ok_or_else(|| {
                    tonic::Status::invalid_argument("missing native membership expression")
                })?;
                if value
                    .candidate
                    .as_ref()
                    .is_none_or(|candidate| candidate.value.len() != 32)
                {
                    return Err(tonic::Status::invalid_argument(
                        "invalid native membership candidate",
                    ));
                }
                self.expression_bounds(expression).map_err(status)?;
            }
            bytes = bytes
                .checked_add(fact.encoded_len())
                .and_then(|n| n.checked_add(8))
                .ok_or_else(|| {
                    tonic::Status::resource_exhausted("native query facts size overflow")
                })?;
            if bytes > limits.max_request_bytes {
                return Err(tonic::Status::resource_exhausted(
                    "native query facts bytes exceeded",
                ));
            }
        }
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let inputs = facts
                .into_iter()
                .map(|fact| {
                    Ok(match fact.fact.context("missing graph fact")? {
                        q::graph_fact::Fact::HasId(v) => tinytree_client::QueryFact::HasId(commit(
                            v.commit.context("missing fact commit")?,
                        )?),
                        q::graph_fact::Fact::IsAncestor(v) => {
                            tinytree_client::QueryFact::IsAncestor {
                                ancestor: commit(v.ancestor.context("missing ancestor")?)?,
                                descendant: commit(v.descendant.context("missing descendant")?)?,
                            }
                        }
                        q::graph_fact::Fact::Heads(v) => tinytree_client::QueryFact::Heads(
                            v.commits.into_iter().map(commit).collect::<Result<_>>()?,
                        ),
                        q::graph_fact::Fact::Membership(v) => {
                            tinytree_client::QueryFact::Membership {
                                expression: v
                                    .expression
                                    .context("missing membership expression")?,
                                candidate: commit(
                                    v.candidate.context("missing membership candidate")?,
                                )?,
                            }
                        }
                        q::graph_fact::Fact::CommonAncestors(v) => {
                            tinytree_client::QueryFact::CommonAncestors {
                                left: v.left.into_iter().map(commit).collect::<Result<_>>()?,
                                right: v.right.into_iter().map(commit).collect::<Result<_>>()?,
                            }
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let answers = rpc_client
                .query_facts(&scope.checked, inputs)
                .await?
                .into_iter()
                .map(|answer| q::GraphFactAnswer {
                    answer: Some(match answer {
                        tinytree_client::FactAnswer::HasId(v) => {
                            q::graph_fact_answer::Answer::HasId(v)
                        }
                        tinytree_client::FactAnswer::IsAncestor(v) => {
                            q::graph_fact_answer::Answer::IsAncestor(v)
                        }
                        tinytree_client::FactAnswer::Heads(ids) => {
                            q::graph_fact_answer::Answer::Heads(q::CommitSet {
                                commits: ids.into_iter().map(wire_commit).collect(),
                            })
                        }
                        tinytree_client::FactAnswer::Membership(value) => {
                            q::graph_fact_answer::Answer::Membership(value)
                        }
                        tinytree_client::FactAnswer::CommonAncestors(ids) => {
                            q::graph_fact_answer::Answer::CommonAncestors(q::CommitSet {
                                commits: ids.into_iter().map(wire_commit).collect(),
                            })
                        }
                    }),
                })
                .collect();
            Ok(q::GraphFacts {
                answers,
                scope_digest: scope.checked.content_digest().to_vec(),
                index_epoch: scope.checked.index_epoch().to_vec(),
            })
        })
        .await
    }
    async fn resolve_commit_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::CommitPrefixResult>, tonic::Status> {
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let result = match rpc_client
                .resolve_commit_prefix(&scope.checked, &prefix)
                .await?
            {
                tinytree_client::CommitPrefix::NoMatch => {
                    q::commit_prefix_result::Result::NoMatch(q::NoMatch {})
                }
                tinytree_client::CommitPrefix::Ambiguous => {
                    q::commit_prefix_result::Result::Ambiguous(q::AmbiguousCommits {})
                }
                tinytree_client::CommitPrefix::Single(id) => {
                    q::commit_prefix_result::Result::Single(q::CommitMatch {
                        commit: Some(wire_commit(id)),
                    })
                }
            };
            Ok(q::CommitPrefixResult {
                selection_digest: None,
                result: Some(result),
            })
        })
        .await
    }
    async fn resolve_change_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        heads: Vec<m::CommitId>,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::ChangePrefixResult>, tonic::Status> {
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let result = match rpc_client
                .resolve_change_prefix(&scope.checked, &prefix, heads)
                .await?
            {
                tinytree_client::ChangePrefix::NoMatch => {
                    q::change_prefix_result::Result::NoMatch(q::NoMatch {})
                }
                tinytree_client::ChangePrefix::Ambiguous => {
                    q::change_prefix_result::Result::Ambiguous(q::AmbiguousChanges {})
                }
                tinytree_client::ChangePrefix::Single(m) => {
                    q::change_prefix_result::Result::Single(q::ChangeMatch {
                        change: Some(q::ChangeId {
                            value: m.change.0.to_vec(),
                        }),
                        targets: m
                            .targets
                            .into_iter()
                            .map(|t| q::ChangeTarget {
                                commit: Some(wire_commit(t.commit)),
                                visible: Some(t.visible),
                            })
                            .collect(),
                    })
                }
            };
            Ok(q::ChangePrefixResult {
                selection_digest: None,
                result: Some(result),
            })
        })
        .await
    }
    async fn shortest_prefix(
        &self,
        scope: [u8; 32],
        target: q::shortest_prefix_request::Target,
        heads: Vec<m::CommitId>,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::ShortestPrefixResult>, tonic::Status> {
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let (target, kind) = match target {
                q::shortest_prefix_request::Target::Commit(id) => (
                    tinytree_client::PrefixTarget::Commit(commit(id)?),
                    q::PrefixKind::CommitPrefix,
                ),
                q::shortest_prefix_request::Target::Change(id) => (
                    tinytree_client::PrefixTarget::Change(m::ChangeId(
                        id.value.try_into().map_err(|_| {
                            tonic::Status::invalid_argument("invalid native ChangeId")
                        })?,
                    )),
                    q::PrefixKind::ChangePrefix,
                ),
            };
            let hex_length = rpc_client
                .shortest_prefix(&scope.checked, target, heads)
                .await?;
            Ok(q::ShortestPrefixResult {
                selection_digest: None,
                kind: kind as i32,
                hex_length,
                in_selection: None,
            })
        })
        .await
    }
    async fn resolve_commit_prefix_selected(
        &self,
        scope: [u8; 32],
        prefix: String,
        selection: q::GraphExpr,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::CommitPrefixResult>, tonic::Status> {
        native_broker::validate_prefix_selection(
            &selection,
            self.inner.config.remote.limits.max_request_bytes,
        )?;
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let digest = tinytree_model::query_prefix_selection_digest(&selection.encode_to_vec());
            let result = match rpc_client
                .resolve_commit_prefix_selected(&scope.checked, &prefix, &selection)
                .await?
            {
                tinytree_client::CommitPrefix::NoMatch => {
                    q::commit_prefix_result::Result::NoMatch(q::NoMatch {})
                }
                tinytree_client::CommitPrefix::Ambiguous => {
                    q::commit_prefix_result::Result::Ambiguous(q::AmbiguousCommits {})
                }
                tinytree_client::CommitPrefix::Single(id) => {
                    q::commit_prefix_result::Result::Single(q::CommitMatch {
                        commit: Some(wire_commit(id)),
                    })
                }
            };
            Ok(q::CommitPrefixResult {
                selection_digest: Some(digest.to_vec()),
                result: Some(result),
            })
        })
        .await
    }
    async fn resolve_change_prefix_selected(
        &self,
        scope: [u8; 32],
        prefix: String,
        heads: Vec<m::CommitId>,
        selection: q::GraphExpr,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::ChangePrefixResult>, tonic::Status> {
        native_broker::validate_prefix_selection(
            &selection,
            self.inner.config.remote.limits.max_request_bytes,
        )?;
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let digest = tinytree_model::query_prefix_selection_digest(&selection.encode_to_vec());
            let result = match rpc_client
                .resolve_change_prefix_selected(&scope.checked, &prefix, heads, &selection)
                .await?
            {
                tinytree_client::ChangePrefix::NoMatch => {
                    q::change_prefix_result::Result::NoMatch(q::NoMatch {})
                }
                tinytree_client::ChangePrefix::Ambiguous => {
                    q::change_prefix_result::Result::Ambiguous(q::AmbiguousChanges {})
                }
                tinytree_client::ChangePrefix::Single(m) => {
                    q::change_prefix_result::Result::Single(q::ChangeMatch {
                        change: Some(q::ChangeId {
                            value: m.change.0.to_vec(),
                        }),
                        targets: m
                            .targets
                            .into_iter()
                            .map(|t| q::ChangeTarget {
                                commit: Some(wire_commit(t.commit)),
                                visible: Some(t.visible),
                            })
                            .collect(),
                    })
                }
            };
            Ok(q::ChangePrefixResult {
                selection_digest: Some(digest.to_vec()),
                result: Some(result),
            })
        })
        .await
    }
    async fn shortest_prefix_selected(
        &self,
        scope: [u8; 32],
        target: q::shortest_prefix_request::Target,
        heads: Vec<m::CommitId>,
        selection: q::GraphExpr,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::ShortestPrefixResult>, tonic::Status> {
        native_broker::validate_prefix_selection(
            &selection,
            self.inner.config.remote.limits.max_request_bytes,
        )?;
        self.scope_request(scope, owner, move |scope, rpc_client| async move {
            let digest = tinytree_model::query_prefix_selection_digest(&selection.encode_to_vec());
            let (target, kind) = match target {
                q::shortest_prefix_request::Target::Commit(id) => (
                    tinytree_client::PrefixTarget::Commit(commit(id)?),
                    q::PrefixKind::CommitPrefix,
                ),
                q::shortest_prefix_request::Target::Change(id) => (
                    tinytree_client::PrefixTarget::Change(m::ChangeId(
                        id.value.try_into().map_err(|_| {
                            tonic::Status::invalid_argument("invalid native ChangeId")
                        })?,
                    )),
                    q::PrefixKind::ChangePrefix,
                ),
            };
            let hex_length = rpc_client
                .shortest_prefix_selected(&scope.checked, target, heads, &selection)
                .await?;
            Ok(q::ShortestPrefixResult {
                selection_digest: Some(digest.to_vec()),
                kind: kind as i32,
                hex_length: hex_length.unwrap_or(0),
                in_selection: Some(hex_length.is_some()),
            })
        })
        .await
    }
    async fn begin(
        &self,
        input: BeginQuery,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<QueryHandle>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            ensure!(
                matches!(
                    input.projection,
                    q::Projection::Ids | q::Projection::Graph | q::Projection::Canonical
                ),
                tonic::Status::invalid_argument("invalid native query projection")
            );
            this.expression_bounds(&input.expression)?;
            let expression = input.expression.encode_to_vec();
            ensure!(
                expression.len() <= this.inner.config.remote.limits.max_request_bytes,
                "native query AST byte limit exceeded"
            );
            let fingerprint = hash(&[
                &input.scope,
                &expression,
                &(input.projection as i32).to_be_bytes(),
                &input.page_size.to_be_bytes(),
            ]);
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            // Binding is checked against the exact local scope before even a
            // cached creation outcome can be observed or a new one reserved.
            let version = registry.query_identity(input.scope, &input.replay_key)?;
            if let Some(handle) = registry.replay(3, input.replay_key, fingerprint)? {
                let query = registry
                    .queries
                    .get(&handle)
                    .context("native query unavailable")?;
                ensure!(!query.closed, "native query closed");
                return Ok(QueryReply {
                    value: QueryHandle {
                        handle,
                        expires_at_unix_seconds: query.expires,
                    },
                    owner: query
                        .scope
                        .clone()
                        .context("native query scope unavailable")?,
                });
            }
            let scope = registry.scope(input.scope)?;
            this.online(&scope).await?;
            ensure!(
                registry.queries.values().filter(|q| !q.closed).count()
                    < this.inner.config.query.max_queries,
                "native query count exceeded"
            );
            let expires = scope
                .checked
                .expires_at_unix_seconds()
                .min(now()?.saturating_add(
                    (this.inner.config.remote.rpc.operation_timeout_ms / 1000 + 1) as i64,
                ));
            let handle =
                this.replay_reservation(&mut registry, 3, input.replay_key, fingerprint, expires)?;
            let result: Result<QueryRecord> = async {
                let charge = this.charge(
                    8192usize
                        .saturating_add(expression.len().saturating_mul(8))
                        .saturating_add(input.page_size as usize * 1024),
                )?;
                // Scope guards contain only independent persistent quotas/roots,
                // never the incoming broker inflight permit. Physical SDK work
                // keeps them after logical query close or caller cancellation.
                let query_client = operation
                    .client(&scope.client)
                    .with_lifetime_guard(scope.clone());
                let checked = if version == 2 {
                    let mut nonce = blake3::Hasher::new();
                    nonce.update(b"tinytree-native-query-upstream-nonce-v2\0");
                    nonce.update(scope.checked.handle());
                    nonce.update(&input.replay_key);
                    let nonce: [u8; 16] = nonce.finalize().as_bytes()[..16]
                        .try_into()
                        .expect("fixed nonce width");
                    let id = scope.checked.bound_query_id(nonce)?;
                    if input.projection == q::Projection::Ids {
                        CheckedQuery::Ids(
                            query_client
                                .begin_query_with_id(
                                    &scope.checked,
                                    &input.expression,
                                    input.page_size,
                                    id,
                                )
                                .await?,
                        )
                    } else {
                        CheckedQuery::Graph(
                            query_client
                                .begin_graph_query_with_id(
                                    &scope.checked,
                                    &input.expression,
                                    input.page_size,
                                    input.projection == q::Projection::Canonical,
                                    id,
                                )
                                .await?,
                        )
                    }
                } else if input.projection == q::Projection::Ids {
                    CheckedQuery::Ids(
                        query_client
                            .begin_query(&scope.checked, &input.expression, input.page_size)
                            .await?,
                    )
                } else {
                    CheckedQuery::Graph(
                        query_client
                            .begin_graph_query(
                                &scope.checked,
                                &input.expression,
                                input.page_size,
                                input.projection == q::Projection::Canonical,
                            )
                            .await?,
                    )
                };
                Ok(QueryRecord {
                    scope_handle: input.scope,
                    query_identity_version: version,
                    begin_key: input.replay_key,
                    close_acked: false,
                    scope: Some(scope.clone()),
                    client: scope.client.clone(),
                    checked: Some(checked),
                    pending: None,
                    pending_ids: None,
                    replay: None,
                    next: 0,
                    expires,
                    closed: false,
                    _memory: charge,
                })
            }
            .await;
            match result {
                Ok(query) => {
                    registry
                        .replays
                        .get_mut(&(3, input.replay_key))
                        .unwrap()
                        .error = None;
                    registry.queries.insert(handle, query);
                    Ok(QueryReply {
                        value: QueryHandle {
                            handle,
                            expires_at_unix_seconds: expires,
                        },
                        owner: scope,
                    })
                }
                Err(error) => {
                    registry
                        .replays
                        .get_mut(&(3, input.replay_key))
                        .unwrap()
                        .error = Some(
                        error
                            .downcast_ref::<tonic::Status>()
                            .map_or(tonic::Code::FailedPrecondition, tonic::Status::code),
                    );
                    Err(error)
                }
            }
        })
        .await
    }
    async fn page(
        &self,
        handle: [u8; 32],
        expected_sequence: u64,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<QueryPage>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            let record = registry
                .queries
                .get_mut(&handle)
                .context("native query unavailable")?;
            ensure!(!record.closed, "native query closed");
            if let Some(replay) = &record.replay {
                if replay.value.sequence == expected_sequence {
                    return Ok(QueryReply {
                        value: replay.value.clone(),
                        owner: replay.clone(),
                    });
                }
            }
            ensure!(
                record.next == expected_sequence,
                "native query sequence unavailable"
            );
            ensure!(
                !record.replay.as_ref().is_some_and(|p| p.value.exhausted),
                "native query already exhausted"
            );
            let scope = record
                .scope
                .clone()
                .context("native query scope unavailable")?;
            this.online(&scope).await?;
            let rpc_client = operation.client(&scope.client);
            let (value, checked) = match record
                .checked
                .as_mut()
                .context("native query state unavailable")?
            {
                CheckedQuery::Ids(query) => {
                    let page = if let Some(page) = &record.pending_ids {
                        page.clone()
                    } else {
                        let page = rpc_client.evaluate_page(query).await?;
                        record.pending_ids = Some(page.clone());
                        page
                    };
                    let value = QueryPage {
                        sequence: page.sequence,
                        exhausted: page.exhausted,
                        rows: page
                            .rows
                            .into_iter()
                            .map(|row| q::GraphRow {
                                commit: Some(wire_commit(row.commit)),
                                change: Some(q::ChangeId {
                                    value: row.change.0.to_vec(),
                                }),
                                edges: Vec::new(),
                                canonical: None,
                            })
                            .collect(),
                    };
                    (value, None)
                }
                CheckedQuery::Graph(query) => {
                    // SDK state advances only once. Retain its checked page until
                    // installation + ledger commit succeed; retry never skips it.
                    let page = if let Some(page) = &record.pending {
                        page.clone()
                    } else {
                        let page = Arc::new(rpc_client.evaluate_graph_page(query).await?);
                        record.pending = Some(page.clone());
                        page
                    };
                    this.install(&scope, &page, operation).await?;
                    let rows = page.rows().iter().map(wire_row).collect();
                    (
                        QueryPage {
                            sequence: page.sequence,
                            exhausted: page.exhausted,
                            rows,
                        },
                        Some(page),
                    )
                }
            };
            let charge = this.charge(page_memory(&value)?)?;
            let page = Arc::new(PageState {
                value,
                _checked: checked,
                _scope: scope.clone(),
                _memory: charge,
            });
            record.next = record
                .next
                .checked_add(1)
                .context("native query sequence exhausted")?;
            record.pending = None;
            record.pending_ids = None;
            record.replay = Some(page.clone());
            Ok(QueryReply {
                value: page.value.clone(),
                owner: page,
            })
        })
        .await
    }
    async fn read_commit(
        &self,
        scope: [u8; 32],
        id: m::CommitId,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<jj_native_backend::VerifiedMetadata>, tonic::Status> {
        self.run(owner, move |this, operation| async move {
            let scope = this.captured(scope).await?;
            this.online(&scope).await?;
            ensure!(
                id != m::CommitId::ROOT,
                tonic::Status::invalid_argument("intrinsic root has no canonical payload")
            );
            if let Some(bytes) = scope.overlays.get(&id) {
                let object = m::decode(bytes, this.inner.model_limits())?;
                let encoded = m::encode(&object, this.inner.model_limits())?;
                let charge = this.charge(bytes.len().saturating_mul(8).saturating_add(4096))?;
                let value = jj_native_backend::VerifiedMetadata::verify(
                    encoded.descriptor,
                    bytes.as_ref().clone(),
                    this.inner.model_limits(),
                )
                .map_err(backend)?;
                return Ok(QueryReply {
                    value,
                    owner: Arc::new((scope, charge)),
                });
            }
            let expression = q::GraphExpr {
                version: 1,
                expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                    commits: vec![wire_commit(id)],
                })),
            };
            let query_client = operation
                .client(&scope.client)
                .with_lifetime_guard(scope.clone());
            let rpc_client = operation.client(&query_client);
            let mut query = query_client
                .begin_graph_query(&scope.checked, &expression, 1, true)
                .await?;
            let mut found = None;
            while !query.is_exhausted() {
                let page = rpc_client.evaluate_graph_page(&mut query).await?;
                this.install(&scope, &page, operation.clone()).await?;
                for row in page.rows() {
                    ensure!(
                        row.commit == id && found.is_none(),
                        "native literal query returned another commit"
                    );
                    let canonical = row
                        .canonical
                        .as_ref()
                        .context("native literal query omitted canonical payload")?;
                    let charge = this.charge(
                        canonical
                            .canonical_bytes()
                            .len()
                            .saturating_mul(8)
                            .saturating_add(4096),
                    )?;
                    let verified = jj_native_backend::VerifiedMetadata::verify(
                        canonical.descriptor(),
                        canonical.canonical_bytes().to_vec(),
                        this.inner.model_limits(),
                    )
                    .map_err(backend)?;
                    found = Some((verified, charge));
                }
            }
            // This inline release is still part of the data operation, unlike
            // explicit release-only RPCs. Never renew its original allowance.
            let cleanup_deadline =
                this.restore_bound
                    .as_ref()
                    .map_or(operation.deadline, |bound| {
                        operation
                            .deadline
                            .min(tokio::time::Instant::now() + bound.cleanup)
                    });
            rpc_client
                .with_cleanup_deadline(cleanup_deadline)
                .close_graph_query(&mut query)
                .await?;
            let (value, charge) = found.context("native commit is outside the captured scope")?;
            Ok(QueryReply {
                value,
                owner: Arc::new((scope, charge)),
            })
        })
        .await
    }
    async fn path_history(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::PathHistoryResult>, tonic::Status> {
        if input.path.len().saturating_add(128) > self.inner.config.remote.limits.max_request_bytes
        {
            return Err(tonic::Status::resource_exhausted(
                "native history request limit exceeded",
            ));
        }
        self.run(owner, move |this, operation| async move {
            let scope = this.captured(input.scope).await?;
            this.online(&scope).await?;
            let client = operation.client(&scope.client);
            let checked = client
                .path_history(
                    &scope.checked,
                    input.revision,
                    &input.path,
                    input.policy,
                    input.max_commits,
                )
                .await?;
            // The SDK's checked result retains its independent output budget.
            // Reserve the broker wire copy before cloning, and retain both
            // owners without keeping the physical RPC/inflight permit alive.
            let bytes = checked
                .result()
                .encoded_len()
                .checked_mul(128)
                .and_then(|n| n.checked_add(4096))
                .context("native history reply allocation bound overflow")?;
            let retained = this.charge(bytes)?;
            Ok(QueryReply {
                value: checked.result().clone(),
                owner: Arc::new((scope, checked, retained)),
            })
        })
        .await
    }
    async fn blame(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> std::result::Result<QueryReply<q::BlameResult>, tonic::Status> {
        if input.path.len().saturating_add(128) > self.inner.config.remote.limits.max_request_bytes
        {
            return Err(tonic::Status::resource_exhausted(
                "native history request limit exceeded",
            ));
        }
        self.run(owner, move |this, operation| async move {
            let scope = this.captured(input.scope).await?;
            this.online(&scope).await?;
            let client = operation.client(&scope.client);
            let checked = client
                .blame(
                    &scope.checked,
                    input.revision,
                    &input.path,
                    input.policy,
                    input.max_commits,
                )
                .await?;
            let bytes = checked
                .result()
                .encoded_len()
                .checked_mul(128)
                .and_then(|n| n.checked_add(4096))
                .context("native history reply allocation bound overflow")?;
            let retained = this.charge(bytes)?;
            Ok(QueryReply {
                value: checked.result().clone(),
                owner: Arc::new((scope, checked, retained)),
            })
        })
        .await
    }
    async fn close_query(
        &self,
        handle: [u8; 32],
        owner: QueryOwner,
    ) -> std::result::Result<(), tonic::Status> {
        self.run_mode(owner, true, move |this, operation| async move {
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            let Some(record) = registry.queries.get_mut(&handle) else {
                return Ok(());
            };
            record.closed = true;
            record.pending = None;
            record.pending_ids = None;
            record.replay = None;
            record.scope = None;
            let rpc_client = operation.client(&record.client);
            if let Some(query) = record.checked.as_mut() {
                match query {
                    CheckedQuery::Ids(query) => rpc_client.close_ids_query(query).await?,
                    CheckedQuery::Graph(query) => rpc_client.close_graph_query(query).await?,
                }
            }
            record.checked = None;
            record.close_acked = true;
            let scope_handle = record.scope_handle;
            // ACK removes all active query/overlay ownership. Keep only a small
            // local tombstone until the original replay window expires.
            let old = std::mem::replace(&mut record._memory, this.charge(0)?);
            drop(old);
            record._memory = this.charge(4096)?;
            let retired = registry.retire_closed_queries(scope_handle)?;
            drop(registry);
            this.drop_retired(retired, operation).await?;
            Ok(())
        })
        .await
    }
    async fn close_scope(
        &self,
        handle: [u8; 32],
        owner: QueryOwner,
    ) -> std::result::Result<(), tonic::Status> {
        self.run_mode(owner, true, move |this, operation| async move {
            this.clean_expired().await?;
            let mut registry = this.registry.lock().await;
            let Some(record) = registry.scopes.get_mut(&handle) else {
                return Ok(());
            };
            record.closed = true;
            if let Some(scope) = &record.captured {
                operation
                    .client(&scope.client)
                    .close_query_scope(&scope.checked)
                    .await?;
            }
            record.captured = None;
            record.close_acked = true;
            let retired = registry.retire_closed_queries(handle)?;
            drop(registry);
            this.drop_retired(retired, operation).await?;
            Ok(())
        })
        .await
    }
}
fn wire_row(row: &tinytree_client::GraphRow) -> q::GraphRow {
    use tinytree_client::GraphEdge;
    let edges = row
        .edges()
        .iter()
        .map(|edge| q::SelectedEdge {
            edge: Some(match edge {
                GraphEdge::Direct(id) => q::selected_edge::Edge::Direct(q::DirectEdge {
                    target: Some(wire_commit(*id)),
                }),
                GraphEdge::Indirect(id) => q::selected_edge::Edge::Indirect(q::IndirectEdge {
                    target: Some(wire_commit(*id)),
                }),
                GraphEdge::Missing(id) => q::selected_edge::Edge::Missing(q::MissingEdge {
                    boundary: Some(wire_commit(*id)),
                }),
            }),
        })
        .collect();
    let canonical = row.canonical.as_ref().map(|metadata| {
        let d = metadata.descriptor();
        tinytree_client::protocol::ObjectData {
            descriptor: Some(tinytree_client::protocol::ObjectDescriptor {
                key: Some(tinytree_client::protocol::ObjectKey {
                    kind: d.key.kind as i32,
                    native_id: d.key.id.0.to_vec(),
                }),
                content_blake3: d.key.id.0.to_vec(),
                size_bytes: d.size_bytes,
                object_format_version: d.format_version as u32,
            }),
            canonical_envelope: metadata.canonical_bytes().to_vec(),
        }
    });
    q::GraphRow {
        commit: Some(wire_commit(row.commit)),
        change: Some(q::ChangeId {
            value: row.change.0.to_vec(),
        }),
        edges,
        canonical,
    }
}
fn page_memory(page: &QueryPage) -> Result<usize> {
    let mut size = 4096usize;
    for row in &page.rows {
        size = size
            .checked_add(row.encoded_len().saturating_mul(8))
            .and_then(|n| n.checked_add(row.edges.capacity().saturating_mul(256)))
            .and_then(|n| n.checked_add(512))
            .context("native query page bytes overflow")?;
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct UnusedSource;
    #[async_trait]
    impl local_cas::BlobSource for UnusedSource {
        async fn fetch(&self, _: &local_cas::Artifact, _: &mut std::fs::File) -> Result<()> {
            anyhow::bail!("unexpected query cleanup fetch")
        }
        async fn missing(
            &self,
            artifacts: &[local_cas::Artifact],
        ) -> Result<Vec<local_cas::Artifact>> {
            Ok(artifacts.to_vec())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_registry_expiry_reclaims_quota_without_a_request_and_does_not_pin_provider() {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let local_path = directory.path().join("local");
        std::fs::create_dir(&local_path).unwrap();
        let direct = Arc::new(
            jj_native_backend::DirectObjectIo::init(&local_path, Default::default()).unwrap(),
        );
        let cache = local_cas::LocalCas::open(
            directory.path().join("cache"),
            Arc::new(UnusedSource),
            Default::default(),
        )
        .unwrap();
        let mut config = crate::Config::default();
        config.query.expiry_interval_ms = 10;
        let repo = crate::RemoteRepository::open(
            direct,
            cache,
            &directory.path().join("ledger"),
            &directory.path().join("staging"),
            config,
        )
        .unwrap();
        let weak_inner = Arc::downgrade(&repo.query.inner);
        let weak_registry = Arc::downgrade(&repo.query.registry);
        let baseline = repo.query.memory.available_permits();
        let retained = repo.query.charge(16384).unwrap();
        repo.query.registry.lock().await.replays.insert(
            (1, [9; 32]),
            Replay {
                fingerprint: [8; 32],
                handle: [7; 32],
                error: Some(tonic::Code::Unavailable),
                expires: now().unwrap() - 1,
                _memory: retained,
            },
        );
        assert_eq!(repo.query.memory.available_permits(), baseline - 16384);
        // No incoming method invokes clean_expired: only the weak background
        // loop can return this accepted persistent reservation.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if repo.query.memory.available_permits() == baseline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        repo.wait_for_idle().await;
        assert!(repo.query.registry.lock().await.replays.is_empty());
        drop(repo);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if weak_inner.upgrade().is_none() && weak_registry.upgrade().is_none() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
