// Copyright 2020 The Jujutsu Authors
// Copyright 2026 The git-tools Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Selected conflict-report and private path work over a read-only scratch store.
//! Recursive parent merging and matcher-driven before-side resolution are shared
//! with the bounded stock-compatible path engine.
use super::*;
use futures::AsyncReadExt as _;
use jj_lib::backend::{BackendError, BackendResult};
use std::sync::Mutex;

#[derive(Debug, Default)]
/// Conservative logical selected-work accounting, not a process RSS metric.
/// Provider transport/cache/staging admission remains independently enforced.
pub(super) struct Work {
    items: usize,
    bytes: usize,
    diff: Option<jj_lib::diff::LineDiffBudget>,
    tree_terms: usize,
    deadline: Option<std::time::Instant>,
}
impl Work {
    pub(super) fn exhausted() -> Self {
        Self {
            items: usize::MAX,
            bytes: usize::MAX,
            diff: None,
            tree_terms: usize::MAX,
            deadline: Some(std::time::Instant::now()),
        }
    }
    pub(super) fn refund_bytes(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or(Error::Corrupt)?;
        Ok(())
    }
    pub(super) fn with_deadline(deadline: std::time::Instant) -> Self {
        Self {
            deadline: Some(deadline),
            ..Self::default()
        }
    }
    pub(super) fn check(&self) -> Result<(), Error> {
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            Err(Error::Query(tonic::Code::DeadlineExceeded))
        } else {
            Ok(())
        }
    }
    pub(super) fn reserve(
        &mut self,
        limits: &Limits,
        items: usize,
        bytes: usize,
    ) -> Result<(), Error> {
        self.check()?;
        let next_items = self.items.checked_add(items).ok_or(Error::Limit)?;
        let next_bytes = self.bytes.checked_add(bytes).ok_or(Error::Limit)?;
        if next_items > limits.max_query_rows.min(limits.max_query_edges)
            || next_bytes > limits.max_checkpoint_bytes
        {
            return Err(Error::Limit);
        }
        self.items = next_items;
        self.bytes = next_bytes;
        Ok(())
    }
}
fn backend(error: Error) -> BackendError {
    BackendError::Other(Box::new(error))
}
struct ReadOnlyIo {
    inner: Arc<dyn NativeObjectIo>,
    query: Arc<dyn NativeQueryIo>,
    scope: Arc<CachedScope>,
    limits: Limits,
    work: Arc<Mutex<Work>>,
    files: Mutex<BTreeMap<m::FileId, Arc<[u8]>>>,
    // A size bound only, not authority: retain the longest verified source
    // prefix for every TREE ID, including aliases at different paths.
    tree_prefixes: Mutex<BTreeMap<m::TreeId, usize>>,
    replies: Arc<Mutex<Vec<QueryOwner>>>,
    owner: QueryOwner,
    deadline: std::time::Instant,
}
// jj Store drops its backend before its caches. Keep response owners outside
// the backend too, so decoded cache payloads are destroyed before their guards.
struct SelectedScratch {
    store: Arc<Store>,
    _replies: Arc<Mutex<Vec<QueryOwner>>>,
}
impl std::fmt::Debug for ReadOnlyIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectedReadOnlyIo").finish_non_exhaustive()
    }
}
const MAX_DIFF_PATH_BYTES: usize = 4096;
#[derive(Debug)]
struct SelectedMatcher {
    work: Arc<Mutex<Work>>,
    limits: Limits,
    failed: std::sync::atomic::AtomicBool,
    inner: Box<dyn jj_lib::matchers::Matcher>,
}
impl SelectedMatcher {
    fn accept(&self, path: &jj_lib::repo_path::RepoPath) -> bool {
        use std::sync::atomic::Ordering;
        let text = path.as_internal_file_string();
        let allowed = text.len() <= MAX_DIFF_PATH_BYTES
            && text.split('/').count() <= self.limits.max_depth
            && self.work.lock().is_ok_and(|mut work| {
                work.reserve(&self.limits, 1, text.len().saturating_add(64))
                    .is_ok()
            });
        if !allowed {
            self.failed.store(true, Ordering::SeqCst);
        }
        allowed
    }
    fn check(&self) -> Result<(), RevsetEvaluationError> {
        if self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            Err(rev_err(Error::Limit))
        } else {
            Ok(())
        }
    }
}
impl jj_lib::matchers::Matcher for SelectedMatcher {
    fn matches(&self, path: &jj_lib::repo_path::RepoPath) -> bool {
        self.accept(path) && self.inner.matches(path)
    }
    fn visit(&self, path: &jj_lib::repo_path::RepoPath) -> jj_lib::matchers::Visit {
        if self.accept(path) {
            self.inner.visit(path)
        } else {
            jj_lib::matchers::Visit::Nothing
        }
    }
}
impl ReadOnlyIo {
    async fn metadata(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> BackendResult<jj_native_backend::VerifiedMetadata> {
        if key.kind == m::Kind::Commit {
            // A graph fact ID is not a fetch grant. Admit exactly this canonical
            // commit under the original immutable scope, including deep bases.
            let maximum = self
                .limits
                .max_metadata_bytes
                .checked_mul(16)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| backend(Error::Limit))?;
            self.charge(1, maximum)?;
            let reply = self
                .query
                .read_commit(self.scope.value.handle, m::CommitId(key.id.0), owner)
                .await
                .map_err(|e| backend(status(e)))?;
            self.charge(1, std::mem::size_of::<QueryOwner>() + 64)?;
            let value = reply.value;
            // Keep the independently admitted canonical response owner, not
            // merely the caller's scope/work guard, through decoded cache use.
            {
                let mut replies = self.replies.lock().map_err(|_| backend(Error::Corrupt))?;
                replies.try_reserve(1).map_err(|_| backend(Error::Limit))?;
                replies.push(reply.owner);
            }
            let descriptor = value.descriptor();
            if descriptor.key != key || descriptor.format_version != m::FORMAT_VERSION {
                return Err(backend(Error::Canonical));
            }
            if descriptor.size_bytes > self.limits.max_metadata_bytes as u64 {
                return Err(backend(Error::Limit));
            }
            let actual = usize::try_from(descriptor.size_bytes)
                .ok()
                .and_then(|n| n.checked_mul(16))
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| backend(Error::Limit))?;
            self.work.lock().map_err(|_| backend(Error::Corrupt))?.bytes -= maximum - actual;
            self.metadata_resources(&value)?;
            return Ok(value);
        }
        let expected = self.prepare(key, owner.clone()).await?;
        let value = self.inner.read_metadata_with_owner(key, owner).await?;
        if value.descriptor() != expected {
            return Err(backend(Error::Canonical));
        }
        self.metadata_resources(&value)?;
        Ok(value)
    }
    fn metadata_resources(&self, value: &jj_native_backend::VerifiedMetadata) -> BackendResult<()> {
        self.charge(0, 0)?;
        let descriptor = value.descriptor();
        // VerifiedMetadata intentionally owns its model without exposing a
        // borrowed accessor. The prepaid raw-codec allowance covers this bounded
        // inspection model; it dies before NativeBackend consumes the original.
        let object = m::decode(
            value.bytes(),
            m::Limits {
                max_object_bytes: self.limits.max_metadata_bytes,
                max_items: self.limits.max_query_edges.max(self.limits.max_parents),
                max_field_bytes: self.limits.max_metadata_bytes,
                max_path_bytes: MAX_DIFF_PATH_BYTES,
            },
        )
        .map_err(|_| backend(Error::Canonical))?;
        let prefix = if descriptor.key.kind == m::Kind::Tree {
            *self
                .tree_prefixes
                .lock()
                .map_err(|_| backend(Error::Corrupt))?
                .get(&m::TreeId(descriptor.key.id.0))
                .ok_or_else(|| backend(Error::Canonical))?
        } else {
            0
        };
        if let m::Object::Tree(tree) = &object {
            let terms = self
                .work
                .lock()
                .map_err(|_| backend(Error::Corrupt))?
                .tree_terms
                .max(1);
            let items = tree
                .entries
                .len()
                .checked_mul(terms)
                .and_then(|n| n.checked_mul(terms))
                .and_then(|n| n.checked_mul(4))
                .ok_or_else(|| backend(Error::Limit))?;
            let bytes = tree.entries.iter().try_fold(0usize, |sum, entry| {
                let full_path = prefix
                    .checked_add(entry.name.len())
                    .ok_or_else(|| backend(Error::Limit))?;
                let one = full_path
                    .checked_add(256)
                    .and_then(|n| n.checked_mul(terms))
                    .and_then(|n| n.checked_mul(8))
                    .ok_or_else(|| backend(Error::Limit))?;
                sum.checked_add(one).ok_or_else(|| backend(Error::Limit))
            })?;
            self.charge(items, bytes)?;
        }
        let count = match &object {
            m::Object::Commit(commit) => commit.root_tree.terms.len(),
            m::Object::Tree(tree) => tree.entries.len(),
            _ => 0,
        };
        self.charge(
            count,
            count
                .checked_mul(256)
                .ok_or_else(|| backend(Error::Limit))?,
        )?;
        let mut prefixes = self
            .tree_prefixes
            .lock()
            .map_err(|_| backend(Error::Corrupt))?;
        match &object {
            m::Object::Commit(commit) => {
                for id in &commit.root_tree.terms {
                    prefixes.entry(*id).or_insert(0);
                }
            }
            m::Object::Tree(tree) => {
                for entry in &tree.entries {
                    if let m::TreeValue::Tree(id) = &entry.value {
                        let next = prefix
                            .saturating_add(entry.name.len())
                            .saturating_add(1)
                            .min(MAX_DIFF_PATH_BYTES);
                        let previous = prefixes.entry(*id).or_insert(next);
                        *previous = (*previous).max(next);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn ephemeral(&self, key: m::ObjectKey) -> BackendResult<Option<Arc<[u8]>>> {
        if key.kind != m::Kind::File {
            return Ok(None);
        }
        Ok(self
            .files
            .lock()
            .map_err(|_| backend(Error::Corrupt))?
            .get(&m::FileId(key.id.0))
            .cloned())
    }
    fn charge(&self, items: usize, bytes: usize) -> BackendResult<()> {
        if std::time::Instant::now() >= self.deadline {
            return Err(backend(Error::Query(tonic::Code::DeadlineExceeded)));
        }
        self.work
            .lock()
            .map_err(|_| backend(Error::Corrupt))?
            .reserve(&self.limits, items, bytes)
            .map_err(backend)
    }
    async fn prepare(&self, key: m::ObjectKey, owner: QueryOwner) -> BackendResult<m::Descriptor> {
        let descriptor = if let Some(bytes) = self.ephemeral(key)? {
            m::describe_file(bytes.as_ref())
        } else {
            self.inner
                .descriptor_with_owner(key, owner)
                .await?
                .ok_or_else(|| backend(Error::Canonical))?
        };
        if descriptor.key != key || descriptor.format_version != m::FORMAT_VERSION {
            return Err(backend(Error::Canonical));
        }
        if key.kind != m::Kind::File
            && descriptor.size_bytes > self.limits.max_metadata_bytes as u64
        {
            return Err(backend(Error::Limit));
        }
        // Admit raw canonical bytes and bounded codec/model copies BEFORE the
        // provider can fetch or decode. Stock TREE scratch is charged from the
        // verified entry count below, before backend conversion or iteration.
        let bytes = usize::try_from(descriptor.size_bytes)
            .ok()
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| backend(Error::Limit))?;
        self.charge(1, bytes)?;
        Ok(descriptor)
    }
}
struct MemoryReader {
    bytes: futures::io::Cursor<Arc<[u8]>>,
    _owner: QueryOwner,
}
impl futures::io::AsyncRead for MemoryReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        output: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.bytes).poll_read(cx, output)
    }
}
#[async_trait]
impl NativeObjectIo for ReadOnlyIo {
    fn metadata_work_budget(&self) -> Option<jj_native_backend::ObjectIoResources> {
        self.inner.metadata_work_budget()
    }
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        if let Some(bytes) = self.ephemeral(key)? {
            return Ok(Some(m::describe_file(bytes.as_ref())));
        }
        self.inner
            .descriptor_with_owner(key, self.owner.clone())
            .await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> BackendResult<Option<m::Descriptor>> {
        if let Some(bytes) = self.ephemeral(key)? {
            return Ok(Some(m::describe_file(bytes.as_ref())));
        }
        self.inner
            .descriptor_with_owner(key, Arc::new((self.owner.clone(), owner)))
            .await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> BackendResult<jj_native_backend::VerifiedMetadata> {
        self.metadata(key, self.owner.clone()).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> BackendResult<jj_native_backend::VerifiedMetadata> {
        self.metadata(key, Arc::new((self.owner.clone(), owner)))
            .await
    }
    async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
        Err(backend(Error::Unsupported))
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<jj_native_backend::FileRead> {
        let expected = self
            .prepare(
                m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(id.0),
                },
                self.owner.clone(),
            )
            .await?;
        if let Some(bytes) = self.ephemeral(expected.key)? {
            return jj_native_backend::FileRead::from_preverified_pinned_source(
                expected,
                Box::pin(MemoryReader {
                    bytes: futures::io::Cursor::new(bytes),
                    _owner: self.owner.clone(),
                }),
            );
        }
        let source = self
            .inner
            .read_file_with_owner(id, self.owner.clone())
            .await?;
        let (actual, reader) = source.into_parts();
        if actual != expected {
            return Err(backend(Error::Canonical));
        }
        jj_native_backend::FileRead::from_preverified_pinned_source(actual, reader)
    }
    async fn write_file(
        &self,
        input: &mut (dyn futures::io::AsyncRead + Send + Unpin),
        maximum: u64,
    ) -> BackendResult<m::FileId> {
        // jj auto-resolution synthesizes file contents. They are derived scratch,
        // never authored repository objects or remotely published cache entries.
        self.charge(1, 4096)?;
        let maximum = maximum.min(self.limits.max_checkpoint_bytes as u64);
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            let n = input
                .read(&mut chunk)
                .await
                .map_err(|e| BackendError::Other(Box::new(e)))?;
            if n == 0 {
                break;
            }
            if (bytes.len() as u64)
                .checked_add(n as u64)
                .is_none_or(|n| n > maximum)
            {
                return Err(backend(Error::Limit));
            }
            self.charge(1, n.checked_mul(16).ok_or_else(|| backend(Error::Limit))?)?;
            bytes
                .try_reserve_exact(n)
                .map_err(|_| backend(Error::Limit))?;
            bytes.extend_from_slice(&chunk[..n]);
        }
        let descriptor = m::describe_file(&bytes);
        let id = m::FileId(descriptor.key.id.0);
        self.charge(1, 256)?;
        let mut files = self.files.lock().map_err(|_| backend(Error::Corrupt))?;
        if let Some(existing) = files.get(&id) {
            if existing.as_ref() != bytes.as_slice() {
                return Err(backend(Error::Canonical));
            }
        } else {
            files.insert(id, Arc::from(bytes));
        }
        Ok(id)
    }
    async fn copy_catalog_snapshot(
        &self,
        _: jj_native_backend::CopyCatalogLimits,
    ) -> BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        Err(backend(Error::Unsupported))
    }
}
fn reserve(
    work: &Arc<Mutex<Work>>,
    limits: &Limits,
    items: usize,
    bytes: usize,
) -> Result<(), RevsetEvaluationError> {
    work.lock()
        .map_err(|_| rev_err(Error::Corrupt))?
        .reserve(limits, items, bytes)
        .map_err(rev_err)
}

pub(super) async fn changed(
    caller: &Arc<Store>,
    index: &NativeReadonlyIndex,
    scope: Arc<CachedScope>,
    id: CommitId,
    owner: QueryOwner,
    initial: Work,
) -> Result<(bool, Work), RevsetEvaluationError> {
    changed_impl(caller, index, scope, id, owner, initial, None).await
}
pub(super) async fn changed_path(
    caller: &Arc<Store>,
    index: &NativeReadonlyIndex,
    scope: Arc<CachedScope>,
    id: CommitId,
    owner: QueryOwner,
    initial: Work,
    filter: &q::ChangedPathExpr,
) -> Result<(bool, Work), RevsetEvaluationError> {
    if filter.policy_version != 2 {
        return Err(rev_err(Error::Invalid));
    }
    path_filter::validate_options(caller).map_err(rev_err)?;
    changed_impl(caller, index, scope, id, owner, initial, Some(filter)).await
}
async fn changed_impl(
    caller: &Arc<Store>,
    index: &NativeReadonlyIndex,
    scope: Arc<CachedScope>,
    id: CommitId,
    owner: QueryOwner,
    mut initial: Work,
    filter: Option<&q::ChangedPathExpr>,
) -> Result<(bool, Work), RevsetEvaluationError> {
    let deadline = *initial.deadline.get_or_insert_with(|| {
        std::time::Instant::now() + Duration::from_millis(index.core.limits.sync_query_timeout_ms)
    });
    initial.check().map_err(rev_err)?;
    // Move the same cumulative ledger through every selected candidate. It is
    // never copied or recreated after a successful candidate.
    let mut diff_budget = initial.diff.take().unwrap_or_else(|| {
        jj_lib::diff::LineDiffBudget::new(jj_lib::diff::LineDiffLimits {
            max_input_bytes: index.core.limits.max_checkpoint_bytes,
            max_lines: index.core.limits.max_query_rows,
            max_work: index.core.limits.max_query_edges,
            max_allocation_bytes: index.core.limits.max_checkpoint_bytes,
            max_recursion_depth: index.core.limits.max_depth.min(128),
        })
    });
    let work = Arc::new(Mutex::new(initial));
    let replies = Arc::new(Mutex::new(Vec::new()));
    // Backend initialization verifies this exact intrinsic object before any
    // COMMIT is loaded. It is fixed protocol geometry, not an arbitrary ID grant.
    reserve(&work, &index.core.limits, 1, 256)?;
    let empty_tree = m::TreeId(m::empty_tree().descriptor.key.id.0);
    let io = Arc::new(ReadOnlyIo {
        inner: index.core.objects.clone(),
        query: index.core.query.clone(),
        scope,
        limits: index.core.limits.clone(),
        work: work.clone(),
        files: Mutex::new(BTreeMap::new()),
        tree_prefixes: Mutex::new(BTreeMap::from([(empty_tree, 0)])),
        replies: replies.clone(),
        owner,
        deadline,
    });
    let mut config = jj_native_backend::Config::default();
    config.store.model_limits.max_object_bytes = index.core.limits.max_metadata_bytes;
    config.store.max_file_bytes = index.core.limits.max_checkpoint_bytes as u64;
    config.max_tree_entries = index.core.limits.max_query_edges;
    let backend = jj_native_backend::NativeBackend::from_io(io, config).await?;
    let scratch = SelectedScratch {
        store: Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            caller.merge_options().clone(),
        ),
        _replies: replies,
    };
    let store = &scratch.store;
    let commit = store.get_commit_async(&id).await?;
    if filter.is_none() && !commit.has_conflict() {
        return Err(rev_err(Error::Corrupt));
    }
    let parent_count = commit.parent_ids().len();
    if parent_count > index.core.limits.max_parents {
        return Err(rev_err(Error::Limit));
    }
    reserve(
        &work,
        &index.core.limits,
        parent_count,
        parent_count
            .checked_mul(std::mem::size_of::<Commit>())
            .ok_or_else(|| rev_err(Error::Limit))?,
    )?;
    let parents = commit.parents().await?;
    let changed = if filter.is_none() && parents.len() == 1 {
        commit.tree_ids() != parents[0].tree_ids()
    } else {
        let inner: Box<dyn jj_lib::matchers::Matcher> = if let Some(filter) = filter {
            let path = RepoPathBuf::from_internal_string(&filter.path)
                .map_err(|_| rev_err(Error::Invalid))?;
            if filter.exact_file {
                Box::new(jj_lib::matchers::FilesMatcher::new([path]))
            } else {
                Box::new(jj_lib::matchers::PrefixMatcher::new([path]))
            }
        } else {
            Box::new(jj_lib::matchers::EverythingMatcher)
        };
        let matcher = SelectedMatcher {
            work: work.clone(),
            limits: index.core.limits.clone(),
            failed: std::sync::atomic::AtomicBool::new(false),
            inner,
        };
        let mut graph = ParentSource {
            index,
            work: &work,
            matcher: &matcher,
            deadline,
        };
        if let Some(filter) = filter.filter(|filter| filter.exact_file) {
            use tinytree_annotate::{Budget as _, path::ParentGraph as _};
            let before = tinytree_annotate::path::merged_parent_tree(store, &parents, &mut graph)
                .await
                .map_err(path_error)?;
            let terms = before
                .tree_ids()
                .iter()
                .len()
                .checked_add(commit.tree_ids().iter().len())
                .ok_or_else(|| rev_err(Error::Limit))?;
            graph.prepare_diff(terms)?;
            let path = RepoPathBuf::from_internal_string(&filter.path)
                .map_err(|_| rev_err(Error::Invalid))?;
            for tree in [before, commit.tree()] {
                graph.check()?;
                let value = tree.path_value(&path).await?;
                if value
                    .iter()
                    .any(|term| matches!(term, Some(jj_lib::backend::TreeValue::Tree(_))))
                {
                    return Err(rev_err(Error::UnsupportedExpression(
                        "path_exact_file_directory",
                    )));
                }
            }
        }
        tinytree_annotate::path::changed_with_matcher(
            store,
            &commit,
            &matcher,
            &mut graph,
            &mut diff_budget,
        )
        .await
        .map_err(path_error)?
    };
    // Quiesce scratch caches and payload owners before returning the spent
    // ledger to the next selected candidate.
    drop(commit);
    drop(parents);
    drop(scratch);
    let mut result = std::mem::take(&mut *work.lock().map_err(|_| rev_err(Error::Corrupt))?);
    result.diff = Some(diff_budget);
    Ok((changed, result))
}

struct ParentSource<'a> {
    index: &'a NativeReadonlyIndex,
    work: &'a Arc<Mutex<Work>>,
    matcher: &'a SelectedMatcher,
    deadline: std::time::Instant,
}
impl tinytree_annotate::Budget for ParentSource<'_> {
    type Error = RevsetEvaluationError;
    fn check(&self) -> Result<(), Self::Error> {
        self.matcher.check()?;
        if std::time::Instant::now() >= self.deadline {
            Err(rev_err(Error::Query(tonic::Code::DeadlineExceeded)))
        } else {
            Ok(())
        }
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), Self::Error> {
        self.check()?;
        reserve(
            self.work,
            &self.index.core.limits,
            items.try_into().map_err(|_| rev_err(Error::Limit))?,
            bytes.try_into().map_err(|_| rev_err(Error::Limit))?,
        )
    }
    fn frontier(&mut self, entries: usize) -> Result<(), Self::Error> {
        self.check()?;
        if entries
            > self
                .index
                .core
                .limits
                .max_parents
                .min(self.index.core.limits.max_depth)
        {
            Err(rev_err(Error::Limit))
        } else {
            Ok(())
        }
    }
    fn diff_active(&self) -> bool {
        self.check().is_ok()
    }
}
impl tinytree_annotate::path::ParentGraph for ParentSource<'_> {
    fn prepare_diff(&mut self, raw_terms: usize) -> Result<(), Self::Error> {
        use tinytree_annotate::Budget as _;
        self.frontier(raw_terms)?;
        self.work
            .lock()
            .map_err(|_| rev_err(Error::Corrupt))?
            .tree_terms = raw_terms;
        Ok(())
    }
    async fn common_ancestors(
        &mut self,
        left: &[CommitId],
        right: &[CommitId],
    ) -> Result<Vec<CommitId>, Self::Error> {
        self.index
            .common_ancestors(left, right)
            .await
            .map_err(|e| RevsetEvaluationError::Other(Box::new(e)))
    }
}
fn path_error(
    error: tinytree_annotate::path::Error<RevsetEvaluationError>,
) -> RevsetEvaluationError {
    use tinytree_annotate::path::Error as P;
    match error {
        P::Source(e) => e,
        P::Backend(e) => e.into(),
        P::Resolve(e) => RevsetEvaluationError::Other(Box::new(e)),
        P::Limit => rev_err(Error::Limit),
    }
}
