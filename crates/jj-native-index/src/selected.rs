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

//! Selected conflict-report work. The recursive merge worklist is a bounded
//! adaptation of jj 0.46 `rewrite::find_recursive_merge_commits`; file-diff
//! semantics follow its `default_index::revset_engine::has_diff_from_parent`.
use super::*;
use futures::{AsyncRead as _, AsyncReadExt as _, StreamExt as _};
use jj_lib::{
    backend::{BackendError, BackendResult},
    merge::Merge,
    merged_tree::MergedTree,
};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Default)]
/// Conservative logical selected-work accounting, not a process RSS metric.
/// Provider transport/cache/staging admission remains independently enforced.
pub(super) struct Work {
    items: usize,
    bytes: usize,
}
impl Work {
    fn reserve(&mut self, limits: &Limits, items: usize, bytes: usize) -> Result<(), Error> {
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
    replies: Arc<Mutex<Vec<QueryOwner>>>,
    owner: QueryOwner,
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
        self.accept(path)
    }
    fn visit(&self, path: &jj_lib::repo_path::RepoPath) -> jj_lib::matchers::Visit {
        use jj_lib::matchers::{Visit, VisitDirs, VisitFiles};
        if self.accept(path) {
            Visit::Specific {
                dirs: VisitDirs::All,
                files: VisitFiles::All,
            }
        } else {
            Visit::Nothing
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
            return Ok(value);
        }
        let expected = self.prepare(key, owner.clone()).await?;
        let value = self.inner.read_metadata_with_owner(key, owner).await?;
        if value.descriptor() != expected {
            return Err(backend(Error::Canonical));
        }
        Ok(value)
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
        // Canonical decode/model/string buffers and merge scratch are charged
        // conservatively before physical payload reads, including repeated reads.
        // A tree can expand short entry names into full paths in jj's diff
        // frontier. Reserve the portable path cap per encoded byte as well as
        // ordinary decode scratch, before reading/decoding any tree payload.
        let factor = if key.kind == m::Kind::Tree {
            MAX_DIFF_PATH_BYTES + 16
        } else {
            16
        };
        let bytes = usize::try_from(descriptor.size_bytes)
            .ok()
            .and_then(|v| v.checked_mul(factor))
            .and_then(|v| v.checked_add(4096))
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
    let work = Arc::new(Mutex::new(initial));
    let replies = Arc::new(Mutex::new(Vec::new()));
    let io = Arc::new(ReadOnlyIo {
        inner: index.core.objects.clone(),
        query: index.core.query.clone(),
        scope,
        limits: index.core.limits.clone(),
        work: work.clone(),
        files: Mutex::new(BTreeMap::new()),
        replies: replies.clone(),
        owner,
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
    if !commit.has_conflict() {
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
    let changed = if let [parent] = parents.as_slice() {
        commit.tree_ids() != parent.tree_ids()
    } else {
        let from = parent_tree(store, index, &parents, &work).await?;
        let to = commit.tree();
        let matcher = SelectedMatcher {
            work: work.clone(),
            limits: index.core.limits.clone(),
            failed: std::sync::atomic::AtomicBool::new(false),
        };
        let mut diff = from.diff_stream(&to, &matcher);
        let mut changed = false;
        while let Some(entry) = diff.next().await {
            matcher.check()?;
            reserve(
                &work,
                &index.core.limits,
                1,
                entry.path.as_internal_file_string().len(),
            )?;
            let mut values = entry.values?;
            values.before =
                jj_lib::tree_merge::resolve_file_values(store, &entry.path, values.before).await?;
            if values.is_changed() {
                changed = true;
                break;
            }
        }
        matcher.check()?;
        changed
    };
    let result = *work.lock().map_err(|_| rev_err(Error::Corrupt))?;
    Ok((changed, result))
}

async fn parent_tree(
    store: &Arc<Store>,
    index: &NativeReadonlyIndex,
    parents: &[Commit],
    work: &Arc<Mutex<Work>>,
) -> Result<MergedTree, RevsetEvaluationError> {
    struct Frame {
        ids: Vec<CommitId>,
        next: usize,
        result: Merge<CommitId>,
    }
    fn frame(ids: Vec<CommitId>) -> Frame {
        let result = Merge::resolved(ids[0].clone());
        Frame {
            ids,
            next: 1,
            result,
        }
    }
    fn combine(
        frame: &mut Frame,
        base: Merge<CommitId>,
        work: &Arc<Mutex<Work>>,
        limits: &Limits,
    ) -> Result<(), RevsetEvaluationError> {
        let terms = frame
            .result
            .iter()
            .len()
            .checked_add(base.iter().len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| rev_err(Error::Limit))?;
        reserve(
            work,
            limits,
            terms,
            terms
                .checked_mul(std::mem::size_of::<CommitId>() + 32)
                .ok_or_else(|| rev_err(Error::Limit))?,
        )?;
        let previous = std::mem::replace(
            &mut frame.result,
            Merge::resolved(CommitId::from_bytes(&[0; 32])),
        );
        let other = Merge::resolved(frame.ids[frame.next].clone());
        frame.result = Merge::from_vec(vec![previous, base, other]).flatten();
        frame.next += 1;
        Ok(())
    }
    let merge = match parents {
        [] => Merge::resolved(store.root_commit_id().clone()),
        [parent] => Merge::resolved(parent.id().clone()),
        _ => {
            reserve(
                work,
                &index.core.limits,
                parents.len(),
                parents
                    .len()
                    .checked_mul(64)
                    .ok_or_else(|| rev_err(Error::Limit))?,
            )?;
            let ids = parents.iter().map(|p| p.id().clone()).collect();
            reserve(work, &index.core.limits, 1, std::mem::size_of::<Frame>())?;
            let mut frames = vec![frame(ids)];
            loop {
                if frames.last().unwrap().next == frames.last().unwrap().ids.len() {
                    let done = frames.pop().unwrap().result;
                    if let Some(parent) = frames.last_mut() {
                        combine(parent, done, work, &index.core.limits)?;
                    } else {
                        break done;
                    }
                } else {
                    reserve(work, &index.core.limits, 1, 0)?;
                    let current = frames.last().unwrap();
                    let bases = index
                        .common_ancestors(
                            &current.ids[..current.next],
                            std::slice::from_ref(&current.ids[current.next]),
                        )
                        .await
                        .map_err(|e| RevsetEvaluationError::Other(Box::new(e)))?;
                    reserve(
                        work,
                        &index.core.limits,
                        bases.len(),
                        bases
                            .len()
                            .checked_mul(64)
                            .ok_or_else(|| rev_err(Error::Limit))?,
                    )?;
                    match bases.as_slice() {
                        [] => combine(
                            frames.last_mut().unwrap(),
                            Merge::resolved(store.root_commit_id().clone()),
                            work,
                            &index.core.limits,
                        )?,
                        [base] => combine(
                            frames.last_mut().unwrap(),
                            Merge::resolved(base.clone()),
                            work,
                            &index.core.limits,
                        )?,
                        _ => {
                            reserve(work, &index.core.limits, 1, std::mem::size_of::<Frame>())?;
                            frames.push(frame(bases));
                        }
                    }
                }
            }
        }
    };
    reserve(
        work,
        &index.core.limits,
        merge.iter().len(),
        merge
            .iter()
            .len()
            .checked_mul(std::mem::size_of::<(MergedTree, String)>())
            .ok_or_else(|| rev_err(Error::Limit))?,
    )?;
    let mut trees = Vec::with_capacity(merge.iter().len());
    for id in merge.iter() {
        let commit = store.get_commit_async(id).await?;
        let terms = commit.tree_ids().iter().len();
        let existing_labels =
            commit
                .store_commit()
                .conflict_labels
                .iter()
                .try_fold(0usize, |n, label| {
                    n.checked_add(label.len())
                        .ok_or_else(|| rev_err(Error::Limit))
                })?;
        let fallback = commit
            .description()
            .len()
            .checked_add(128)
            .ok_or_else(|| rev_err(Error::Limit))?;
        let label_bytes = terms
            .checked_mul(
                std::mem::size_of::<jj_lib::backend::TreeId>() + std::mem::size_of::<String>(),
            )
            .and_then(|n| n.checked_add(existing_labels))
            .and_then(|n| n.checked_add(fallback))
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(|| rev_err(Error::Limit))?;
        // Existing conflict labels survive labels_by_term; repeated cached
        // commit terms must pay for each clone/flatten, not just their subject.
        reserve(work, &index.core.limits, terms, label_bytes)?;
        trees.push((commit.tree(), commit.conflict_label()));
    }
    Ok(MergedTree::merge_no_resolve(Merge::from_vec(trees)))
}
