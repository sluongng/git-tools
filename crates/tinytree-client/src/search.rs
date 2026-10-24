//! Revision-pinned indexed search. Snippets and coverage are authenticated
//! assertions; a partial FILE snippet cannot prove a canonical FILE digest.
use super::*;
use crate::CheckedScopeHandle;
use std::{
    sync::{Mutex, Weak},
    time::{SystemTime, UNIX_EPOCH},
};
use tinytree_model::CommitId;
use tinytree_proto::query_v1 as q;

const STATE_BYTES: usize = 16 * 1024;
const MAX_CURSOR_BYTES: usize = 4096;

struct SearchMemory {
    _guards: crate::verified::LifetimeGuards,
    _state: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
struct OwnedPage {
    // Payload must disappear before its independent allocation reservation.
    value: q::SearchResult,
    _memory: SearchMemory,
}
#[derive(Clone)]
pub struct CheckedSearchPage(Arc<OwnedPage>);
impl CheckedSearchPage {
    pub fn result(&self) -> &q::SearchResult {
        &self.0.value
    }
}
impl std::fmt::Debug for CheckedSearchPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedSearchPage")
            .field("hits", &self.result().hits.len())
            .field("coverage", &self.result().coverage)
            .finish_non_exhaustive()
    }
}
/// Bounded immutable search state. Waiting between pages consumes the original
/// deadline, but never holds an inflight permit. Warming does not advance it.
pub struct CheckedSearch {
    scope: CheckedScopeHandle,
    client: Weak<Inner>,
    revision: CommitId,
    pattern: Vec<u8>,
    prefix: String,
    policy: q::SearchPolicy,
    page_size: u32,
    effective_page_size: Option<u32>,
    cursor: Vec<u8>,
    root: Option<[u8; 32]>,
    manifest: Option<[u8; 32]>,
    progress: (u64, u64),
    last: Option<SearchPosition>,
    pages: usize,
    deadline: Instant,
    coverage: Option<q::SearchCoverage>,
    memory: Arc<SearchMemory>,
}
struct SearchPosition {
    path: String,
    file_id: [u8; 32],
    file_size: u64,
    offset: u64,
    width: u64,
    line: u64,
    column: u64,
}
impl std::fmt::Debug for CheckedSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedSearch")
            .field("pages", &self.pages)
            .field("coverage", &self.coverage)
            .finish_non_exhaustive()
    }
}
impl CheckedSearch {
    pub fn is_complete(&self) -> bool {
        self.coverage == Some(q::SearchCoverage::SearchComplete)
    }
    pub fn is_warming(&self) -> bool {
        self.coverage == Some(q::SearchCoverage::SearchWarming)
    }
    pub fn is_terminal(&self) -> bool {
        self.coverage.is_some_and(|value| {
            !matches!(
                value,
                q::SearchCoverage::SearchMore | q::SearchCoverage::SearchWarming
            )
        })
    }
}
fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.contains(['\\', '\0'])
        && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
}
fn selected(prefix: &str, path: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}
fn checked_input(
    pattern: &[u8],
    prefix: &str,
    policy: q::SearchPolicy,
    page_size: u32,
    cursor: &[u8],
    limits: &crate::Limits,
) -> Result<(), Status> {
    let mode = match policy {
        q::SearchPolicy::LiteralBytesV1 => tinytree_search::SearchMode::Literal,
        q::SearchPolicy::RegexBytesV1 => tinytree_search::SearchMode::Regex,
        _ => {
            return Err(Status::invalid_argument(
                "unsupported immutable search policy",
            ));
        }
    };
    tinytree_search::validate_input(pattern, mode)
        .map_err(|_| Status::invalid_argument("invalid immutable search pattern"))?;
    if (!prefix.is_empty() && !valid_path(prefix))
        || page_size as usize > limits.max_batch_items
        || cursor.len() > MAX_CURSOR_BYTES
    {
        return Err(Status::invalid_argument("invalid immutable search request"));
    }
    Ok(())
}
fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], Status> {
    value.try_into().map_err(|_| integrity())
}
fn validate(
    reply: &q::SearchResult,
    scope: &CheckedScopeHandle,
    revision: CommitId,
    pattern: &[u8],
    prefix: &str,
    policy: q::SearchPolicy,
    page_size: u32,
    cursor: &[u8],
    limits: &crate::Limits,
) -> Result<usize, Status> {
    use q::SearchCoverage as Coverage;
    let coverage = Coverage::try_from(reply.coverage).map_err(|_| integrity())?;
    if reply.scope_digest.as_slice() != scope.content_digest()
        || reply.index_epoch.as_slice() != scope.index_epoch()
        || reply
            .revision
            .as_ref()
            .is_none_or(|id| id.value.as_slice() != revision.0)
        || reply.pattern != pattern
        || reply.path_prefix != prefix
        || reply.policy != policy as i32
        || coverage as i32 == 0
        || reply.page_size == 0
        || reply.page_size as usize > limits.max_batch_items
        || (page_size != 0 && reply.page_size > page_size)
        || reply.hits.len() > reply.page_size as usize
        || reply.cursor.len() > MAX_CURSOR_BYTES
        || (coverage == Coverage::SearchMore) != !reply.cursor.is_empty()
        || (coverage == Coverage::SearchMore && reply.cursor == cursor)
    {
        return Err(integrity());
    }
    if !reply.root_tree.is_empty() {
        fixed::<32>(&reply.root_tree)?;
    }
    if !reply.manifest_digest.is_empty() {
        fixed::<32>(&reply.manifest_digest)?;
    }
    let ready = !reply.root_tree.is_empty() && !reply.manifest_digest.is_empty();
    if matches!(coverage, Coverage::SearchComplete | Coverage::SearchMore) && !ready {
        return Err(integrity());
    }
    if reply.root_tree.is_empty()
        && (!reply.manifest_digest.is_empty() || matches!(coverage, Coverage::SearchWarming))
    {
        return Err(integrity());
    }
    if !ready
        && (!reply.hits.is_empty() || reply.entries_examined != 0 || reply.file_bytes_read != 0)
    {
        return Err(integrity());
    }
    if coverage == Coverage::SearchWarming
        && (!reply.hits.is_empty() || !reply.manifest_digest.is_empty())
    {
        return Err(integrity());
    }
    let mut retained = std::mem::size_of::<q::SearchResult>()
        + 1024
        + reply.scope_digest.capacity()
        + reply.index_epoch.capacity()
        + reply.revision.as_ref().map_or(0, |v| v.value.capacity())
        + reply.root_tree.capacity()
        + reply.manifest_digest.capacity()
        + reply.pattern.capacity()
        + reply.path_prefix.capacity()
        + reply.cursor.capacity()
        + reply
            .hits
            .capacity()
            .checked_mul(std::mem::size_of::<q::SearchHit>())
            .ok_or_else(integrity)?;
    let mut previous: Option<&q::SearchHit> = None;
    for hit in &reply.hits {
        if !valid_path(&hit.path)
            || !selected(prefix, &hit.path)
            || hit.file_id.len() != 32
            || hit.match_bytes == 0
            || hit.match_bytes as usize > tinytree_search::MAX_PATTERN_BYTES
            || (policy == q::SearchPolicy::LiteralBytesV1
                && hit.match_bytes as usize != pattern.len())
            || hit.line == 0
            || hit.byte_column > hit.byte_offset
            || hit
                .byte_offset
                .checked_add(hit.match_bytes as u64)
                .is_none_or(|end| end > hit.file_size)
            || hit.snippet_offset > hit.byte_offset
            || hit.byte_offset - hit.snippet_offset > 128
            || hit.snippet.len() > tinytree_search::MAX_SNIPPET_BYTES
            || hit.snippet.len() > hit.match_bytes as usize + 256
            || hit
                .snippet_offset
                .checked_add(hit.snippet.len() as u64)
                .is_none_or(|end| end > hit.file_size)
            || hit.line > hit.byte_offset.saturating_add(1)
            || (hit.line == 1 && hit.byte_column != hit.byte_offset)
            || (hit.line > 1 && hit.byte_column == hit.byte_offset)
        {
            return Err(integrity());
        }
        let at = usize::try_from(hit.byte_offset - hit.snippet_offset).map_err(|_| integrity())?;
        let end = at
            .checked_add(hit.match_bytes as usize)
            .ok_or_else(integrity)?;
        let matched = hit.snippet.get(at..end).ok_or_else(integrity)?;
        // Snippets are authenticated assertions, not complete FILE proofs.
        // Re-running anchors or boundaries against truncated context can reject
        // genuine regex hits; validate their span, not an invented file context.
        if (policy == q::SearchPolicy::LiteralBytesV1 && matched != pattern)
            || matched.contains(&b'\n')
            || hit.snippet.len() - end > 128
        {
            return Err(integrity());
        }
        let left = &hit.snippet[..at];
        let prefix_lines = left.iter().filter(|&&b| b == b'\n').count() as u64;
        if hit.line <= prefix_lines
            || hit.byte_column < left.iter().rev().take_while(|&&b| b != b'\n').count() as u64
        {
            return Err(integrity());
        }
        if let Some(previous) = previous {
            if previous.path > hit.path
                || (previous.path == hit.path
                    && (previous
                        .byte_offset
                        .checked_add(previous.match_bytes as u64)
                        .is_none_or(|end| hit.byte_offset < end)
                        || previous.file_id != hit.file_id
                        || previous.file_size != hit.file_size
                        || previous.line > hit.line
                        || (previous.line == hit.line
                            && hit.byte_column.checked_sub(previous.byte_column)
                                != Some(hit.byte_offset - previous.byte_offset))))
            {
                return Err(integrity());
            }
        }
        previous = Some(hit);
        retained = retained
            .checked_add(hit.path.capacity())
            .and_then(|n| n.checked_add(hit.file_id.capacity()))
            .and_then(|n| n.checked_add(hit.snippet.capacity()))
            .ok_or_else(integrity)?;
    }
    if !reply.hits.is_empty() && reply.entries_examined == 0 {
        return Err(integrity());
    }
    Ok(retained)
}
impl NativeClient {
    async fn search_budget(
        &self,
        deadline: Instant,
    ) -> Result<(Arc<Budget>, Arc<Mutex<OwnedSemaphorePermit>>), Status> {
        let mut budget = self.admit(deadline, self.unary_memory()).await?;
        let admitted = Arc::get_mut(&mut budget).ok_or_else(integrity)?;
        let bytes = self
            .inner
            .config
            .limits
            .max_batch_bytes
            .checked_mul(3)
            .ok_or_else(integrity)?;
        let window = Arc::new(Mutex::new(
            admitted._bytes.split(bytes).ok_or_else(integrity)?,
        ));
        let mut guards = admitted._guards.as_ref().clone();
        guards.push(window.clone());
        admitted._guards = Arc::new(guards);
        Ok((budget, window))
    }
    /// One checked page. The cursor remains opaque; callers combining stateless
    /// pages must preserve their original inputs and validate cross-page order.
    pub async fn search(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        pattern: &[u8],
        path_prefix: &str,
        policy: q::SearchPolicy,
        page_size: u32,
        cursor: &[u8],
    ) -> Result<CheckedSearchPage, Status> {
        self.checked_scope(scope, false)?;
        checked_input(
            pattern,
            path_prefix,
            policy,
            page_size,
            cursor,
            &self.inner.config.limits,
        )?;
        let deadline = self.deadline();
        let (budget, window) = self.search_budget(deadline).await?;
        let request = q::SearchRequest {
            scope: Some(scope.key()),
            revision: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
            pattern: pattern.to_vec(),
            path_prefix: path_prefix.to_owned(),
            policy: policy as i32,
            page_size,
            cursor: cursor.to_vec(),
        };
        self.bounded(&request)?;
        let reply = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self.rpc_client(budget.clone());
                let request = request.clone();
                async move {
                    grpc.search(self.request(request, allowance))
                        .await
                        .map(|v| v.into_inner())
                }
            })
            .await?;
        let captured = scope.clone();
        let pattern = pattern.to_vec();
        let prefix = path_prefix.to_owned();
        let cursor = cursor.to_vec();
        let limits = self.inner.config.limits.clone();
        let owner = budget.clone();
        let (reply, retained, _owner) = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let retained = validate(
                    &reply, &captured, revision, &pattern, &prefix, policy, page_size, &cursor,
                    &limits,
                );
                (reply, retained, owner)
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("search validation deadline exceeded"))?
        .map_err(|_| Status::internal("search validation worker failed"))?;
        let retained = retained?;
        if retained > self.inner.config.limits.max_batch_bytes.saturating_mul(2) {
            return Err(Status::resource_exhausted(
                "search result allocation exceeds budget",
            ));
        }
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(u32::try_from(retained).map_err(|_| integrity())?)
            .map_err(|_| {
                Status::resource_exhausted("retained search results exhaust query headroom")
            })?;
        let bytes = window
            .lock()
            .map_err(|_| integrity())?
            .split(retained)
            .ok_or_else(integrity)?;
        Ok(CheckedSearchPage(Arc::new(OwnedPage {
            value: reply,
            _memory: SearchMemory {
                _guards: self.guards.clone(),
                _state: state,
                _bytes: bytes,
            },
        })))
    }
    pub async fn begin_search(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        pattern: &[u8],
        path_prefix: &str,
        policy: q::SearchPolicy,
        page_size: u32,
    ) -> Result<CheckedSearch, Status> {
        self.checked_scope(scope, false)?;
        checked_input(
            pattern,
            path_prefix,
            policy,
            page_size,
            &[],
            &self.inner.config.limits,
        )?;
        if STATE_BYTES
            .checked_add(query_headroom(&self.inner.config.limits))
            .is_none_or(|bytes| bytes > self.inner.config.limits.max_buffered_bytes)
        {
            return Err(Status::resource_exhausted(
                "search state leaves no page admission budget",
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| integrity())?
            .as_secs();
        let remaining = u64::try_from(scope.expires_at_unix_seconds())
            .ok()
            .and_then(|expiry| expiry.checked_sub(now))
            .filter(|&remaining| remaining > 0)
            .ok_or_else(|| Status::failed_precondition("native search scope expired"))?;
        let deadline = self
            .deadline()
            .min(Instant::now() + Duration::from_secs(remaining));
        let state = reapi::policy::timeout(deadline, async {
            self.inner
                .query_state
                .clone()
                .acquire_many_owned(STATE_BYTES as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        let bytes = reapi::policy::timeout(deadline, async {
            self.inner
                .bytes
                .clone()
                .acquire_many_owned(STATE_BYTES as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        Ok(CheckedSearch {
            scope: scope.clone(),
            client: Arc::downgrade(&self.inner),
            revision,
            pattern: pattern.to_vec(),
            prefix: path_prefix.to_owned(),
            policy,
            page_size,
            effective_page_size: None,
            cursor: Vec::new(),
            root: None,
            manifest: None,
            progress: (0, 0),
            last: None,
            pages: 0,
            deadline,
            coverage: None,
            memory: Arc::new(SearchMemory {
                _guards: self.guards.clone(),
                _state: state,
                _bytes: bytes,
            }),
        })
    }
    pub async fn search_page(
        &self,
        search: &mut CheckedSearch,
    ) -> Result<CheckedSearchPage, Status> {
        if !search.client.ptr_eq(&Arc::downgrade(&self.inner)) {
            return Err(Status::invalid_argument("search belongs to another client"));
        }
        if search.is_terminal() {
            return Err(Status::failed_precondition("search already terminated"));
        }
        if search.pages >= self.inner.config.limits.max_graph_pages {
            return Err(Status::resource_exhausted("search page budget exhausted"));
        }
        let page = self
            .clone()
            .with_operation_deadline(search.deadline)
            .with_lifetime_guard(search.memory.clone())
            .search(
                &search.scope,
                search.revision,
                &search.pattern,
                &search.prefix,
                search.policy,
                search.page_size,
                &search.cursor,
            )
            .await?;
        let reply = page.result();
        let coverage = q::SearchCoverage::try_from(reply.coverage).map_err(|_| integrity())?;
        if search
            .effective_page_size
            .is_some_and(|size| size != reply.page_size)
            || (!reply.root_tree.is_empty()
                && search
                    .root
                    .is_some_and(|root| root.as_slice() != reply.root_tree))
            || (!reply.manifest_digest.is_empty()
                && search
                    .manifest
                    .is_some_and(|manifest| manifest.as_slice() != reply.manifest_digest))
        {
            return Err(integrity());
        }
        if coverage != q::SearchCoverage::SearchWarming {
            if reply.entries_examined < search.progress.0
                || reply.file_bytes_read < search.progress.1
            {
                return Err(integrity());
            }
            if let (Some(previous), Some(hit)) = (&search.last, reply.hits.first()) {
                if hit.path < previous.path
                    || (hit.path == previous.path
                        && (previous
                            .offset
                            .checked_add(previous.width)
                            .is_none_or(|end| hit.byte_offset < end)
                            || hit.file_id.as_slice() != previous.file_id
                            || hit.file_size != previous.file_size
                            || hit.line < previous.line
                            || (hit.line == previous.line
                                && hit.byte_column.checked_sub(previous.column)
                                    != Some(hit.byte_offset - previous.offset))))
                {
                    return Err(integrity());
                }
            }
            if coverage == q::SearchCoverage::SearchMore
                && reply.entries_examined == search.progress.0
                && reply.file_bytes_read == search.progress.1
                && reply.hits.is_empty()
            {
                return Err(integrity());
            }
        }
        search.pages += 1;
        search.coverage = Some(coverage);
        search.effective_page_size = Some(reply.page_size);
        if !reply.root_tree.is_empty() {
            search.root = Some(fixed(&reply.root_tree)?);
        }
        if !reply.manifest_digest.is_empty() {
            search.manifest = Some(fixed(&reply.manifest_digest)?);
        }
        if coverage != q::SearchCoverage::SearchWarming {
            search.cursor.clone_from(&reply.cursor);
            search.progress = (reply.entries_examined, reply.file_bytes_read);
            if let Some(hit) = reply.hits.last() {
                search.last = Some(SearchPosition {
                    path: hit.path.clone(),
                    file_id: fixed(&hit.file_id)?,
                    file_size: hit.file_size,
                    offset: hit.byte_offset,
                    width: hit.match_bytes as u64,
                    line: hit.line,
                    column: hit.byte_column,
                });
            }
        }
        Ok(page)
    }
}
