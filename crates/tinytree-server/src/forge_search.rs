//! Disposable service-local search pages. Tokens identify immutable page inputs,
//! never authority; every read and cached replay rechecks current Store policy.
use std::{collections::HashMap, sync::Mutex, time::{Duration, Instant}};
use tinytree_forge_core::search::{ForgeSearchRequest, ForgeSearchResponse, ForgeSearchHit, SkippedFiles, query_fingerprint};
use tinytree_store::{ForgeActor, ResolvedView, Store, ViewVersion};
use tonic::Status;

const MAX_PAGES: usize = 256;
const MAX_CACHE_BYTES: usize = 32 * 1024 * 1024;
const MAX_RESPONSE: usize = 2 * 1024 * 1024;
const MAX_FILE: usize = 1024 * 1024;
const MAX_READ: usize = 8 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(900);
#[derive(Clone, PartialEq, Eq)]
struct Binding {
    query: String, principal: Option<String>, version: [u8;32], revision: [u8;32], epoch: u64,
}
#[derive(Clone, Default)]
struct Position { file: usize, offset: u64, skipped: SkippedFiles }
struct Page {
    binding: Binding, position: Position, expires: Instant,
    result: Option<ForgeSearchResponse>, bytes: usize,
}
#[derive(Default)]
struct Pages { rows: HashMap<String, Page>, bytes: usize }
#[derive(Default)]
pub(crate) struct SearchCache { pages: Mutex<Pages> }
impl SearchCache {
    pub(crate) fn search(&self, store: &Store, view: &ResolvedView, actor: Option<&ForgeActor>, request: &ForgeSearchRequest) -> Result<ForgeSearchResponse, Status> {
        request.validate().map_err(|_| Status::invalid_argument("invalid forge search selectors"))?;
        if request.version.is_some_and(|v| v != view.version().0) {
            return Err(Status::invalid_argument("forge search version mismatch"));
        }
        // Bind the transport-derived actor to a fresh opaque context, not a
        // caller's guessed principal alongside another actor's view handle.
        let selected = store.forge_resolve_view(view.account(), actor, view.name(), Some(ViewVersion(view.version().0)))
            .map_err(crate::service::store_status)?;
        if selected.revision() != view.revision() || selected.epoch() != view.epoch() {
            return Err(Status::aborted("forge search policy changed"));
        }
        let view = &selected;
        let query = query_fingerprint(&view.account().0, view.name(), request)
            .map_err(|_| Status::invalid_argument("invalid forge search selectors"))?;
        let binding = Binding { query, principal: actor.map(|a| a.principal.clone()), version: view.version().0,
            revision: view.revision().0, epoch: view.epoch().0 };
        let mode = if request.regex { tinytree_search::SearchMode::Regex } else { tinytree_search::SearchMode::Literal };
        let pattern = tinytree_search::Pattern::new(request.query.as_bytes(), mode)
            .map_err(|_| Status::invalid_argument("invalid bounded forge search pattern"))?;
        // Serialize a page through publication. Concurrent retries receive the
        // same result and successor rather than racing a mutable iterator.
        let mut pages = self.pages.lock().map_err(|_| Status::unavailable("forge search cache unavailable"))?;
        let now = Instant::now();
        pages.rows.retain(|_, page| page.expires > now);
        pages.bytes = pages.rows.values().map(|page| page.bytes).sum();
        let mut position = if let Some(token) = &request.cursor {
            let page = pages.rows.get(token).ok_or_else(|| Status::aborted("forge search cursor expired or unavailable"))?;
            if page.binding != binding { return Err(Status::aborted("forge search cursor binding changed")); }
            store.forge_check_view(view).map_err(crate::service::store_status)?;
            if let Some(result) = &page.result { return Ok(result.clone()); }
            page.position.clone()
        } else { Position::default() };
        let files = store.forge_view_files(view, 10_000).map_err(crate::service::store_status)?;
        if position.file > files.len() { return Err(Status::aborted("forge search cursor unavailable")); }
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut matches = Vec::new();
        let mut reads = 0usize;
        let mut read_bytes = 0usize;
        let mut progressed = false;
        let mut limit_hit = false;
        let mut budget = tinytree_search::MatchBudget::new(tinytree_search::MatchLimits {
            max_input_bytes: MAX_READ, max_work: 64 * 1024 * 1024, max_output_bytes: 256 * 1024,
        });
        while position.file < files.len() {
            if Instant::now() >= deadline { break; }
            let file = &files[position.file];
            if file.descriptor.key.kind != tinytree_model::Kind::File {
                position.file += 1; position.offset = 0; progressed = true; continue;
            }
            let size = usize::try_from(file.descriptor.size_bytes).unwrap_or(usize::MAX);
            if size > MAX_FILE {
                position.skipped.oversized += 1; position.file += 1; position.offset = 0; progressed = true; continue;
            }
            if reads == 8 || read_bytes.checked_add(size).is_none_or(|n| n > MAX_READ) { break; }
            store.forge_check_view(view).map_err(crate::service::store_status)?;
            let fetched = store.forge_read_view_file(view, &file.path, MAX_FILE);
            // Revocation takes precedence even when storage returned an error.
            store.forge_check_view(view).map_err(crate::service::store_status)?;
            let bytes = fetched.map_err(crate::service::store_status)?;
            if bytes.len() != size { return Err(Status::data_loss("forge search object size mismatch")); }
            reads += 1; read_bytes += bytes.len();
            if std::str::from_utf8(&bytes).is_err() {
                position.skipped.binary += 1; position.file += 1; position.offset = 0; progressed = true; continue;
            }
            let page = match pattern.scan(&bytes, position.offset, request.limit - matches.len(), &mut budget, || Instant::now() < deadline) {
                Ok(page) => page,
                Err(tinytree_search::Error::Invalid) => return Err(Status::invalid_argument("invalid forge search")),
                Err(_) if progressed => break, // Unreturned file hits never advance its position.
                Err(_) => return Err(Status::resource_exhausted("forge search work budget exhausted")),
            };
            matches.extend(page.hits.into_iter().map(|hit| ForgeSearchHit { path: file.path.clone(), line: hit.line,
                column: hit.column, preview: String::from_utf8_lossy(&hit.snippet).into_owned() }));
            progressed = true;
            if let Some(offset) = page.next_offset { position.offset = offset; }
            else { position.file += 1; position.offset = 0; }
            if matches.len() == request.limit { limit_hit = true; break; }
        }
        if !progressed && position.file < files.len() { return Err(Status::resource_exhausted("forge search page deadline exhausted")); }
        store.forge_check_view(view).map_err(crate::service::store_status)?;
        let more = position.file < files.len() || limit_hit;
        let successor = more.then(|| uuid::Uuid::new_v4().to_string());
        let result = ForgeSearchResponse { account_id: view.account().0.clone(), view: view.name().into(), view_version: view.version().0,
            revision: view.revision().0, policy_epoch: view.epoch().0, root_prefix: view.root_prefix().into(), matches,
            complete: !more && position.skipped.binary == 0 && position.skipped.oversized == 0,
            cursor: successor.clone(), skipped_files: position.skipped.clone() };
        let size = serde_json::to_vec(&result).map_err(|_| Status::internal("forge search encoding failed"))?.len();
        if size > MAX_RESPONSE { return Err(Status::resource_exhausted("forge search response limit")); }
        let input_size = binding.query.len() + binding.principal.as_ref().map_or(0, String::len) + 256;
        let added_rows = usize::from(successor.is_some());
        let added_bytes = size + added_rows * input_size;
        if pages.rows.len() + added_rows > MAX_PAGES || pages.bytes.checked_add(added_bytes).is_none_or(|n| n > MAX_CACHE_BYTES) {
            return Err(Status::resource_exhausted("forge search cursor capacity exhausted"));
        }
        if let Some(token) = &request.cursor {
            let page = pages.rows.get_mut(token).ok_or_else(|| Status::aborted("forge search cursor unavailable"))?;
            page.bytes += size; page.result = Some(result.clone());
            pages.bytes += size;
        }
        if let Some(token) = successor {
            pages.rows.insert(token, Page { binding, position, expires: now + TTL, result: None, bytes: input_size });
            pages.bytes += input_size;
        }
        store.forge_check_view(view).map_err(crate::service::store_status)?;
        Ok(result)
    }
}
