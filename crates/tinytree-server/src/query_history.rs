//! Revision-pinned, bounded fixed-path history. Graph overlays alone never
//! authorize canonical TREE/FILE reads. Only the selected file body is returned.
use crate::{
    query_index::{QueryConfig, QueryHost, Scoped, index_error},
    query_overlay::GraphNode,
};
use std::io::Read;
use tinytree_index::{ScopedNode, WorkBudget};
use tinytree_model as m;
use tinytree_proto::{query_v1 as q, v1 as p};
use tokio::time::Instant;
use tonic::Status;

use q::HistoryCoverage as Coverage;
use q::HistoryPathKind as PathKind;

const MAX_PATH_BYTES: usize = 4096;

enum Failure {
    Status(Status),
    Boundary(Coverage),
}
impl From<Status> for Failure {
    fn from(value: Status) -> Self {
        Self::Status(value)
    }
}
type Step<T> = Result<T, Failure>;

// This token is constructed only by canonical_source(), never graph membership
// alone. Scoped stays retained by the admitted physical Work for every read.
enum CanonicalSource {
    Remote(ScopedNode),
    Frozen(GraphNode),
}
impl CanonicalSource {
    fn descriptor(&self) -> m::Descriptor {
        match self {
            Self::Remote(node) => node.descriptor(),
            Self::Frozen(node) => node.descriptor(),
        }
    }
}

struct Reader<'a> {
    host: &'a QueryHost,
    scope: &'a Scoped,
    repository: &'a str,
    budget: &'a mut WorkBudget,
    deadline: Instant,
    read_bytes: usize,
}
impl Reader<'_> {
    fn check(&self) -> Step<()> {
        if Instant::now() >= self.deadline {
            return Err(Status::deadline_exceeded("history query deadline exceeded").into());
        }
        Ok(())
    }
    fn canonical_source(&mut self, id: m::CommitId) -> Step<CanonicalSource> {
        self.check()?;
        if let Some(node) = self
            .scope
            .scope
            .lookup_node(id, self.budget)
            .map_err(index_error)?
        {
            return Ok(CanonicalSource::Remote(node));
        }
        let node = self
            .scope
            .graph()
            .lookup_node(id, self.budget)
            .map_err(index_error)?
            .ok_or_else(|| Status::invalid_argument("revision is outside query scope"))?;
        if node.is_frozen() && self.scope.authorizes_frozen(node.descriptor()) {
            Ok(CanonicalSource::Frozen(node))
        } else {
            Err(Failure::Boundary(Coverage::HistoryPrivateOverlay))
        }
    }
    fn charge_read(&mut self, size: u64, maximum: usize) -> Step<usize> {
        self.check()?;
        let size =
            usize::try_from(size).map_err(|_| Failure::Boundary(Coverage::HistoryReadLimit))?;
        let total = self
            .read_bytes
            .checked_add(size)
            .ok_or(Failure::Boundary(Coverage::HistoryReadLimit))?;
        if size > maximum || total > self.host.config.max_history_read_bytes {
            return Err(Failure::Boundary(Coverage::HistoryReadLimit));
        }
        self.budget.charge(1, 0, size as u64).map_err(index_error)?;
        self.read_bytes = total;
        Ok(size)
    }
    fn metadata(&mut self, key: m::ObjectKey, maximum: usize) -> Step<(m::Descriptor, m::Object)> {
        let descriptor = self
            .host
            .store
            .descriptor(self.repository, key)
            .map_err(|_| Status::unavailable("history object metadata unavailable"))?
            .ok_or_else(|| Status::data_loss("retained history metadata missing"))?;
        self.charge_read(descriptor.size_bytes, maximum)?;
        if descriptor.key != key || descriptor.format_version != m::FORMAT_VERSION {
            return Err(Status::data_loss("history metadata descriptor mismatch").into());
        }
        let (read_descriptor, bytes) = self
            .host
            .store
            .read_metadata(self.repository, key)
            .map_err(|_| Status::data_loss("history metadata read failed"))?;
        self.check()?;
        if read_descriptor != descriptor
            || bytes.len() as u64 != descriptor.size_bytes
            || blake3::hash(&bytes).as_bytes() != &key.id.0
        {
            return Err(Status::data_loss("history metadata identity mismatch").into());
        }
        let object = m::decode(
            &bytes,
            m::Limits {
                max_object_bytes: maximum,
                max_items: 65_536,
                max_field_bytes: maximum,
                max_path_bytes: MAX_PATH_BYTES,
            },
        )
        .map_err(|error| match error {
            m::Error::Limit => Failure::Boundary(Coverage::HistoryReadLimit),
            _ => Status::data_loss("invalid canonical history metadata").into(),
        })?;
        Ok((descriptor, object))
    }
    fn commit(&mut self, id: m::CommitId) -> Step<m::Commit> {
        let source = self.canonical_source(id)?;
        let expected = source.descriptor();
        let (descriptor, object) =
            self.metadata(expected.key, self.host.config.max_commit_bytes)?;
        if descriptor != expected
            || expected.key.kind != m::Kind::Commit
            || expected.key.id.0 != id.0
        {
            return Err(
                Status::data_loss("history commit does not match scoped descriptor").into(),
            );
        }
        let m::Object::Commit(commit) = object else {
            return Err(Status::data_loss("history source is not a commit").into());
        };
        let (change, conflict, millis, parents) = match source {
            CanonicalSource::Remote(node) => (
                node.change_id(),
                node.has_conflict(),
                node.committer_millis(),
                self.scope
                    .scope
                    .parent_nodes(&node, self.budget)
                    .map_err(index_error)?
                    .into_iter()
                    .map(|node| node.commit())
                    .collect::<Vec<_>>(),
            ),
            CanonicalSource::Frozen(node) => (
                node.change_id(),
                node.has_conflict(),
                node.committer_millis(),
                self.scope
                    .graph()
                    .parent_nodes(&node, self.budget)
                    .map_err(index_error)?
                    .into_iter()
                    .map(|node| node.commit())
                    .collect::<Vec<_>>(),
            ),
        };
        if commit.change_id != change
            || (commit.root_tree.terms.len() > 1) != conflict
            || commit.committer.timestamp.millis_since_epoch != millis
            || commit.parents != parents
        {
            return Err(Status::data_loss("history canonical/scoped metadata mismatch").into());
        }
        Ok(commit)
    }
    fn path(&mut self, commit: &m::Commit, path: &[&str]) -> Step<q::HistoryPathState> {
        if commit.root_tree.terms.len() != 1 {
            return Err(Failure::Boundary(Coverage::HistoryConflictBoundary));
        }
        let mut id = commit.root_tree.terms[0];
        for (position, component) in path.iter().enumerate() {
            self.check()?;
            let (_, object) = self.metadata(
                m::ObjectKey {
                    kind: m::Kind::Tree,
                    id: m::ObjectId(id.0),
                },
                self.host.config.max_history_object_bytes,
            )?;
            let m::Object::Tree(tree) = object else {
                return Err(Status::data_loss("history tree has wrong object kind").into());
            };
            let Some(entry) = tree
                .entries
                .into_iter()
                .find(|entry| entry.name == *component)
            else {
                return Ok(absent());
            };
            if position + 1 == path.len() {
                return Ok(match entry.value {
                    m::TreeValue::File {
                        id,
                        executable,
                        size_bytes,
                        ..
                    } => q::HistoryPathState {
                        kind: PathKind::HistoryPathFile as i32,
                        native_id: id.0.to_vec(),
                        size_bytes,
                        executable,
                    },
                    m::TreeValue::Symlink { id, target_bytes } => q::HistoryPathState {
                        kind: PathKind::HistoryPathSymlink as i32,
                        native_id: id.0.to_vec(),
                        size_bytes: target_bytes,
                        executable: false,
                    },
                    m::TreeValue::Tree(id) => q::HistoryPathState {
                        kind: PathKind::HistoryPathTree as i32,
                        native_id: id.0.to_vec(),
                        size_bytes: 0,
                        executable: false,
                    },
                });
            }
            match entry.value {
                m::TreeValue::Tree(child) => id = child,
                _ => return Ok(absent()),
            }
        }
        Err(Status::internal("empty validated history path").into())
    }
    fn file(&mut self, state: &q::HistoryPathState) -> Step<(m::Descriptor, Vec<u8>)> {
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(
                state
                    .native_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| Status::data_loss("invalid history file ID"))?,
            ),
        };
        let descriptor = self
            .host
            .store
            .descriptor(self.repository, key)
            .map_err(|_| Status::unavailable("history FILE metadata unavailable"))?
            .ok_or_else(|| Status::data_loss("retained history FILE missing"))?;
        if descriptor.key != key
            || descriptor.size_bytes != state.size_bytes
            || descriptor.format_version != m::FORMAT_VERSION
        {
            return Err(Status::data_loss("history FILE descriptor mismatch").into());
        }
        let size =
            self.charge_read(descriptor.size_bytes, self.host.config.max_blame_file_bytes)?;
        let mut lease = self
            .host
            .store
            .read(self.repository, key)
            .map_err(|_| Status::data_loss("history FILE read failed"))?;
        if lease.descriptor() != descriptor {
            return Err(Status::data_loss("history FILE changed descriptor").into());
        }
        // Allocate only after the declared payload and aggregate read limits.
        let mut bytes = vec![0; size];
        lease
            .read_exact(&mut bytes)
            .map_err(|_| Status::data_loss("truncated history FILE"))?;
        let mut tail = [0; 1];
        if lease
            .read(&mut tail)
            .map_err(|_| Status::data_loss("history FILE read failed"))?
            != 0
            || blake3::hash(&bytes).as_bytes() != &key.id.0
        {
            return Err(Status::data_loss("history FILE identity mismatch").into());
        }
        self.check()?;
        Ok((descriptor, bytes))
    }
}

fn absent() -> q::HistoryPathState {
    q::HistoryPathState {
        kind: PathKind::HistoryPathAbsent as i32,
        native_id: Vec::new(),
        size_bytes: 0,
        executable: false,
    }
}
fn wire(id: m::CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn parse_revision(value: Option<q::CommitId>) -> Result<m::CommitId, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("history revision is required"))?;
    Ok(m::CommitId(value.value.as_slice().try_into().map_err(
        |_| Status::invalid_argument("invalid history revision"),
    )?))
}
fn validate<'a>(
    path: &'a str,
    policy: i32,
    maximum: u32,
    config: &QueryConfig,
) -> Result<(Vec<&'a str>, usize), Status> {
    if policy != q::HistoryPolicy::LinearFixedPathV1 as i32 {
        return Err(Status::unimplemented("unsupported history semantics"));
    }
    if path.is_empty() || path.len() > MAX_PATH_BYTES || path.contains(['\\', '\0']) {
        return Err(Status::invalid_argument(
            "invalid repository-relative history path",
        ));
    }
    let components: Vec<_> = path.split('/').collect();
    if components.len() > config.max_history_path_depth
        || components
            .iter()
            .any(|part| matches!(*part, "" | "." | ".."))
    {
        return Err(Status::invalid_argument(
            "invalid repository-relative history path",
        ));
    }
    let maximum = if maximum == 0 {
        config.max_history_commits
    } else {
        (maximum as usize).min(config.max_history_commits)
    };
    Ok((components, maximum))
}
fn wire_descriptor(value: m::Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        key: Some(p::ObjectKey {
            kind: value.key.kind as i32,
            native_id: value.key.id.0.to_vec(),
        }),
        content_blake3: value.key.id.0.to_vec(),
        size_bytes: value.size_bytes,
        object_format_version: value.format_version as u32,
    }
}
fn finish_failure(failure: Failure) -> Result<Coverage, Status> {
    match failure {
        Failure::Status(status) => Err(status),
        Failure::Boundary(coverage) => Ok(coverage),
    }
}

pub(crate) fn path_history(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::PathHistoryRequest,
    budget: &mut WorkBudget,
    deadline: Instant,
) -> Result<q::PathHistoryResult, Status> {
    let (path, maximum) = validate(&input.path, input.policy, input.max_commits, &host.config)?;
    let revision = parse_revision(input.revision.clone())?;
    let mut result = q::PathHistoryResult {
        scope_digest: scope.wire.content_digest.clone(),
        index_epoch: scope.wire.index_epoch.clone(),
        revision: Some(wire(revision)),
        path: input.path.clone(),
        policy: input.policy,
        coverage: Coverage::HistoryScanLimit as i32,
        scanned_commits: 0,
        entries: Vec::new(),
        boundary: Some(wire(revision)),
    };
    let mut reader = Reader {
        host,
        scope,
        repository,
        budget,
        deadline,
        read_bytes: 0,
    };
    let mut current = revision;
    let mut cached = None;
    while result.scanned_commits < maximum as u32 {
        if current == m::CommitId([0; 32]) {
            // Even the intrinsic root must belong to the accepted logical source.
            match reader.canonical_source(current) {
                Ok(_) => {
                    result.coverage = Coverage::HistoryComplete as i32;
                    result.boundary = None;
                }
                Err(error) => result.coverage = finish_failure(error)? as i32,
            }
            return Ok(result);
        }
        let step = (|| -> Step<(m::CommitId, m::Commit, q::HistoryPathState, q::HistoryPathState)> {
            let (commit, state) = if let Some(value) = cached.take() { value } else {
                let commit = reader.commit(current)?;
                let state = reader.path(&commit, &path)?;
                (commit, state)
            };
            if commit.parents.len() > 1 { return Err(Failure::Boundary(Coverage::HistoryMergeBoundary)); }
            let parent = commit.parents.first().copied().unwrap_or(m::CommitId([0;32]));
            if parent == m::CommitId([0;32]) {
                return Ok((parent, commit, state, absent()));
            }
            let parent_commit = reader.commit(parent)?;
            let parent_state = reader.path(&parent_commit, &path)?;
            cached = Some((parent_commit, parent_state.clone()));
            Ok((parent, commit, state, parent_state))
        })();
        let (parent, _, state, parent_state) = match step {
            Ok(value) => value,
            Err(error) => {
                result.coverage = finish_failure(error)? as i32;
                result.boundary = Some(wire(current));
                return Ok(result);
            }
        };
        result.scanned_commits += 1;
        if state != parent_state {
            result.entries.push(q::PathHistoryEntry {
                commit: Some(wire(current)),
                state: Some(state),
                parent_state: Some(parent_state),
            });
        }
        current = parent;
        result.boundary = Some(wire(current));
        if current == m::CommitId([0; 32]) {
            result.coverage = Coverage::HistoryComplete as i32;
            result.boundary = None;
            return Ok(result);
        }
    }
    Ok(result)
}

fn count_lines(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\n').count()
        + usize::from(bytes.last().is_some_and(|byte| *byte != b'\n'))
}
#[derive(Clone, Copy)]
struct Origin {
    commit: m::CommitId,
    line: usize,
}

pub(crate) fn blame(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::BlameRequest,
    budget: &mut WorkBudget,
    deadline: Instant,
) -> Result<q::BlameResult, Status> {
    let (path, maximum) = validate(&input.path, input.policy, input.max_commits, &host.config)?;
    let revision = parse_revision(input.revision.clone())?;
    let mut result = q::BlameResult {
        scope_digest: scope.wire.content_digest.clone(),
        index_epoch: scope.wire.index_epoch.clone(),
        revision: Some(wire(revision)),
        path: input.path.clone(),
        policy: input.policy,
        coverage: Coverage::HistoryScanLimit as i32,
        scanned_commits: 0,
        boundary: Some(wire(revision)),
        target: None,
        target_bytes: Vec::new(),
        ranges: Vec::new(),
    };
    let mut reader = Reader {
        host,
        scope,
        repository,
        budget,
        deadline,
        read_bytes: 0,
    };
    if revision == m::CommitId([0; 32]) {
        match reader.canonical_source(revision) {
            Ok(_) => {
                return Err(Status::not_found(
                    "history path is absent at intrinsic root",
                ));
            }
            Err(error) => {
                result.coverage = finish_failure(error)? as i32;
                return Ok(result);
            }
        }
    }
    let start = (|| -> Step<_> {
        let commit = reader.commit(revision)?;
        let state = reader.path(&commit, &path)?;
        if state.kind == PathKind::HistoryPathAbsent as i32 {
            return Err(Status::not_found("blame path is absent at selected revision").into());
        }
        if state.kind != PathKind::HistoryPathFile as i32 {
            return Err(Status::failed_precondition(
                "blame requires a regular FILE at selected revision",
            )
            .into());
        }
        let (descriptor, bytes) = reader.file(&state)?;
        Ok((commit, state, descriptor, bytes))
    })();
    let (mut current_commit, mut current_state, target, mut current_bytes) = match start {
        Ok(value) => value,
        Err(error) => {
            result.coverage = finish_failure(error)? as i32;
            return Ok(result);
        }
    };
    let lines = count_lines(&current_bytes);
    if lines > host.config.max_blame_lines {
        result.coverage = Coverage::HistoryDiffLimit as i32;
        return Ok(result);
    }
    result.target = Some(wire_descriptor(target));
    result.target_bytes = current_bytes.clone();
    let mut origins = vec![None; lines];
    // (line at current revision, selected-file line); matching preserves order.
    let mut pending: Vec<(usize, usize)> = (0..lines).map(|line| (line, line)).collect();
    let mut current = revision;
    // Work/input charges are aggregate across history. Allocation charge is
    // per comparison: only one ContentDiff exists at a time and is explicitly
    // dropped before the next comparison, so freed scratch must not consume a
    // history-long residency budget.
    let mut remaining_diff = jj_lib::diff::LineDiffLimits {
        max_input_bytes: host.config.max_history_read_bytes,
        max_lines: host
            .config
            .max_blame_lines
            .saturating_mul(maximum)
            .saturating_mul(2),
        max_work: host.config.max_blame_diff_steps,
        max_allocation_bytes: host.config.max_blame_diff_memory_bytes,
        max_recursion_depth: host.config.max_blame_diff_depth,
    };
    while !pending.is_empty() && result.scanned_commits < maximum as u32 {
        reader.check().map_err(|error| match error {
            Failure::Status(status) => status,
            Failure::Boundary(_) => Status::internal("unexpected history check boundary"),
        })?;
        if current_commit.parents.len() > 1 {
            result.coverage = Coverage::HistoryMergeBoundary as i32;
            break;
        }
        let parent = current_commit
            .parents
            .first()
            .copied()
            .unwrap_or(m::CommitId([0; 32]));
        let parent_source = if parent == m::CommitId([0; 32]) {
            Ok(None)
        } else {
            (|| -> Step<_> {
                let commit = reader.commit(parent)?;
                let state = reader.path(&commit, &path)?;
                Ok(Some((commit, state)))
            })()
        };
        let parent_source = match parent_source {
            Ok(value) => value,
            Err(error) => {
                result.coverage = finish_failure(error)? as i32;
                break;
            }
        };
        let parent_state = parent_source
            .as_ref()
            .map_or_else(absent, |(_, state)| state.clone());
        if parent_state.kind != PathKind::HistoryPathFile as i32 {
            // Pinned jj treats absent/non-file ancestors as empty bytes. A
            // delete/re-add or type transition starts a new attribution lifetime.
            for (line, target_line) in pending.drain(..) {
                origins[target_line] = Some(Origin {
                    commit: current,
                    line,
                });
            }
            result.scanned_commits += 1;
            break;
        }
        if parent_state.native_id == current_state.native_id {
            // Equal canonical FILE IDs imply equal bytes even if mode changed.
            result.scanned_commits += 1;
            let Some((commit, state)) = parent_source else {
                unreachable!()
            };
            current = parent;
            current_commit = commit;
            current_state = state;
            result.boundary = Some(wire(current));
            continue;
        }
        let parent_bytes = match reader.file(&parent_state) {
            Ok((_, bytes)) => bytes,
            Err(error) => {
                result.coverage = finish_failure(error)? as i32;
                break;
            }
        };
        if count_lines(&parent_bytes) > host.config.max_blame_lines {
            result.coverage = Coverage::HistoryDiffLimit as i32;
            break;
        }
        let diff = {
            let mut diff_budget = jj_lib::diff::LineDiffBudget::new(remaining_diff);
            let compared = jj_lib::diff::ContentDiff::by_line_bounded(
                [&current_bytes, &parent_bytes],
                &mut diff_budget,
                || Instant::now() < deadline,
            );
            // Failed attempts also consume their charged work. A failed precharge
            // may exceed the remainder; saturating subtraction exhausts that budget
            // and the request stops below, without another comparison.
            let used = diff_budget.usage();
            remaining_diff.max_input_bytes = remaining_diff
                .max_input_bytes
                .saturating_sub(used.input_bytes);
            remaining_diff.max_lines = remaining_diff.max_lines.saturating_sub(used.lines);
            remaining_diff.max_work = remaining_diff.max_work.saturating_sub(used.work);
            match compared {
                Ok(diff) => diff,
                Err(jj_lib::diff::LineDiffError::Cancelled) => {
                    return Err(Status::deadline_exceeded("blame diff deadline exceeded"));
                }
                Err(jj_lib::diff::LineDiffError::LimitExceeded) => {
                    result.coverage = Coverage::HistoryDiffLimit as i32;
                    break;
                }
            }
        };
        let mut next = Vec::with_capacity(pending.len());
        let mut cursor = 0usize;
        let mut current_line = 0usize;
        let mut parent_line = 0usize;
        for hunk in diff.hunks() {
            let current_count = count_lines(hunk.contents[0]);
            let parent_count = count_lines(hunk.contents[1]);
            let end = current_line + current_count;
            while cursor < pending.len() && pending[cursor].0 < end {
                let (line, target_line) = pending[cursor];
                if line < current_line {
                    return Err(Status::internal("blame line mapping is not monotonic"));
                }
                match hunk.kind {
                    jj_lib::diff::DiffHunkKind::Matching => {
                        next.push((parent_line + line - current_line, target_line))
                    }
                    jj_lib::diff::DiffHunkKind::Different => {
                        origins[target_line] = Some(Origin {
                            commit: current,
                            line,
                        })
                    }
                }
                cursor += 1;
            }
            current_line = end;
            parent_line += parent_count;
        }
        if cursor != pending.len() {
            return Err(Status::internal("blame diff omitted selected lines"));
        }
        drop(diff);
        pending = next;
        current_bytes = parent_bytes;
        let Some((commit, state)) = parent_source else {
            unreachable!()
        };
        current_commit = commit;
        current_state = state;
        current = parent;
        result.scanned_commits += 1;
        result.boundary = Some(wire(current));
    }
    if pending.is_empty() {
        result.coverage = Coverage::HistoryComplete as i32;
        result.boundary = None;
    }
    for (line, origin) in origins.into_iter().enumerate() {
        let matching = result.ranges.last().is_some_and(|range| match origin {
            Some(origin) => {
                range
                    .origin
                    .as_ref()
                    .is_some_and(|id| id.value == origin.commit.0)
                    && range.original_line as usize + (range.end_line - range.start_line) as usize
                        == origin.line
            }
            None => range.origin.is_none(),
        });
        if matching {
            result.ranges.last_mut().unwrap().end_line += 1;
        } else {
            result.ranges.push(q::BlameRange {
                start_line: line as u32,
                end_line: line as u32 + 1,
                origin: origin.map(|origin| wire(origin.commit)),
                original_line: origin.map_or(0, |origin| origin.line as u32),
            });
        }
    }
    Ok(result)
}

// Search receives only sealed children of an authorized canonical revision, or
// children loaded from its integrity-checked derived manifest. These are internal
// provenance tokens, not new client-supplied object-read capabilities.
pub(crate) struct SearchRead<'a> {
    pub index: &'a std::sync::Arc<tinytree_index::Index>,
    pub store: &'a tinytree_store::Store,
    pub metadata_max: usize,
}
#[derive(Clone)]
pub(crate) struct SearchTree {
    id: m::TreeId,
    source: [u8; 32],
    intrinsic: bool,
}
#[derive(Clone)]
pub(crate) struct SearchFile {
    id: m::FileId,
    size: u64,
    source: [u8; 32],
}
pub(crate) enum SearchChild {
    Tree(SearchTree),
    File(SearchFile),
    Other,
}
pub(crate) struct SearchEntry {
    pub name: String,
    pub child: SearchChild,
}
pub(crate) struct SearchManifest {
    root: SearchTree,
    value: tinytree_index::content::ContentManifest,
}
fn search_source(scope: &Scoped, repository: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-search-canonical-source-v1\0");
    hash.update(&(repository.len() as u64).to_be_bytes());
    hash.update(repository.as_bytes());
    hash.update(&scope.wire.content_digest);
    if let Some(key) = &scope.wire.key {
        hash.update(&key.handle);
    }
    *hash.finalize().as_bytes()
}
pub(crate) fn search_root(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    id: m::CommitId,
    budget: &mut WorkBudget,
    deadline: Instant,
) -> Result<SearchTree, Status> {
    let mut reader = Reader {
        host,
        scope,
        repository,
        budget,
        deadline,
        read_bytes: 0,
    };
    let commit = if id == m::CommitId::ROOT {
        reader.canonical_source(id).map_err(search_failure)?;
        m::synthetic_root()
    } else {
        reader.commit(id).map_err(search_failure)?
    };
    if commit.root_tree.terms.len() != 1 {
        return Err(Status::failed_precondition(
            "search requires a resolved canonical tree",
        ));
    }
    Ok(SearchTree {
        id: commit.root_tree.terms[0],
        source: search_source(scope, repository),
        intrinsic: id == m::CommitId::ROOT,
    })
}
fn search_failure(error: Failure) -> Status {
    match error {
        Failure::Status(status) => status,
        Failure::Boundary(Coverage::HistoryPrivateOverlay) => Status::unimplemented(
            "search private metadata-only source has no canonical file authority",
        ),
        Failure::Boundary(_) => {
            Status::resource_exhausted("canonical search source exceeds metadata limits")
        }
    }
}
impl SearchTree {
    pub fn id(&self) -> m::TreeId {
        self.id
    }
    pub fn entries(
        &self,
        read: &SearchRead<'_>,
        scope: &Scoped,
        repository: &str,
        budget: &mut WorkBudget,
        deadline: Instant,
    ) -> Result<Vec<SearchEntry>, Status> {
        if self.source != search_source(scope, repository) {
            return Err(Status::internal("search canonical source mismatch"));
        }
        if self.intrinsic {
            return Ok(Vec::new());
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded("search tree deadline exceeded"));
        }
        let key = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(self.id.0),
        };
        let descriptor = read
            .store
            .descriptor(repository, key)
            .map_err(|_| Status::unavailable("search tree metadata unavailable"))?
            .ok_or_else(|| Status::data_loss("retained search tree missing"))?;
        if descriptor.key != key
            || descriptor.format_version != m::FORMAT_VERSION
            || descriptor.size_bytes > read.metadata_max as u64
        {
            return Err(Status::resource_exhausted(
                "search tree exceeds configured limit",
            ));
        }
        budget
            .charge(1, 0, descriptor.size_bytes)
            .map_err(index_error)?;
        let (actual, bytes) = read
            .store
            .read_metadata(repository, key)
            .map_err(|_| Status::data_loss("search tree read failed"))?;
        if actual != descriptor
            || bytes.len() as u64 != descriptor.size_bytes
            || blake3::hash(&bytes).as_bytes() != &key.id.0
        {
            return Err(Status::data_loss("search tree identity mismatch"));
        }
        let object = m::decode(
            &bytes,
            m::Limits {
                max_object_bytes: read.metadata_max,
                max_items: 65_536,
                max_field_bytes: read.metadata_max,
                max_path_bytes: 4096,
            },
        )
        .map_err(|_| Status::data_loss("invalid search tree"))?;
        let m::Object::Tree(tree) = object else {
            return Err(Status::data_loss("search canonical tree has wrong kind"));
        };
        Ok(tree
            .entries
            .into_iter()
            .map(|entry| SearchEntry {
                name: entry.name,
                child: match entry.value {
                    m::TreeValue::Tree(id) => SearchChild::Tree(SearchTree {
                        id,
                        source: self.source,
                        intrinsic: false,
                    }),
                    m::TreeValue::File { id, size_bytes, .. } => SearchChild::File(SearchFile {
                        id,
                        size: size_bytes,
                        source: self.source,
                    }),
                    _ => SearchChild::Other,
                },
            })
            .collect())
    }
    pub fn manifest(
        &self,
        read: &SearchRead<'_>,
        scope: &Scoped,
        repository: &str,
        limits: &tinytree_index::content::ContentLimits,
    ) -> Result<Option<SearchManifest>, Status> {
        if self.source != search_source(scope, repository) {
            return Err(Status::internal("search canonical source mismatch"));
        }
        read.index
            .content_manifest(repository, self.id, limits)
            .map_err(|_| Status::unavailable("search derived manifest unavailable"))?
            .map(|value| {
                if value.root != self.id {
                    return Err(Status::data_loss("search manifest root mismatch"));
                }
                Ok(SearchManifest {
                    root: self.clone(),
                    value,
                })
            })
            .transpose()
    }
}
impl SearchManifest {
    pub fn value(&self) -> &tinytree_index::content::ContentManifest {
        &self.value
    }
    pub fn file(&self, index: usize) -> Option<SearchFile> {
        self.value.entries.get(index).map(|entry| SearchFile {
            id: entry.file,
            size: entry.size,
            source: self.root.source,
        })
    }
}
impl SearchFile {
    pub fn id(&self) -> m::FileId {
        self.id
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn read(
        &self,
        read: &SearchRead<'_>,
        scope: &Scoped,
        repository: &str,
        maximum: usize,
        budget: &mut WorkBudget,
        deadline: Instant,
    ) -> Result<Vec<u8>, Status> {
        if self.source != search_source(scope, repository) {
            return Err(Status::internal("search canonical source mismatch"));
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded("search source deadline exceeded"));
        }
        let size = usize::try_from(self.size)
            .map_err(|_| Status::resource_exhausted("search file exceeds size limit"))?;
        if size > maximum {
            return Err(Status::resource_exhausted("search file exceeds size limit"));
        }
        // Store::read verifies the whole member and can reconstruct a packed
        // member before returning. Admission therefore covers the entire FILE.
        budget.charge(1, 0, self.size).map_err(index_error)?;
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(self.id.0),
        };
        let descriptor = read
            .store
            .descriptor(repository, key)
            .map_err(|_| Status::unavailable("search FILE metadata unavailable"))?
            .ok_or_else(|| Status::data_loss("retained search FILE missing"))?;
        if descriptor.key != key
            || descriptor.size_bytes != self.size
            || descriptor.format_version != m::FORMAT_VERSION
        {
            return Err(Status::data_loss(
                "search canonical FILE descriptor mismatch",
            ));
        }
        let mut lease = read
            .store
            .read(repository, key)
            .map_err(|_| Status::data_loss("search FILE read failed"))?;
        if lease.descriptor() != descriptor {
            return Err(Status::data_loss("search FILE descriptor changed"));
        }
        let mut bytes = vec![0; size];
        lease
            .read_exact(&mut bytes)
            .map_err(|_| Status::data_loss("truncated search FILE"))?;
        let mut tail = [0];
        if lease
            .read(&mut tail)
            .map_err(|_| Status::data_loss("search FILE read failed"))?
            != 0
            || m::file_id(&bytes) != self.id
        {
            return Err(Status::data_loss("search FILE identity mismatch"));
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded("search source deadline exceeded"));
        }
        Ok(bytes)
    }
}
