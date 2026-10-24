//! Revision-pinned path queries. Private file bodies stay in the local provider;
//! only an observed or exactly retained canonical baseline is queried remotely.
use crate::{broker, query_source};
use futures::io::AsyncReadExt as _;
use jj_cli::{
    cli_util::{CommandHelper, RevisionArg},
    command_error::{CommandError, cli_error, user_error},
    ui::Ui,
};
use jj_lib::{
    backend::{FileId, TreeValue},
    commit::Commit,
    diff::{ContentDiff, DiffHunkKind, LineDiffBudget, LineDiffLimits},
    object_id::ObjectId as _,
    repo::Repo as _,
    repo_path::RepoPath,
    store::Store,
};
use native_broker::{
    BrokerQueryClient, NativeQueryIo, QueryScope, ScopedPathQuery, query_protocol as q,
};
use serde::Serialize;
use std::{
    io::Write as _,
    sync::Arc,
    time::{Duration, Instant},
};
use tinytree_model::CommitId;

const MAX_FILE_BYTES: usize = 512 * 1024;
const MAX_LINES: usize = 16_384;
const MAX_LOCAL_BYTES: usize = 32 * 1024 * 1024;
const MAX_DIFF_WORK: usize = 8_000_000;
const MAX_COMMAND_TIME: Duration = Duration::from_secs(30);

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct PathArgs {
    /// Exact file path, interpreted relative to the current directory.
    pub path: String,
    #[arg(short, long, value_name = "REVSET")]
    pub revision: Option<RevisionArg>,
    /// Bound local draft traversal and server history work; never raises server limits.
    #[arg(long, default_value_t = 256)]
    pub max_commits: u32,
    #[arg(long)]
    pub json: bool,
}

fn failure(error: impl std::fmt::Display) -> CommandError {
    user_error(format!("Native path query failed: {error}"))
}
fn native_id(commit: &Commit) -> Result<CommitId, CommandError> {
    CommitId::from_slice(commit.id().as_bytes()).map_err(failure)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn random() -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes
}
fn count_lines(bytes: &[u8]) -> usize {
    bytes.split_inclusive(|byte| *byte == b'\n').count()
}
fn check_time(started: Instant) -> Result<(), CommandError> {
    if started.elapsed() >= MAX_COMMAND_TIME {
        Err(user_error("native path query elapsed budget exceeded"))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct State {
    kind: &'static str,
    id: Option<String>,
    executable: bool,
    // Local tree entries contain a FileId, not a size. Do not hydrate their
    // bytes merely to render history metadata.
    size_bytes: Option<u64>,
    #[serde(skip)]
    file: Option<FileId>,
}
impl State {
    fn absent() -> Self {
        Self {
            kind: "absent",
            id: None,
            executable: false,
            size_bytes: None,
            file: None,
        }
    }
    fn same_entry(&self, other: &Self) -> bool {
        self.kind == other.kind && self.id == other.id && self.executable == other.executable
    }
    fn wire(value: &q::HistoryPathState) -> Result<Self, CommandError> {
        let kind = match q::HistoryPathKind::try_from(value.kind).map_err(failure)? {
            q::HistoryPathKind::HistoryPathAbsent => "absent",
            q::HistoryPathKind::HistoryPathFile => "file",
            q::HistoryPathKind::HistoryPathSymlink => "symlink",
            q::HistoryPathKind::HistoryPathTree => "tree",
            _ => return Err(user_error("invalid native history path state")),
        };
        Ok(Self {
            kind,
            id: (kind != "absent").then(|| hex(&value.native_id)),
            executable: value.executable,
            size_bytes: (kind != "absent").then_some(value.size_bytes),
            file: None,
        })
    }
}
async fn path_state(commit: &Commit, path: &RepoPath) -> Result<State, CommandError> {
    if !commit.tree_ids().is_resolved() {
        return Err(user_error(
            "native path query does not support conflicted trees",
        ));
    }
    let value = commit.tree().path_value(path).await?;
    let Some(value) = value.as_resolved() else {
        return Err(user_error(
            "native path query does not support conflicted paths",
        ));
    };
    let Some(value) = value else {
        return Ok(State::absent());
    };
    let (kind, id, executable, file) = match value {
        TreeValue::File { id, executable, .. } => ("file", id.hex(), *executable, Some(id.clone())),
        TreeValue::Symlink(id) => ("symlink", id.hex(), false, None),
        TreeValue::Tree(id) => ("tree", id.hex(), false, None),
        TreeValue::GitSubmodule(_) => {
            return Err(user_error("native path query does not support submodules"));
        }
    };
    Ok(State {
        kind,
        id: Some(id),
        executable,
        size_bytes: None,
        file,
    })
}

struct PrivateStep {
    id: CommitId,
    state: State,
    parent_state: State,
}
struct CapturedPath {
    target: State,
    steps: Vec<PrivateStep>,
    baseline: CommitId,
    baseline_state: State,
}
async fn remote_member(
    query: &BrokerQueryClient,
    scope: &QueryScope,
    id: CommitId,
) -> Result<bool, CommandError> {
    let reply = query
        .facts(
            scope.handle,
            vec![q::GraphFact {
                fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                    commit: Some(q::CommitId {
                        value: id.0.to_vec(),
                    }),
                })),
            }],
            Arc::new(()),
        )
        .await
        .map_err(failure)?;
    match reply.value.answers.as_slice() {
        [answer] => match answer.answer.as_ref() {
            Some(q::graph_fact_answer::Answer::HasId(value)) => Ok(*value),
            _ => Err(user_error("invalid native membership answer")),
        },
        _ => Err(user_error("invalid native membership answer count")),
    }
}
async fn capture_path(
    query: &BrokerQueryClient,
    scope: &QueryScope,
    mut current: Commit,
    path: &RepoPath,
    max_commits: u32,
    started: Instant,
) -> Result<CapturedPath, CommandError> {
    let target = path_state(&current, path).await?;
    let mut state = target.clone();
    let mut steps = Vec::new();
    loop {
        check_time(started)?;
        let id = native_id(&current)?;
        if remote_member(query, scope, id).await? {
            return Ok(CapturedPath {
                target,
                steps,
                baseline: id,
                baseline_state: state,
            });
        }
        if steps.len() >= max_commits as usize {
            return Err(user_error("native private path history scan limit reached"));
        }
        let [parent_id] = current.parent_ids() else {
            return Err(user_error(
                "native private path query requires resolved linear ancestry",
            ));
        };
        let parent = current.store().get_commit_async(parent_id).await?;
        let parent_state = path_state(&parent, path).await?;
        steps.push(PrivateStep {
            id,
            state,
            parent_state: parent_state.clone(),
        });
        current = parent;
        state = parent_state;
    }
}

fn input(scope: &QueryScope, revision: CommitId, path: &str, max_commits: u32) -> ScopedPathQuery {
    ScopedPathQuery {
        scope: scope.handle,
        revision,
        path: path.to_owned(),
        policy: q::HistoryPolicy::LinearFixedPathV1,
        max_commits,
    }
}
fn coverage(value: i32) -> Result<&'static str, CommandError> {
    Ok(
        match q::HistoryCoverage::try_from(value).map_err(failure)? {
            q::HistoryCoverage::HistoryComplete => "complete",
            q::HistoryCoverage::HistoryScanLimit => "scan_limit",
            q::HistoryCoverage::HistoryMergeBoundary => "merge_boundary",
            q::HistoryCoverage::HistoryConflictBoundary => "conflict_boundary",
            q::HistoryCoverage::HistoryPrivateOverlay => "private_overlay",
            q::HistoryCoverage::HistoryReadLimit => "read_limit",
            q::HistoryCoverage::HistoryDiffLimit => "diff_limit",
            _ => return Err(user_error("invalid native history coverage")),
        },
    )
}

#[derive(Serialize)]
struct HistoryEntry {
    commit: String,
    state: State,
    parent_state: State,
    local: bool,
}
#[derive(Serialize)]
struct HistoryOutput {
    version: u32,
    revision: String,
    path: String,
    policy: &'static str,
    coverage: &'static str,
    local_commits: usize,
    remote_scanned_commits: u32,
    entries: Vec<HistoryEntry>,
}
async fn history(
    ui: &mut Ui,
    query: &BrokerQueryClient,
    scope: &QueryScope,
    captured: &CapturedPath,
    revision: CommitId,
    path: &str,
    args: &PathArgs,
) -> Result<(), CommandError> {
    let remote = query
        .path_history(
            input(scope, captured.baseline, path, args.max_commits),
            Arc::new(()),
        )
        .await
        .map_err(failure)?;
    let mut entries = captured
        .steps
        .iter()
        .filter(|step| !step.state.same_entry(&step.parent_state))
        .map(|step| HistoryEntry {
            commit: hex(&step.id.0),
            state: step.state.clone(),
            parent_state: step.parent_state.clone(),
            local: true,
        })
        .collect::<Vec<_>>();
    for entry in &remote.value.entries {
        entries.push(HistoryEntry {
            commit: hex(&entry
                .commit
                .as_ref()
                .ok_or_else(|| user_error("missing history commit"))?
                .value),
            state: State::wire(
                entry
                    .state
                    .as_ref()
                    .ok_or_else(|| user_error("missing history state"))?,
            )?,
            parent_state: State::wire(
                entry
                    .parent_state
                    .as_ref()
                    .ok_or_else(|| user_error("missing history parent state"))?,
            )?,
            local: false,
        });
    }
    let output = HistoryOutput {
        version: 1,
        revision: hex(&revision.0),
        path: path.to_owned(),
        policy: "linear_fixed_path_v1",
        coverage: coverage(remote.value.coverage)?,
        local_commits: captured.steps.len(),
        remote_scanned_commits: remote.value.scanned_commits,
        entries,
    };
    if args.json {
        serde_json::to_writer(ui.stdout(), &output).map_err(failure)?;
        writeln!(ui.stdout())?;
    } else {
        for entry in &output.entries {
            writeln!(
                ui.stdout(),
                "{} {} -> {} {}",
                &entry.commit[..12],
                entry.parent_state.kind,
                entry.state.kind,
                path
            )?;
        }
        writeln!(
            ui.stdout(),
            "Coverage: {} ({} local, {} remote commits)",
            output.coverage,
            output.local_commits,
            output.remote_scanned_commits
        )?;
    }
    // The remote QueryReply owner intentionally survives all decoding/rendering.
    drop(remote);
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
struct Origin {
    commit: String,
    original_line: usize,
}
#[derive(Serialize)]
struct AnnotatedLine {
    line: usize,
    origin: Option<Origin>,
    bytes: Vec<u8>,
}
#[derive(Serialize)]
struct BlameOutput {
    version: u32,
    revision: String,
    path: String,
    policy: &'static str,
    coverage: &'static str,
    local_commits: usize,
    remote_scanned_commits: u32,
    target_id: String,
    lines: Vec<AnnotatedLine>,
}
async fn read_bytes(
    store: &Store,
    path: &RepoPath,
    state: &State,
    total: &mut usize,
    started: Instant,
) -> Result<Vec<u8>, CommandError> {
    check_time(started)?;
    let Some(id) = &state.file else {
        return Ok(Vec::new());
    };
    let reader = store.read_file(path, id).await?;
    let mut bytes = Vec::new();
    reader
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(failure)?;
    *total = total
        .checked_add(bytes.len())
        .ok_or_else(|| user_error("local history byte budget overflow"))?;
    if bytes.len() > MAX_FILE_BYTES || *total > MAX_LOCAL_BYTES || count_lines(&bytes) > MAX_LINES {
        return Err(user_error("native local history content limit reached"));
    }
    Ok(bytes)
}
// Mapping uses the same current->parent orientation and matching-run line
// counts as pinned jj's FileAnnotator. Unmatched lines originate at current.
fn propagate(
    current: &[u8],
    parent: &[u8],
    commit: CommitId,
    pending: &mut Vec<(usize, usize)>,
    origins: &mut [Option<Origin>],
    budget: &mut LineDiffBudget,
    started: Instant,
) -> Result<(), CommandError> {
    let diff = ContentDiff::by_line_bounded([current, parent], budget, || {
        started.elapsed() < MAX_COMMAND_TIME
    })
    .map_err(failure)?;
    let mut mappings = Vec::new();
    let mut current_line = 0;
    let mut parent_line = 0;
    let mut next = 0;
    for hunk in diff.hunks() {
        let count = count_lines(hunk.contents[0]);
        if hunk.kind == DiffHunkKind::Matching {
            while next < pending.len() && pending[next].0 < current_line + count {
                let (line, original) = pending[next];
                if line >= current_line {
                    mappings.push((parent_line + line - current_line, original));
                } else {
                    origins[original] = Some(Origin {
                        commit: hex(&commit.0),
                        original_line: line + 1,
                    });
                }
                next += 1;
            }
        } else {
            while next < pending.len() && pending[next].0 < current_line + count {
                let (line, original) = pending[next];
                origins[original] = Some(Origin {
                    commit: hex(&commit.0),
                    original_line: line + 1,
                });
                next += 1;
            }
        }
        current_line += count;
        parent_line += count_lines(hunk.contents[1]);
    }
    if next != pending.len() {
        return Err(user_error("native annotation line mapping is inconsistent"));
    }
    *pending = mappings;
    Ok(())
}
async fn blame(
    ui: &mut Ui,
    query: &BrokerQueryClient,
    scope: &QueryScope,
    captured: &CapturedPath,
    revision: CommitId,
    path: &RepoPath,
    store: &Store,
    args: &PathArgs,
    started: Instant,
) -> Result<(), CommandError> {
    if captured.target.kind != "file" {
        return Err(user_error(
            "native blame requires an existing resolved regular file",
        ));
    }
    let path_text = path.as_internal_file_string();
    // Non-file ancestors terminate this fixed path's lifetime. Never mistake an
    // upstream NotFound (which may mean missing canonical data) for an empty file.
    let remote = if captured.baseline_state.kind == "file" {
        Some(
            query
                .blame(
                    input(scope, captured.baseline, path_text, args.max_commits),
                    Arc::new(()),
                )
                .await
                .map_err(failure)?,
        )
    } else {
        None
    };
    if let Some(reply) = &remote {
        // An incomplete response may legitimately omit an over-budget target.
        // Empty bytes in that response are not an empty regular file. Require
        // the selected canonical FileId before using any baseline attribution.
        let descriptor = reply.value.target.as_ref().ok_or_else(|| {
            user_error(format!(
                "native blame baseline content unavailable ({})",
                coverage(reply.value.coverage).unwrap_or("invalid_coverage")
            ))
        })?;
        let key = descriptor
            .key
            .as_ref()
            .ok_or_else(|| user_error("native blame baseline descriptor is missing its key"))?;
        let actual = tinytree_model::describe_file(&reply.value.target_bytes);
        if key.kind != tinytree_model::Kind::File as i32
            || key.native_id.as_slice() != actual.key.id.0
            || descriptor.content_blake3.as_slice() != actual.key.id.0
            || descriptor.size_bytes != actual.size_bytes
            || descriptor.object_format_version != tinytree_model::FORMAT_VERSION as u32
            || captured.baseline_state.id.as_deref() != Some(hex(&actual.key.id.0).as_str())
        {
            return Err(user_error(
                "native blame baseline does not match the selected canonical file",
            ));
        }
    }
    let baseline_bytes = remote
        .as_ref()
        .map_or(&[][..], |reply| reply.value.target_bytes.as_slice());
    if baseline_bytes.len() > MAX_FILE_BYTES || count_lines(baseline_bytes) > MAX_LINES {
        return Err(user_error(
            "native blame baseline exceeds local content limit",
        ));
    }
    let mut total = baseline_bytes.len();
    let target =
        if captured.target.id == captured.baseline_state.id && captured.target.kind == "file" {
            baseline_bytes.to_vec()
        } else {
            read_bytes(store, path, &captured.target, &mut total, started).await?
        };
    let mut current = target.clone();
    let mut origins = vec![None; count_lines(&target)];
    let mut pending = (0..origins.len())
        .map(|line| (line, line))
        .collect::<Vec<_>>();
    let mut budget = LineDiffBudget::new(LineDiffLimits {
        max_input_bytes: MAX_LOCAL_BYTES,
        max_lines: MAX_LINES * 256,
        max_work: MAX_DIFF_WORK,
        max_allocation_bytes: 32 * 1024 * 1024,
        max_recursion_depth: 64,
    });
    for step in &captured.steps {
        check_time(started)?;
        if pending.is_empty() {
            break;
        }
        if step.state.id == step.parent_state.id && step.state.kind == step.parent_state.kind {
            continue;
        }
        let parent = if step.parent_state.kind == "file"
            && step.parent_state.id == captured.baseline_state.id
        {
            baseline_bytes.to_vec()
        } else {
            read_bytes(store, path, &step.parent_state, &mut total, started).await?
        };
        propagate(
            &current,
            &parent,
            step.id,
            &mut pending,
            &mut origins,
            &mut budget,
            started,
        )?;
        current = parent;
    }
    let mut result_coverage = "complete";
    if !pending.is_empty() {
        let remote = remote.as_ref().ok_or_else(|| {
            user_error("native annotation reached a non-file boundary with unresolved lines")
        })?;
        result_coverage = coverage(remote.value.coverage)?;
        let mut range_index = 0;
        for (line, starting_line) in pending {
            while range_index < remote.value.ranges.len()
                && remote.value.ranges[range_index].end_line as usize <= line
            {
                range_index += 1;
            }
            let range = remote
                .value
                .ranges
                .get(range_index)
                .ok_or_else(|| user_error("native annotation baseline range is missing"))?;
            if line < range.start_line as usize {
                return Err(user_error("native annotation baseline range has a gap"));
            }
            origins[starting_line] = range.origin.as_ref().map(|origin| Origin {
                commit: hex(&origin.value),
                original_line: range.original_line as usize + line - range.start_line as usize + 1,
            });
        }
    }
    if origins.iter().any(Option::is_none) && result_coverage == "complete" {
        return Err(user_error(
            "complete native annotation contains unresolved origins",
        ));
    }
    let output = BlameOutput {
        version: 1,
        revision: hex(&revision.0),
        path: path_text.to_owned(),
        policy: "linear_fixed_path_v1",
        coverage: result_coverage,
        local_commits: captured.steps.len(),
        remote_scanned_commits: remote
            .as_ref()
            .map_or(0, |reply| reply.value.scanned_commits),
        target_id: captured.target.id.clone().unwrap(),
        lines: target
            .split_inclusive(|byte| *byte == b'\n')
            .enumerate()
            .map(|(line, bytes)| AnnotatedLine {
                line: line + 1,
                origin: origins[line].clone(),
                bytes: bytes.to_vec(),
            })
            .collect(),
    };
    if args.json {
        serde_json::to_writer(ui.stdout(), &output).map_err(failure)?;
        writeln!(ui.stdout())?;
    } else {
        for line in &output.lines {
            let id = line
                .origin
                .as_ref()
                .map_or("????????????", |origin| &origin.commit[..12]);
            write!(ui.stdout(), "{id} {:>5} ", line.line)?;
            ui.stdout().write_all(&line.bytes)?;
            if !line.bytes.ends_with(b"\n") {
                writeln!(ui.stdout())?;
            }
        }
        if output.coverage != "complete" {
            writeln!(
                ui.stdout(),
                "Coverage: {} (unresolved lines are marked ?)",
                output.coverage
            )?;
        }
    }
    drop(remote);
    Ok(())
}

pub(crate) async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    args: PathArgs,
    annotate: bool,
) -> Result<(), CommandError> {
    // jj's CLI runner uses pollster rather than a Tokio executor. Drive timers
    // explicitly for this command; broker transport still uses its owned runtime.
    if tokio::runtime::Handle::try_current().is_ok() {
        return run_query(ui, command, args, annotate).await;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(failure)?;
    runtime.block_on(run_query(ui, command, args, annotate))
}

async fn run_query(
    ui: &mut Ui,
    command: &CommandHelper,
    args: PathArgs,
    annotate: bool,
) -> Result<(), CommandError> {
    if args.max_commits == 0 || args.max_commits > 4096 {
        return Err(cli_error("--max-commits must be between 1 and 4096"));
    }
    // Snapshot through the supported native working-copy implementation. Its
    // changed data remains local; no query operation publishes private objects.
    let helper = command.workspace_helper(ui).await?;
    // The ordinary snapshot is a separate supported command phase. All query
    // work after it shares one absolute deadline, including local object reads.
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + MAX_COMMAND_TIME;
    let selected = tokio::time::timeout_at(
        deadline,
        helper.resolve_single_rev(ui, args.revision.as_ref().unwrap_or(&RevisionArg::AT)),
    )
    .await
    .map_err(|_| user_error("native path query deadline exceeded"))??;
    let revision = native_id(&selected)?;
    let path = helper.parse_file_path(&args.path)?;
    let path_text = path.as_internal_file_string();
    if path_text.is_empty() || path_text.len() > 4096 || path_text.contains('\\') {
        return Err(cli_error(
            "native path query requires a nonempty repository-relative file path",
        ));
    }
    let loader = command.workspace_loader()?;
    let locator = broker::locator(&loader.repo_path().join("store"))
        .map_err(|_| user_error("native path queries require a broker-backed workspace"))?;
    let query_source = query_source::Source::resolve(
        helper.repo().readonly_index(),
        &crate::vfs::working_copy_path(loader.workspace_root()).map_err(failure)?,
        &locator,
    )
    .map_err(failure)?;
    let query = tokio::time::timeout_at(
        deadline,
        BrokerQueryClient::connect(&locator.socket, locator.store, Default::default()),
    )
    .await
    .map_err(|_| user_error("native path query deadline exceeded"))?
    .map_err(failure)?;
    let query = if let Some(token) = crate::commands::token(loader.repo_path()).map_err(failure)? {
        query.with_command_token(token)
    } else {
        query
    };
    let cleanup = query.clone();
    let query = query.with_operation_deadline(deadline);
    let scope = query_source.open(&query, random()).await.map_err(failure)?;
    let result = tokio::time::timeout_at(deadline, async {
        let captured = capture_path(
            &query,
            &scope.value,
            selected,
            &path,
            args.max_commits,
            started,
        )
        .await?;
        if annotate {
            blame(
                ui,
                &query,
                &scope.value,
                &captured,
                revision,
                &path,
                helper.repo().store(),
                &args,
                started,
            )
            .await
        } else {
            history(
                ui,
                &query,
                &scope.value,
                &captured,
                revision,
                path_text,
                &args,
            )
            .await
        }
    })
    .await
    .map_err(|_| user_error("native path query deadline exceeded"))
    .and_then(|result| result);
    // Cleanup has an independent bounded allowance. It cannot renew the data
    // deadline, and logical close is never proof that accepted work rolled back.
    let closed = cleanup
        .with_operation_deadline(tokio::time::Instant::now() + Duration::from_secs(5))
        .close_scope(scope.value.handle, Arc::new(()))
        .await
        .map_err(failure);
    drop(scope);
    result?;
    closed?;
    Ok(())
}
