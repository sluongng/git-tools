//! Exact selected-tree search. Clean paths stay remote; changed canonical
//! branches are inspected locally without publishing private objects.
use crate::{broker, query_source};
use futures::io::AsyncReadExt as _;
use jj_cli::{
    cli_util::{CommandHelper, RevisionArg},
    command_error::{CommandError, cli_error, user_error},
    ui::Ui,
};
use jj_lib::{commit::Commit, object_id::ObjectId as _};
use jj_native_backend::NativeObjectIo as _;
use native_broker::{
    BrokerObjectIo, BrokerQueryClient, NativeQueryIo, QueryOwner, QueryScope, ScopedSearchQuery,
    query_protocol as q,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    sync::{Arc, Mutex},
    time::Duration,
};
use tinytree_model as m;
use tinytree_search::{MatchBudget, MatchLimits, Pattern, SearchMode};
use tokio::time::Instant;

const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_PATH: usize = 4096;
const MAX_METADATA: usize = 1024 * 1024;
const MAX_METADATA_CHARGE: usize = 128 * 1024 * 1024;
const MAX_PATHS: usize = 4096;
const MAX_FILE: usize = 1024 * 1024;
const MAX_FILES_BYTES: usize = 16 * 1024 * 1024;
const MAX_PAGES: usize = 512;
const MAX_REMOTE_HITS: usize = 65_536;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct SearchArgs {
    /// Case-sensitive, line-oriented regular expression; inline flags are supported.
    pub pattern: String,
    /// Match the pattern literally instead of interpreting regular expression syntax.
    #[arg(short = 'F', long)]
    pub fixed_strings: bool,
    /// Exact repository directory prefix or complete file path.
    #[arg(long, default_value = "")]
    pub path_prefix: String,
    #[arg(short, long, value_name = "REVSET")]
    pub revision: Option<RevisionArg>,
    #[arg(long, default_value_t = 128)]
    pub limit: usize,
    #[arg(long)]
    pub json: bool,
}
fn failure(error: impl std::fmt::Display) -> CommandError {
    user_error(format!("Native search failed: {error}"))
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
fn check(deadline: Instant) -> Result<(), CommandError> {
    if Instant::now() >= deadline {
        Err(user_error("native search deadline exceeded"))
    } else {
        Ok(())
    }
}
fn matches_prefix(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}
fn root_tree(commit: &Commit) -> Result<m::TreeId, CommandError> {
    let tree = commit
        .tree_ids()
        .as_resolved()
        .ok_or_else(|| user_error("native search requires a resolved selected tree"))?;
    m::TreeId::from_slice(tree.as_bytes()).map_err(failure)
}
#[derive(Default)]
struct Usage {
    metadata_charge: usize,
    objects: usize,
    file_bytes: usize,
}
struct PhysicalBudget {
    usage: Mutex<Usage>,
    _scope: QueryOwner,
}
struct Source {
    io: BrokerObjectIo,
    owner: Arc<PhysicalBudget>,
    deadline: Instant,
}
impl Source {
    async fn metadata(&self, key: m::ObjectKey) -> Result<m::Object, CommandError> {
        check(self.deadline)?;
        let descriptor = self
            .io
            .descriptor_with_owner(key, self.owner.clone())
            .await
            .map_err(failure)?
            .ok_or_else(|| user_error("native search canonical metadata is missing"))?;
        if descriptor.key != key
            || descriptor.format_version != m::FORMAT_VERSION
            || descriptor.size_bytes > MAX_METADATA as u64
        {
            return Err(user_error(
                "native search metadata descriptor exceeds limits",
            ));
        }
        // Charge possible path expansion before a wide tree is decoded. The
        // counters are cumulative and owned by physical reads after cancellation.
        let factor = if key.kind == m::Kind::Tree {
            MAX_PATH + 16
        } else {
            16
        };
        let charge = (descriptor.size_bytes as usize)
            .checked_mul(factor)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| user_error("native search metadata budget overflow"))?;
        {
            let mut work = self.owner.usage.lock().map_err(failure)?;
            work.objects = work
                .objects
                .checked_add(1)
                .ok_or_else(|| user_error("native search object budget overflow"))?;
            work.metadata_charge = work
                .metadata_charge
                .checked_add(charge)
                .ok_or_else(|| user_error("native search metadata budget overflow"))?;
            if work.objects > 4096 || work.metadata_charge > MAX_METADATA_CHARGE {
                return Err(user_error(
                    "native search metadata/path expansion limit reached",
                ));
            }
        }
        let value = self
            .io
            .read_metadata_with_owner(key, self.owner.clone())
            .await
            .map_err(failure)?;
        if value.descriptor() != descriptor {
            return Err(user_error("native search canonical descriptor changed"));
        }
        check(self.deadline)?;
        Ok(value.into_object())
    }
    async fn commit(&self, id: m::CommitId) -> Result<m::Commit, CommandError> {
        if id == m::CommitId::ROOT {
            return Ok(m::synthetic_root());
        }
        match self
            .metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id.0),
            })
            .await?
        {
            m::Object::Commit(value) => Ok(value),
            _ => Err(user_error("native search source is not a commit")),
        }
    }
    async fn tree(&self, id: Option<m::TreeId>) -> Result<m::Tree, CommandError> {
        let Some(id) = id else {
            return Ok(m::Tree::default());
        };
        match self
            .metadata(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            })
            .await?
        {
            m::Object::Tree(value) => Ok(value),
            _ => Err(user_error("native search source is not a tree")),
        }
    }
    async fn file(&self, id: m::FileId, size: u64) -> Result<Vec<u8>, CommandError> {
        check(self.deadline)?;
        if size > MAX_FILE as u64 {
            return Err(user_error("native search local file size limit reached"));
        }
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        let descriptor = self
            .io
            .descriptor_with_owner(key, self.owner.clone())
            .await
            .map_err(failure)?
            .ok_or_else(|| user_error("native search local file is unavailable"))?;
        if descriptor.key != key
            || descriptor.size_bytes != size
            || descriptor.format_version != m::FORMAT_VERSION
        {
            return Err(user_error("native search local FILE descriptor mismatch"));
        }
        {
            let mut work = self.owner.usage.lock().map_err(failure)?;
            work.file_bytes = work
                .file_bytes
                .checked_add(size as usize)
                .ok_or_else(|| user_error("native search file budget overflow"))?;
            if work.file_bytes > MAX_FILES_BYTES {
                return Err(user_error("native search local content byte limit reached"));
            }
        }
        let read = self
            .io
            .read_file_with_owner(id, self.owner.clone())
            .await
            .map_err(failure)?;
        let (actual, reader) = read.into_parts();
        if actual != descriptor {
            return Err(user_error("native search local FILE identity mismatch"));
        }
        let mut bytes = Vec::new();
        reader
            .take(size + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(failure)?;
        if bytes.len() as u64 != size || m::file_id(&bytes) != id {
            return Err(user_error("native search local FILE verification failed"));
        }
        check(self.deadline)?;
        Ok(bytes)
    }
}
async fn member(
    query: &BrokerQueryClient,
    scope: &QueryScope,
    id: m::CommitId,
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
            _ => Err(user_error("native search membership response is invalid")),
        },
        _ => Err(user_error("native search membership count is invalid")),
    }
}
async fn baseline(
    source: &Source,
    query: &BrokerQueryClient,
    scope: &QueryScope,
    mut id: m::CommitId,
) -> Result<(m::CommitId, m::TreeId), CommandError> {
    for _ in 0..256 {
        check(source.deadline)?;
        if member(query, scope, id).await? {
            // Fetch canonical metadata through the already captured scope so a
            // graph membership answer is not treated as standalone read authority.
            let commit = if id == m::CommitId::ROOT {
                m::synthetic_root()
            } else {
                {
                    let mut work = source.owner.usage.lock().map_err(failure)?;
                    work.metadata_charge = work
                        .metadata_charge
                        .checked_add(MAX_METADATA * 16 + 4096)
                        .ok_or_else(|| user_error("native search baseline budget overflow"))?;
                    if work.metadata_charge > MAX_METADATA_CHARGE {
                        return Err(user_error("native search baseline memory limit reached"));
                    }
                }
                let reply = query
                    .read_commit(scope.handle, id, source.owner.clone())
                    .await
                    .map_err(failure)?;
                match reply.value.into_object() {
                    m::Object::Commit(value) => value,
                    _ => return Err(user_error("native search baseline is not canonical COMMIT")),
                }
            };
            let [tree] = commit.root_tree.terms.as_slice() else {
                return Err(user_error("native search baseline tree is conflicted"));
            };
            return Ok((id, *tree));
        }
        let commit = source.commit(id).await?;
        let [parent] = commit.parents.as_slice() else {
            return Err(user_error(
                "native search private baseline discovery requires linear ancestry",
            ));
        };
        id = *parent;
    }
    Err(user_error("native search private ancestry limit reached"))
}
struct Overlay {
    suppressed: BTreeSet<String>,
    files: BTreeMap<String, (m::FileId, u64)>,
}
// Canonical paired-tree merge skips identical subtree IDs. It charges direct
// directory bytes before decoding, including unchanged entries of wide trees.
async fn overlay(
    source: &Source,
    before: m::TreeId,
    after: m::TreeId,
) -> Result<Overlay, CommandError> {
    let mut result = Overlay {
        suppressed: BTreeSet::new(),
        files: BTreeMap::new(),
    };
    let mut pending = vec![(String::new(), Some(before), Some(after), 0usize)];
    while let Some((prefix, before, after, depth)) = pending.pop() {
        check(source.deadline)?;
        if before == after {
            continue;
        }
        if depth >= 128 {
            return Err(user_error("native search local tree depth limit reached"));
        }
        let left = source.tree(before).await?;
        let right = source.tree(after).await?;
        let mut entries: BTreeMap<String, (Option<m::TreeValue>, Option<m::TreeValue>)> =
            BTreeMap::new();
        for entry in left.entries {
            entries.entry(entry.name).or_default().0 = Some(entry.value);
        }
        for entry in right.entries {
            entries.entry(entry.name).or_default().1 = Some(entry.value);
        }
        for (name, (before, after)) in entries {
            if before == after {
                continue;
            }
            // Search is content-only. Mode/copy metadata changes with the
            // exact same canonical FILE identity and length preserve all hits.
            if matches!((&before, &after),
                (Some(m::TreeValue::File { id: left, size_bytes: a, .. }),
                 Some(m::TreeValue::File { id: right, size_bytes: b, .. })) if left == right && a == b)
            {
                continue;
            }
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if path.len() > MAX_PATH {
                return Err(user_error("native search path length limit reached"));
            }
            let left_tree = match before {
                Some(m::TreeValue::Tree(id)) => Some(id),
                _ => None,
            };
            let right_tree = match after {
                Some(m::TreeValue::Tree(id)) => Some(id),
                _ => None,
            };
            if left_tree.is_some() || right_tree.is_some() {
                pending.push((path.clone(), left_tree, right_tree, depth + 1));
            }
            // Tree-to-file changes suppress all removed descendants via the
            // pending pair, and the old leaf at this path when it was a FILE.
            if before
                .as_ref()
                .is_some_and(|value| !matches!(value, m::TreeValue::Tree(_)))
                || after
                    .as_ref()
                    .is_some_and(|value| !matches!(value, m::TreeValue::Tree(_)))
            {
                result.suppressed.insert(path.clone());
                if let Some(m::TreeValue::File { id, size_bytes, .. }) = after {
                    result.files.insert(path, (id, size_bytes));
                }
            }
            if result.suppressed.len() + pending.len() > MAX_PATHS {
                return Err(user_error(
                    "native search changed-path/frontier limit reached",
                ));
            }
        }
    }
    Ok(result)
}
#[derive(Serialize)]
struct Hit {
    path: String,
    file_id: String,
    file_size: u64,
    byte_offset: u64,
    match_bytes: u32,
    line: u64,
    byte_column: u64,
    snippet_offset: u64,
    snippet: Vec<u8>,
    local: bool,
}
impl Hit {
    fn remote(value: &q::SearchHit) -> Self {
        Self {
            path: value.path.clone(),
            file_id: hex(&value.file_id),
            file_size: value.file_size,
            byte_offset: value.byte_offset,
            match_bytes: value.match_bytes,
            line: value.line,
            byte_column: value.byte_column,
            snippet_offset: value.snippet_offset,
            snippet: value.snippet.clone(),
            local: false,
        }
    }
}
#[derive(Serialize)]
struct Output {
    version: u32,
    revision: String,
    baseline_revision: String,
    root_tree: String,
    pattern: String,
    path_prefix: String,
    policy: &'static str,
    coverage: String,
    changed_paths: usize,
    remote_pages: usize,
    hits: Vec<Hit>,
}
fn charge_output(total: &mut usize, path: usize, snippet: usize) -> Result<(), CommandError> {
    *total = total
        .checked_add(path)
        .and_then(|n| n.checked_add(snippet))
        .and_then(|n| n.checked_add(256))
        .ok_or_else(|| user_error("native search output budget overflow"))?;
    if *total > MAX_OUTPUT {
        return Err(user_error("native search output byte limit reached"));
    }
    Ok(())
}
fn merged_nth<'a>(local: &'a [Hit], remote: &'a [Hit], count: usize) -> Option<&'a Hit> {
    let (mut left, mut right) = (0, 0);
    let mut selected = None;
    for _ in 0..count {
        let a = local.get(left);
        let b = remote.get(right);
        let take_left = match (a, b) {
            (Some(a), Some(b)) => {
                (a.path.as_bytes(), a.byte_offset) <= (b.path.as_bytes(), b.byte_offset)
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return None,
        };
        selected = if take_left {
            left += 1;
            a
        } else {
            right += 1;
            b
        };
    }
    selected
}
async fn collect(
    source: &Source,
    query: &BrokerQueryClient,
    scope: &QueryScope,
    selected: m::CommitId,
    selected_tree: m::TreeId,
    pattern: &[u8],
    compiled: &Pattern,
    policy: q::SearchPolicy,
    prefix: &str,
    limit: usize,
) -> Result<Output, CommandError> {
    let (base, base_tree) = baseline(source, query, scope, selected).await?;
    let local = overlay(source, base_tree, selected_tree).await?;
    let mut local_hits = Vec::new();
    let mut limited = false;
    let mut output_bytes = 0usize;
    let mut match_budget = MatchBudget::new(MatchLimits {
        max_input_bytes: MAX_FILES_BYTES,
        max_work: 64 * 1024 * 1024,
        max_output_bytes: MAX_OUTPUT,
    });
    for (path, (id, size)) in &local.files {
        if !matches_prefix(path, prefix) {
            continue;
        }
        let bytes = source.file(*id, *size).await?;
        let page = compiled
            .scan(&bytes, 0, limit + 1, &mut match_budget, || {
                Instant::now() < source.deadline
            })
            .map_err(failure)?;
        for hit in page.hits {
            charge_output(&mut output_bytes, path.len(), hit.snippet.len())?;
            local_hits.push(Hit {
                path: path.clone(),
                file_id: hex(&id.0),
                file_size: *size,
                byte_offset: hit.offset,
                match_bytes: hit.length,
                line: hit.line,
                byte_column: hit.column - 1,
                snippet_offset: hit.snippet_offset,
                snippet: hit.snippet,
                local: true,
            });
            if local_hits.len() > limit {
                break;
            }
        }
        if page.next_offset.is_some() || local_hits.len() > limit {
            limited = true;
            break;
        }
    }
    let mut remote_prefix = Vec::new();
    let mut cursor = Vec::new();
    let mut pages = 0;
    let mut remote_hits = 0usize;
    let mut coverage = "page_limit".to_owned();
    while pages < MAX_PAGES {
        check(source.deadline)?;
        let reply = query
            .search(
                ScopedSearchQuery {
                    scope: scope.handle,
                    revision: base,
                    pattern: pattern.to_vec(),
                    path_prefix: prefix.to_owned(),
                    policy,
                    page_size: 128,
                    cursor: cursor.clone(),
                },
                source.owner.clone(),
            )
            .await
            .map_err(failure)?;
        let status = q::SearchCoverage::try_from(reply.value.coverage).map_err(failure)?;
        if status == q::SearchCoverage::SearchWarming {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        pages += 1;
        for hit in &reply.value.hits {
            remote_hits = remote_hits
                .checked_add(1)
                .ok_or_else(|| user_error("native search remote count overflow"))?;
            if remote_hits > MAX_REMOTE_HITS {
                return Err(user_error(
                    "native search remote result processing limit reached",
                ));
            }
            if !local.suppressed.contains(&hit.path) && remote_prefix.len() <= limit {
                charge_output(&mut output_bytes, hit.path.len(), hit.snippet.len())?;
                remote_prefix.push(Hit::remote(hit));
            }
        }
        match status {
            q::SearchCoverage::SearchMore => {
                cursor = reply.value.cursor.clone();
                // A local late-path prefix cannot stop earlier remote pages.
                // Only the raw remote page watermark proves that all remote
                // hits before the merged Nth result have been considered.
                if let (Some(nth), Some(last)) = (
                    merged_nth(&local_hits, &remote_prefix, limit),
                    reply.value.hits.last(),
                ) {
                    if (last.path.as_bytes(), last.byte_offset)
                        >= (nth.path.as_bytes(), nth.byte_offset)
                    {
                        coverage = "result_limit".to_owned();
                        break;
                    }
                }
            }
            q::SearchCoverage::SearchComplete => {
                coverage = if limited || local_hits.len() + remote_prefix.len() > limit {
                    "result_limit"
                } else {
                    "complete"
                }
                .to_owned();
                break;
            }
            other => {
                coverage = other
                    .as_str_name()
                    .trim_start_matches("SEARCH_")
                    .to_ascii_lowercase();
                break;
            }
        }
    }
    let mut hits = local_hits;
    hits.extend(remote_prefix);
    hits.sort_by(|a, b| {
        a.path
            .as_bytes()
            .cmp(b.path.as_bytes())
            .then(a.byte_offset.cmp(&b.byte_offset))
    });
    if hits.len() > limit {
        hits.truncate(limit);
        if coverage == "complete" {
            coverage = "result_limit".into();
        }
    }
    Ok(Output {
        version: 1,
        revision: hex(&selected.0),
        baseline_revision: hex(&base.0),
        root_tree: hex(&selected_tree.0),
        pattern: String::from_utf8_lossy(pattern).into_owned(),
        path_prefix: prefix.to_owned(),
        policy: match policy {
            q::SearchPolicy::LiteralBytesV1 => "literal_bytes_v1",
            q::SearchPolicy::RegexBytesV1 => "regex_bytes_v1",
            _ => unreachable!("CLI selects a supported search policy"),
        },
        coverage,
        changed_paths: local.suppressed.len(),
        remote_pages: pages,
        hits,
    })
}
pub(crate) async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    args: SearchArgs,
) -> Result<(), CommandError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return run_query(ui, command, args).await;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(failure)?;
    runtime.block_on(run_query(ui, command, args))
}
async fn run_query(
    ui: &mut Ui,
    command: &CommandHelper,
    args: SearchArgs,
) -> Result<(), CommandError> {
    let (mode, policy) = if args.fixed_strings {
        (SearchMode::Literal, q::SearchPolicy::LiteralBytesV1)
    } else {
        (SearchMode::Regex, q::SearchPolicy::RegexBytesV1)
    };
    let compiled = Pattern::new(args.pattern.as_bytes(), mode).map_err(failure)?;
    if !(1..=10_000).contains(&args.limit) {
        return Err(cli_error("--limit must be between 1 and 10000"));
    }
    let helper = command.workspace_helper(ui).await?;
    let deadline = Instant::now() + TIMEOUT;
    let selected = tokio::time::timeout_at(
        deadline,
        helper.resolve_single_rev(ui, args.revision.as_ref().unwrap_or(&RevisionArg::AT)),
    )
    .await
    .map_err(|_| user_error("native search deadline exceeded"))??;
    let selected_id = m::CommitId::from_slice(selected.id().as_bytes()).map_err(failure)?;
    let selected_tree = root_tree(&selected)?;
    let prefix = if args.path_prefix.is_empty() {
        String::new()
    } else {
        helper
            .parse_file_path(&args.path_prefix)?
            .as_internal_file_string()
            .to_owned()
    };
    if prefix.len() > MAX_PATH
        || prefix.contains(['\\', '\0'])
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| matches!(part, "" | "." | "..")))
    {
        return Err(cli_error("invalid native search path prefix"));
    }
    let loader = command.workspace_loader()?;
    let locator = broker::locator(&loader.repo_path().join("store"))
        .map_err(|_| user_error("native search requires a broker-backed workspace"))?;
    let query_source = query_source::Source::resolve(
        helper.repo().readonly_index(),
        &crate::vfs::working_copy_path(loader.workspace_root()).map_err(failure)?,
        &locator,
    )
    .map_err(failure)?;
    let query = tokio::time::timeout_at(
        deadline,
        BrokerQueryClient::connect(&locator.socket, &locator.store, Default::default()),
    )
    .await
    .map_err(|_| user_error("native search connection deadline exceeded"))?
    .map_err(failure)?;
    let io = tokio::time::timeout_at(
        deadline,
        BrokerObjectIo::connect(
            &locator.socket,
            &locator.store,
            loader.repo_path().join("store/native-broker-spool"),
            Default::default(),
        ),
    )
    .await
    .map_err(|_| user_error("native search connection deadline exceeded"))?
    .map_err(failure)?;
    let (query, io) =
        if let Some(token) = crate::commands::token(loader.repo_path()).map_err(failure)? {
            (
                query.with_command_token(token),
                io.with_command_token(token),
            )
        } else {
            (query, io)
        };
    let cleanup = query.clone();
    let query = query.with_operation_deadline(deadline);
    let io = io.with_operation_deadline(deadline);
    let scope = query_source.open(&query, random()).await.map_err(failure)?;
    let owner = Arc::new(PhysicalBudget {
        usage: Mutex::new(Usage::default()),
        _scope: scope.owner.clone(),
    });
    let source = Source {
        io,
        owner,
        deadline,
    };
    let result = tokio::time::timeout_at(
        deadline,
        collect(
            &source,
            &query,
            &scope.value,
            selected_id,
            selected_tree,
            args.pattern.as_bytes(),
            &compiled,
            policy,
            &prefix,
            args.limit,
        ),
    )
    .await
    .map_err(|_| user_error("native search deadline exceeded"))
    .and_then(|value| value);
    let closed = cleanup
        .with_operation_deadline(Instant::now() + Duration::from_secs(5))
        .close_scope(scope.value.handle, Arc::new(()))
        .await
        .map_err(failure);
    let output = result?;
    closed?;
    if args.json {
        serde_json::to_writer(ui.stdout(), &output).map_err(failure)?;
        writeln!(ui.stdout())?;
    } else {
        for hit in &output.hits {
            writeln!(
                ui.stdout(),
                "{}:{}:{}:{}",
                hit.path,
                hit.line,
                hit.byte_column + 1,
                String::from_utf8_lossy(&hit.snippet)
            )?;
        }
        writeln!(
            ui.stdout(),
            "Coverage: {} (revision {})",
            output.coverage,
            &output.revision[..12]
        )?;
    }
    Ok(())
}
