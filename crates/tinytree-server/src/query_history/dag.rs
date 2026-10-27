//! Canonical source adapter for the shared, bounded path annotation engine.
use super::*;
use crate::query_overlay::GraphKey;
use tinytree_annotate as engine;

pub(super) struct CanonicalDagReader<'a, 'b> {
    pub(super) reader: &'a mut Reader<'b>,
    pub(super) path: &'a [&'a str],
}
impl engine::Budget for CanonicalDagReader<'_, '_> {
    type Error = Failure;
    fn check(&self) -> Step<()> {
        self.reader.check()
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Step<()> {
        self.reader
            .budget
            .charge(items, 0, bytes)
            .map_err(index_error)?;
        Ok(())
    }
    fn frontier(&mut self, entries: usize) -> Step<()> {
        self.reader.budget.frontier(entries).map_err(index_error)?;
        Ok(())
    }
    fn diff_active(&self) -> bool {
        Instant::now() < self.reader.deadline
    }
}
impl engine::GraphSource for CanonicalDagReader<'_, '_> {
    type Key = GraphKey;
    fn node(&mut self, id: m::CommitId) -> Step<engine::SourceNode<GraphKey>> {
        // This check precedes TREE/path/body access. Graph-only overlays do not
        // become content authority by participating in annotation.
        self.reader.canonical_source(id)?;
        let key = self
            .reader
            .scope
            .graph()
            .lookup_node(id, self.reader.budget)
            .map_err(index_error)?
            .ok_or_else(|| Status::data_loss("DAG source metadata missing"))?
            .key();
        let (parents, value) = if id == m::CommitId::ROOT {
            (Vec::new(), None)
        } else {
            let commit = self.reader.commit(id)?;
            let value = self.reader.path_entry(&commit, self.path)?;
            let parents = if commit.parents.is_empty() {
                vec![m::CommitId::ROOT]
            } else {
                commit.parents
            };
            (parents, value)
        };
        Ok(engine::SourceNode {
            key,
            parents,
            value,
        })
    }
    fn common_ancestors(
        &mut self,
        left: &[m::CommitId],
        right: &[m::CommitId],
    ) -> Step<Vec<m::CommitId>> {
        self.reader
            .scope
            .graph()
            .common_ancestors(left, right, self.reader.budget)
            .map_err(index_error)
            .map_err(Into::into)
    }
    fn is_ancestor(&mut self, ancestor: m::CommitId, descendant: m::CommitId) -> Step<bool> {
        self.reader
            .scope
            .graph()
            .is_ancestor(ancestor, descendant, self.reader.budget)
            .map_err(index_error)
            .map_err(Into::into)
    }
}
impl engine::ContentSource for CanonicalDagReader<'_, '_> {
    fn read_file(&mut self, value: &m::TreeValue) -> Step<Vec<u8>> {
        // Only checked path values loaded by node() are passed by the engine.
        // The physical query Work retains the captured source and admission.
        self.reader.file(&state(value)).map(|(_, bytes)| bytes)
    }
}
pub(super) fn engine_failure(error: engine::Error<Failure>) -> Failure {
    match error {
        engine::Error::Source(error) => error,
        engine::Error::Boundary(boundary) => Failure::Boundary(match boundary {
            engine::Boundary::ScanLimit => Coverage::HistoryScanLimit,
            engine::Boundary::ReadLimit => Coverage::HistoryReadLimit,
            engine::Boundary::DiffLimit => Coverage::HistoryDiffLimit,
        }),
        engine::Error::InvalidGraph(message) => Status::data_loss(message).into(),
        engine::Error::Internal(message) => Status::internal(message).into(),
        engine::Error::DiffDeadline => {
            Status::deadline_exceeded("DAG blame diff deadline exceeded").into()
        }
    }
}
fn state(value: &m::TreeValue) -> q::HistoryPathState {
    match value {
        m::TreeValue::File {
            id,
            executable,
            size_bytes,
            ..
        } => q::HistoryPathState {
            kind: PathKind::HistoryPathFile as i32,
            native_id: id.0.to_vec(),
            size_bytes: *size_bytes,
            executable: *executable,
        },
        _ => absent(),
    }
}

pub(super) fn blame(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::BlameRequest,
    budget: &mut WorkBudget,
    deadline: Instant,
) -> Result<q::BlameResult, Status> {
    // Declared before DAG allocations: it outlives graph/source teardown on
    // success, failure, cancellation and unwinding. The returned target/ranges
    // remain covered by the original physical history Work through encoding.
    let _dag_memory = host.admit_dag_history()?;
    let (path, maximum) = validate(
        &input.path,
        q::HistoryPolicy::LinearFixedPathV1 as i32,
        input.max_commits,
        &host.config,
    )?;
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
        read_limit: None,
    };
    let mut origins = Vec::new();
    let step = (|| -> Step<()> {
        if revision == m::CommitId::ROOT {
            reader.canonical_source(revision)?;
            return Err(Status::not_found("blame path is absent at intrinsic root").into());
        }
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
        let (descriptor, target) = reader.file(&state)?;
        let lines = count_lines(&target);
        if lines > host.config.max_blame_lines {
            return Err(Failure::Boundary(Coverage::HistoryDiffLimit));
        }
        result.target = Some(wire_descriptor(descriptor));
        result.target_bytes = target;
        origins = vec![None; lines];
        if lines == 0 {
            return Ok(());
        }
        let mut diff_limits = jj_lib::diff::LineDiffLimits {
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
        engine::annotate(
            &mut CanonicalDagReader {
                reader: &mut reader,
                path: &path,
            },
            revision,
            &result.target_bytes,
            m::FileId(descriptor.key.id.0),
            engine::Limits {
                max_commits: maximum,
                max_lines: host.config.max_blame_lines,
                max_file_bytes: host.config.max_blame_file_bytes,
            },
            &mut diff_limits,
            &mut result.scanned_commits,
            &mut origins,
        )
        .map_err(engine_failure)?;
        Ok(())
    })();
    match step {
        Ok(()) => {
            result.coverage = Coverage::HistoryComplete as i32;
            result.boundary = None;
        }
        Err(error) => result.coverage = finish_failure(error)? as i32,
    }
    for (line, origin) in origins.into_iter().enumerate() {
        if let Some(last) = result.ranges.last_mut() {
            let contiguous = match origin {
                Some(origin) => {
                    last.origin
                        .as_ref()
                        .is_some_and(|id| id.value == origin.commit.0)
                        && last.original_line as usize + (last.end_line - last.start_line) as usize
                            == origin.line
                }
                None => last.origin.is_none(),
            };
            if contiguous {
                last.end_line += 1;
                continue;
            }
        }
        result.ranges.push(q::BlameRange {
            start_line: line as u32,
            end_line: line as u32 + 1,
            origin: origin.map(|origin| wire(origin.commit)),
            original_line: origin.map_or(0, |origin| origin.line as u32),
        });
    }
    Ok(result)
}

pub(super) fn frontier(
    reader: &mut Reader<'_>,
    path: &[&str],
    anchors: &[m::CommitId],
    maximum: usize,
    diff_limits: &mut jj_lib::diff::LineDiffLimits,
    scanned: &mut u32,
) -> Step<Vec<engine::FrontierNode<GraphKey>>> {
    let limits = engine::Limits {
        max_commits: maximum,
        max_lines: reader.host.config.max_blame_lines,
        max_file_bytes: reader.host.config.max_blame_file_bytes,
    };
    engine::frontier(
        &mut CanonicalDagReader { reader, path },
        anchors,
        limits,
        diff_limits,
        scanned,
    )
    .map_err(engine_failure)
}
