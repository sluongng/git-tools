use super::*;
use tempfile::TempDir;

fn canonical(parents: &[m::CommitId], change: u8, label: &str) -> m::EncodedObject {
    let signature = m::Signature {
        name: "Author".into(),
        email: "author@example.invalid".into(),
        timestamp: m::Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    m::encode(
        &m::Object::Commit(m::Commit {
            parents: parents.to_vec(),
            predecessors: Vec::new(),
            root_tree: m::TreeMerge {
                terms: vec![m::TreeId(m::empty_tree().descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId([change; 16]),
            description: label.into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }),
        m::Limits::default(),
    )
    .unwrap()
}
fn add(index: &Index, repo: &str, parents: &[m::CommitId], change: u8, label: &str) -> CommitNode {
    let object = canonical(parents, change, label);
    index
        .ingest(repo, object.descriptor, &object.bytes)
        .unwrap()
}
fn fixture() -> (TempDir, Arc<Index>) {
    let dir = tempfile::tempdir().unwrap();
    let index = Index::open(dir.path().join("graph"), &Limits::default()).unwrap();
    (dir, index)
}
fn reference_ancestor(
    graph: &BTreeMap<m::CommitId, Vec<m::CommitId>>,
    ancestor: m::CommitId,
    descendant: m::CommitId,
) -> bool {
    let mut pending = vec![descendant];
    let mut visited = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if id == ancestor {
            return true;
        }
        if visited.insert(id) {
            pending.extend(graph.get(&id).into_iter().flatten().copied())
        }
    }
    false
}
#[test]
fn scoped_graph_facts_match_reference_dag_and_preserve_merge_alternatives() {
    let (_dir, index) = fixture();
    let mut graph = BTreeMap::from([(m::CommitId::ROOT, Vec::new())]);
    let mut commits = vec![m::CommitId::ROOT];
    for ordinal in 0..24usize {
        let mut parents = vec![commits[ordinal]];
        if ordinal > 3 && ordinal % 4 == 0 {
            parents.push(commits[ordinal / 2]);
        }
        let node = add(
            &index,
            "repo",
            &parents,
            ordinal as u8 + 1,
            &format!("commit {ordinal}"),
        );
        graph.insert(node.commit, parents);
        commits.push(node.commit);
    }
    let scope = index.scope("repo", &[*commits.last().unwrap()]).unwrap();
    for ancestor in &commits {
        for descendant in &commits {
            assert_eq!(
                scope
                    .is_ancestor(*ancestor, *descendant, &mut WorkBudget::default())
                    .unwrap(),
                reference_ancestor(&graph, *ancestor, *descendant)
            );
        }
    }
    assert_eq!(
        scope.heads(&commits, &mut WorkBudget::default()).unwrap(),
        vec![*commits.last().unwrap()]
    );
    let base = add(&index, "fork", &[m::CommitId::ROOT], 10, "base");
    let left = add(&index, "fork", &[base.commit], 11, "left");
    let right = add(&index, "fork", &[base.commit], 12, "right");
    let left2 = add(
        &index,
        "fork",
        &[left.commit],
        15,
        "interleaved left descendant",
    );
    let branches = index.scope("fork", &[left2.commit, right.commit]).unwrap();
    assert!(
        !branches
            .is_ancestor(right.commit, left2.commit, &mut WorkBudget::default())
            .unwrap()
    );
    assert!(
        branches
            .has_id(right.commit, &mut WorkBudget::default())
            .unwrap()
    );
    let left_only = index.scope("fork", &[left2.commit]).unwrap();
    assert!(
        !left_only
            .has_id(right.commit, &mut WorkBudget::default())
            .unwrap()
    );
    let reference = BTreeMap::from([
        (m::CommitId::ROOT, Vec::new()),
        (base.commit, vec![m::CommitId::ROOT]),
        (left.commit, vec![base.commit]),
        (right.commit, vec![base.commit]),
        (left2.commit, vec![left.commit]),
    ]);
    for ancestor in reference.keys() {
        for descendant in reference.keys() {
            assert_eq!(
                branches
                    .is_ancestor(*ancestor, *descendant, &mut WorkBudget::default())
                    .unwrap(),
                reference_ancestor(&reference, *ancestor, *descendant)
            );
        }
    }
    let merge = add(&index, "fork", &[left2.commit, right.commit], 13, "merge");
    let tail = add(&index, "fork", &[merge.commit], 14, "tail");
    let both = index.scope("fork", &[merge.commit]).unwrap();
    let mut expected = vec![left.commit, right.commit];
    expected.sort();
    assert_eq!(
        both.common_ancestors(
            &[left.commit, right.commit],
            &[left.commit, right.commit],
            &mut WorkBudget::default()
        )
        .unwrap(),
        expected,
        "common ancestors intersect unions of input sets, not every individual history"
    );

    let scope = index.scope("fork", &[tail.commit]).unwrap();
    assert!(
        scope
            .is_ancestor(right.commit, tail.commit, &mut WorkBudget::default())
            .unwrap()
    );
    assert_eq!(
        scope
            .common_ancestors(&[left.commit], &[right.commit], &mut WorkBudget::default())
            .unwrap(),
        vec![base.commit]
    );
    assert_eq!(
        scope
            .heads(
                &[base.commit, left.commit, right.commit],
                &mut WorkBudget::default()
            )
            .unwrap(),
        {
            let mut values = vec![left.commit, right.commit];
            values.sort();
            values
        }
    );
}
#[test]
fn prefixes_and_divergent_change_targets_are_exactly_root_scoped() {
    let (_dir, index) = fixture();
    let base = add(&index, "repo", &[m::CommitId::ROOT], 0x30, "base");
    let left = add(&index, "repo", &[base.commit], 0x11, "left");
    let right = add(&index, "repo", &[base.commit], 0x11, "right divergence");
    let unrelated = add(
        &index,
        "repo",
        &[m::CommitId::ROOT],
        0x11,
        "unrelated divergence",
    );
    let hidden = add(&index, "repo", &[m::CommitId::ROOT], 0x12, "other change");
    let scope = index.scope("repo", &[left.commit, right.commit]).unwrap();
    assert!(
        !scope
            .has_id(unrelated.commit, &mut WorkBudget::default())
            .unwrap()
    );
    assert!(
        !scope
            .has_id(hidden.commit, &mut WorkBudget::default())
            .unwrap()
    );
    let mut divergent = vec![left.commit, right.commit];
    divergent.sort();
    assert_eq!(
        scope
            .resolve_change_prefix("1", &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Single(ChangeTargets {
            change_id: m::ChangeId([0x11; 16]),
            commits: divergent,
        })
    );
    assert_eq!(
        scope
            .resolve_commit_prefix(&hex(&unrelated.commit.0), &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::NoMatch
    );
    let full = index
        .scope("repo", &[left.commit, right.commit, hidden.commit])
        .unwrap();
    assert_eq!(
        full.resolve_change_prefix("1", &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Ambiguous
    );
    let length = scope
        .shortest_commit_prefix(left.commit, &mut WorkBudget::default())
        .unwrap();
    assert_eq!(
        scope
            .resolve_commit_prefix(&hex(&left.commit.0)[..length], &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Single(left.commit)
    );
    if length > 1 {
        assert_ne!(
            scope
                .resolve_commit_prefix(
                    &hex(&left.commit.0)[..length - 1],
                    &mut WorkBudget::default()
                )
                .unwrap(),
            PrefixResolution::Single(left.commit)
        );
    }
    assert_eq!(
        scope
            .shortest_change_prefix(m::ChangeId([0x11; 16]), &mut WorkBudget::default())
            .unwrap(),
        1
    );
    // Frozen high-water and roots exclude subsequently ingested descendants.
    let future = add(&index, "repo", &[left.commit], 0x11, "future");
    assert!(
        !scope
            .has_id(future.commit, &mut WorkBudget::default())
            .unwrap()
    );
    assert_eq!(
        scope
            .resolve_commit_prefix(&hex(&future.commit.0), &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::NoMatch
    );
}
#[test]
fn readiness_idempotence_exclusive_ownership_and_restart_keep_epoch_and_positions() {
    let (dir, index) = fixture();
    let epoch = index.epoch();
    let missing = canonical(&[m::CommitId([99; 32])], 1, "missing parent");
    assert!(matches!(
        index.ingest("repo", missing.descriptor, &missing.bytes),
        Err(Error::NotReady)
    ));
    assert!(
        index
            .node("repo", m::CommitId(missing.descriptor.key.id.0))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        index.scope("repo", &[m::CommitId([99; 32])]),
        Err(Error::NotReady)
    ));
    let object = canonical(&[m::CommitId::ROOT], 1, "ready");
    let first = index
        .ingest("repo", object.descriptor, &object.bytes)
        .unwrap();
    assert_eq!(first.position, 1);
    assert_eq!(
        index
            .ingest("repo", object.descriptor, &object.bytes)
            .unwrap(),
        first
    );
    assert!(Index::open(dir.path().join("graph"), &Limits::default()).is_err());
    let frozen = index.scope("repo", &[first.commit]).unwrap();
    assert_eq!(frozen.high_water(), 1);
    assert_eq!(frozen.generation(), 1);
    drop(frozen);
    drop(index);
    let reopened = Index::open(dir.path().join("graph"), &Limits::default()).unwrap();
    assert_eq!(reopened.epoch(), epoch);
    assert_eq!(
        reopened.node("repo", first.commit).unwrap(),
        Some(first.clone())
    );
    let second = add(&reopened, "repo", &[first.commit], 2, "after restart");
    assert_eq!(second.position, 2);
    {
        let bounded_dir = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_parents: 1,
            ..Limits::default()
        };
        let bounded = Index::open(bounded_dir.path().join("graph"), &limits).unwrap();
        let mut commit = match m::decode(&object.bytes, m::Limits::default()).unwrap() {
            m::Object::Commit(commit) => commit,
            _ => unreachable!(),
        };
        // Predecessors and merge labels cannot bypass the predecode vector cap.
        commit.predecessors = vec![m::CommitId::ROOT; 2];
        let oversized = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
        assert!(matches!(
            bounded.ingest("repo", oversized.descriptor, &oversized.bytes),
            Err(Error::Model(m::Error::Limit))
        ));
        assert!(
            bounded
                .node("repo", m::CommitId(oversized.descriptor.key.id.0))
                .unwrap()
                .is_none()
        );
    }
    let wrong = m::empty_tree();
    assert!(matches!(
        reopened.ingest("repo", wrong.descriptor, &wrong.bytes),
        Err(Error::Invalid(_))
    ));
    let mut corrupt = object.bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(matches!(
        reopened.ingest("repo", object.descriptor, &corrupt),
        Err(Error::Model(_))
    ));
}
#[test]
fn corrupted_derived_rows_fail_closed_instead_of_becoming_missing() {
    let (dir, index) = fixture();
    let node = add(&index, "repo", &[m::CommitId::ROOT], 1, "commit");
    drop(index);
    {
        let db = SingleWriterTxDatabase::builder(dir.path().join("graph/index.fjall"))
            .open()
            .unwrap();
        let table = db
            .keyspace("graph", KeyspaceCreateOptions::default)
            .unwrap();
        let node_key = key(b'n', &repo_hash("repo").unwrap(), &node.commit.0);
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&table, node_key, b"corrupt");
        tx.commit().unwrap();
    }
    assert!(matches!(
        Index::open(dir.path().join("graph"), &Limits::default()),
        Err(Error::Corrupt)
    ));
}
#[test]
fn exhausted_work_is_not_a_negative_and_linear_probes_use_skip_links() {
    let (_dir, index) = fixture();
    let mut commits = vec![m::CommitId::ROOT];
    for ordinal in 0..512 {
        let node = add(
            &index,
            "repo",
            &[*commits.last().unwrap()],
            1,
            &format!("linear {ordinal}"),
        );
        commits.push(node.commit);
    }
    let scope = index.scope("repo", &[*commits.last().unwrap()]).unwrap();
    let mut budget = WorkBudget::new(64, 64, 1024 * 1024, 128);
    assert!(
        scope
            .is_ancestor(commits[17], commits[512], &mut budget)
            .unwrap()
    );
    assert!(
        budget.nodes_read() < 64,
        "linear history should use logarithmic record probes"
    );
    let mut common_budget = WorkBudget::new(64, 64, 1024 * 1024, 128);
    assert_eq!(
        scope
            .common_ancestors(&[commits[17]], &[commits[512]], &mut common_budget)
            .unwrap(),
        vec![commits[17]]
    );
    assert!(
        common_budget.nodes_read() < 64,
        "ancestor-related common roots must use chain skips"
    );
    let mut empty = WorkBudget::new(0, 0, 0, 0);
    assert!(matches!(
        scope.has_id(commits[17], &mut empty),
        Err(Error::BudgetExceeded)
    ));
    let mut small = WorkBudget::new(2, 1, 1024 * 1024, 4);
    assert!(matches!(
        scope.resolve_change_prefix("0", &mut small),
        Err(Error::BudgetExceeded)
    ));
}

#[test]
fn compact_records_bound_actual_nodes_and_round_trip_escaped_repository_names() {
    let directory = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_record_bytes: 1024,
        ..Limits::default()
    };
    let path = directory.path().join("graph");
    let index = Index::open(&path, &limits).unwrap();
    let repository = "\"\\".repeat(256);
    let first = add(
        &index,
        &repository,
        &[m::CommitId::ROOT],
        1,
        "escaped repository",
    );
    assert_eq!(first.position, 1);
    let mut parents = Vec::new();
    for ordinal in 0..32 {
        parents.push(
            add(
                &index,
                "repo",
                &[m::CommitId::ROOT],
                2,
                &format!("parent {ordinal}"),
            )
            .commit,
        );
    }
    let oversized = canonical(&parents, 3, "oversized derived parent record");
    let before = index.stats().unwrap();
    assert!(matches!(
        index.ingest("repo", oversized.descriptor, &oversized.bytes),
        Err(Error::Invalid(_))
    ));
    assert_eq!(index.stats().unwrap(), before);
    assert!(
        index
            .node("repo", m::CommitId(oversized.descriptor.key.id.0))
            .unwrap()
            .is_none()
    );
    drop(index);
    let reopened = Index::open(&path, &limits).unwrap();
    assert_eq!(
        reopened.node(&repository, first.commit).unwrap(),
        Some(first)
    );
}
#[test]
fn empty_prefix_and_unknown_id_shortest_lengths_match_jj_semantics() {
    let (_directory, index) = fixture();
    let scope = index.scope("repo", &[m::CommitId::ROOT]).unwrap();
    assert_eq!(
        scope
            .resolve_commit_prefix("", &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Single(m::CommitId::ROOT)
    );
    assert_eq!(
        scope
            .resolve_change_prefix("", &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Single(ChangeTargets {
            change_id: m::ChangeId::ROOT,
            commits: vec![m::CommitId::ROOT]
        })
    );
    assert_eq!(
        scope
            .shortest_commit_prefix(m::CommitId::ROOT, &mut WorkBudget::default())
            .unwrap(),
        0
    );
    assert_eq!(
        scope
            .shortest_change_prefix(m::ChangeId::ROOT, &mut WorkBudget::default())
            .unwrap(),
        0
    );
    for unknown in [
        m::CommitId([0xff; 32]),
        m::CommitId({
            let mut bytes = [0; 32];
            bytes[31] = 1;
            bytes
        }),
    ] {
        let length = scope
            .shortest_commit_prefix(unknown, &mut WorkBudget::default())
            .unwrap();
        assert_eq!(
            scope
                .resolve_commit_prefix(&hex(&unknown.0)[..length], &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::NoMatch
        );
        for shorter in 0..length {
            assert_ne!(
                scope
                    .resolve_commit_prefix(&hex(&unknown.0)[..shorter], &mut WorkBudget::default())
                    .unwrap(),
                PrefixResolution::NoMatch
            );
        }
    }
    for unknown in [
        m::ChangeId([0xff; 16]),
        m::ChangeId({
            let mut bytes = [0; 16];
            bytes[15] = 1;
            bytes
        }),
    ] {
        let length = scope
            .shortest_change_prefix(unknown, &mut WorkBudget::default())
            .unwrap();
        assert_eq!(
            scope
                .resolve_change_prefix(&hex(&unknown.0)[..length], &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::NoMatch
        );
        for shorter in 0..length {
            assert_ne!(
                scope
                    .resolve_change_prefix(&hex(&unknown.0)[..shorter], &mut WorkBudget::default())
                    .unwrap(),
                PrefixResolution::NoMatch
            );
        }
    }
}

/// Manual comparison of the current JSON records and chain-skip overhead.
/// Reports measurements, not thresholds; normal unit tests do not run it.
#[test]
#[ignore = "manual persistent index storage and latency benchmark"]
fn benchmark_storage_and_scoped_queries() {
    use std::time::Instant;
    fn file_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    file_bytes(&entry.path())
                } else if kind.is_file() {
                    entry.metadata().unwrap().len()
                } else {
                    panic!("unexpected benchmark storage entry")
                }
            })
            .sum()
    }
    fn allocated_bytes(path: &Path) -> Option<u64> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let mut bytes = std::fs::metadata(path).unwrap().blocks().checked_mul(512)?;
            for entry in std::fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                let allocated = if kind.is_dir() {
                    allocated_bytes(&entry.path())?
                } else if kind.is_file() {
                    entry.metadata().unwrap().blocks().checked_mul(512)?
                } else {
                    panic!("unexpected benchmark storage entry")
                };
                bytes = bytes.checked_add(allocated)?;
            }
            Some(bytes)
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            None
        }
    }
    fn outcome<T>(result: &Result<T>) -> &'static str {
        match result {
            Ok(_) => "complete",
            Err(Error::BudgetExceeded) => "budget_exhausted",
            Err(_) => "error",
        }
    }
    for shape in ["chain", "branch_merge"] {
        for size in [64usize, 512, 4096] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("graph");
            let index = Index::open(&path, &Limits::default()).unwrap();
            let mut commits = Vec::with_capacity(size);
            let mut left = m::CommitId::ROOT;
            let mut right = m::CommitId::ROOT;
            let mut canonical_bytes = 0u64;
            let started = Instant::now();
            for ordinal in 0..size {
                let parents = if shape == "chain" {
                    vec![left]
                } else if ordinal > 0 && ordinal % 16 == 0 && left != right {
                    vec![left, right]
                } else if ordinal % 2 == 0 {
                    vec![left]
                } else {
                    vec![right]
                };
                let object = canonical(
                    &parents,
                    (ordinal % 255 + 1) as u8,
                    &format!("benchmark commit {ordinal}"),
                );
                canonical_bytes += object.descriptor.size_bytes;
                let node = index
                    .ingest("benchmark", object.descriptor, &object.bytes)
                    .unwrap();
                if shape == "chain" || ordinal % 2 == 0 {
                    left = node.commit;
                } else {
                    right = node.commit;
                }
                if parents.len() == 2 {
                    left = node.commit;
                    right = node.commit;
                }
                commits.push(node.commit);
            }
            let ingest_ns = started.elapsed().as_nanos();
            let stats = index.stats().unwrap();
            assert_eq!(stats.nodes, size as u64);
            let roots = if shape == "chain" {
                vec![left]
            } else {
                vec![left, right]
            };
            drop(index);
            let closed_file_bytes = file_bytes(&path);
            let closed_allocated_bytes = allocated_bytes(&path);
            let started = Instant::now();
            let reopened = Index::open(&path, &Limits::default()).unwrap();
            let reopen_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            let scope = reopened.scope("benchmark", &roots).unwrap();
            let scope_ns = started.elapsed().as_nanos();
            let mut facts_budget = WorkBudget::default();
            let started = Instant::now();
            let facts = (|| -> Result<()> {
                if !scope.has_id(commits[size / 4], &mut facts_budget)? {
                    return Err(Error::Corrupt);
                }
                if !scope.is_ancestor(m::CommitId::ROOT, left, &mut facts_budget)? {
                    return Err(Error::Corrupt);
                }
                scope.heads(&roots, &mut facts_budget)?;
                scope.common_ancestors(&[left], &[right], &mut facts_budget)?;
                Ok(())
            })();
            let facts_ns = started.elapsed().as_nanos();
            assert!(!matches!(facts, Err(ref error) if !matches!(error, Error::BudgetExceeded)));
            let mut prefix_budget = WorkBudget::default();
            let started = Instant::now();
            let prefixes = (|| -> Result<()> {
                let target = commits[size / 3];
                let text = hex(&target.0);
                if scope.resolve_commit_prefix(&text, &mut prefix_budget)?
                    != PrefixResolution::Single(target)
                {
                    return Err(Error::Corrupt);
                }
                scope.shortest_commit_prefix(target, &mut prefix_budget)?;
                scope.resolve_change_prefix("01", &mut prefix_budget)?;
                Ok(())
            })();
            let prefixes_ns = started.elapsed().as_nanos();
            assert!(!matches!(prefixes, Err(ref error) if !matches!(error, Error::BudgetExceeded)));
            drop(scope);
            drop(reopened);
            let final_file_bytes = file_bytes(&path);
            let final_allocated_bytes = allocated_bytes(&path);
            println!(
                "{}",
                serde_json::json!({
                    "benchmark": "tinytree_index_storage_v1", "shape": shape, "commits": size,
                    "canonical_commit_bytes": canonical_bytes, "logical_record_bytes": stats.logical_bytes,
                    "closed_file_bytes": closed_file_bytes, "final_file_bytes": final_file_bytes,
                    "closed_allocated_bytes": closed_allocated_bytes, "final_allocated_bytes": final_allocated_bytes,
                    "logical_bytes_per_commit": stats.logical_bytes as f64 / size as f64,
                    "logical_to_canonical_ratio": stats.logical_bytes as f64 / canonical_bytes as f64,
                    "ingest_ns": ingest_ns, "reopen_ns": reopen_ns, "scope_ns": scope_ns,
                    "facts_ns": facts_ns, "facts_status": outcome(&facts),
                    "facts_node_probes": facts_budget.nodes_read(), "facts_edges": facts_budget.edges_read(),
                    "facts_record_bytes": facts_budget.bytes_read(),
                    "prefixes_ns": prefixes_ns, "prefixes_status": outcome(&prefixes),
                    "prefixes_node_probes": prefix_budget.nodes_read(), "prefixes_edges": prefix_budget.edges_read(),
                    "prefixes_record_bytes": prefix_budget.bytes_read(),
                })
            );
        }
    }
}

#[test]
fn wide_merge_dag_remains_ready_and_exact_after_reopen() {
    let (directory, index) = fixture();
    // Earlier position and equal generation force traversal through every
    // merge parent without reaching this independent branch.
    let unrelated = add(
        &index,
        "repo",
        &[m::CommitId::ROOT],
        7,
        "unrelated earlier branch",
    )
    .commit;
    let mut shared = Vec::new();
    for ordinal in 0..32 {
        shared.push(
            add(
                &index,
                "repo",
                &[m::CommitId::ROOT],
                1,
                &format!("shared {ordinal}"),
            )
            .commit,
        );
    }
    let mut merges = Vec::new();
    for ordinal in 0..16 {
        merges.push(add(&index, "repo", &shared, 2, &format!("merge {ordinal}")).commit);
    }
    let final_merge = add(&index, "repo", &merges, 3, "wide final merge");
    assert_eq!(final_merge.parents, merges);
    assert_eq!(final_merge.generation, 3);
    drop(index);
    let reopened = Index::open(directory.path().join("graph"), &Limits::default()).unwrap();
    assert_eq!(
        reopened.node("repo", final_merge.commit).unwrap(),
        Some(final_merge.clone())
    );
    let scope = reopened.scope("repo", &[final_merge.commit]).unwrap();
    for parent in &merges {
        assert!(
            scope
                .is_ancestor(*parent, final_merge.commit, &mut WorkBudget::default())
                .unwrap()
        );
    }
    let both = reopened
        .scope("repo", &[final_merge.commit, unrelated])
        .unwrap();
    let mut bounded = WorkBudget::new(256, 4096, 64 * 1024 * 1024, 128);
    assert!(
        !both
            .is_ancestor(unrelated, final_merge.commit, &mut bounded)
            .unwrap()
    );
    assert!(
        bounded.nodes_read() < 256,
        "shared parents must not be repeatedly decoded across merge fan-in"
    );
    let mut exhausted = WorkBudget::new(8, 4096, 64 * 1024 * 1024, 128);
    assert!(matches!(
        both.is_ancestor(unrelated, final_merge.commit, &mut exhausted),
        Err(Error::BudgetExceeded)
    ));
    shared.sort();
    assert_eq!(
        scope
            .common_ancestors(&[merges[0]], &[merges[15]], &mut WorkBudget::default())
            .unwrap(),
        shared
    );
}

#[test]
fn common_ancestor_sets_match_reference_for_every_crisscross_subset() {
    let (_directory, index) = fixture();
    let a = add(&index, "repo", &[m::CommitId::ROOT], 1, "a").commit;
    let b = add(&index, "repo", &[m::CommitId::ROOT], 2, "b").commit;
    let c = add(&index, "repo", &[a, b], 3, "c merge").commit;
    let d = add(&index, "repo", &[b, a], 4, "d crisscross merge").commit;
    let left_tip = add(&index, "repo", &[c], 5, "left tip").commit;
    let right_tip = add(&index, "repo", &[d], 6, "right tip").commit;
    let graph = BTreeMap::from([
        (m::CommitId::ROOT, Vec::new()),
        (a, vec![m::CommitId::ROOT]),
        (b, vec![m::CommitId::ROOT]),
        (c, vec![a, b]),
        (d, vec![b, a]),
        (left_tip, vec![c]),
        (right_tip, vec![d]),
    ]);
    let scope = index.scope("repo", &[left_tip, right_tip]).unwrap();
    let ids = [a, b, c, d, left_tip, right_tip];
    let sets: Vec<Vec<_>> = (0..1usize << ids.len())
        .map(|mask| {
            ids.iter()
                .enumerate()
                .filter_map(|(bit, id)| (mask & (1 << bit) != 0).then_some(*id))
                .collect()
        })
        .collect();
    for left in &sets {
        for right in &sets {
            let common: Vec<_> = graph
                .keys()
                .copied()
                .filter(|candidate| {
                    left.iter()
                        .any(|head| reference_ancestor(&graph, *candidate, *head))
                        && right
                            .iter()
                            .any(|head| reference_ancestor(&graph, *candidate, *head))
                })
                .collect();
            let expected: Vec<_> = common
                .iter()
                .copied()
                .filter(|candidate| {
                    !common.iter().any(|other| {
                        candidate != other && reference_ancestor(&graph, *candidate, *other)
                    })
                })
                .collect();
            assert_eq!(
                scope
                    .common_ancestors(left, right, &mut WorkBudget::default())
                    .unwrap(),
                expected,
                "left={left:?}, right={right:?}"
            );
        }
    }
    let mut expected = vec![a, b];
    expected.sort();
    assert_eq!(
        scope
            .common_ancestors(&[left_tip], &[right_tip], &mut WorkBudget::default())
            .unwrap(),
        expected
    );
    assert_eq!(
        scope
            .common_ancestors(&[c], &[left_tip], &mut WorkBudget::default())
            .unwrap(),
        vec![c]
    );
    assert_eq!(
        scope
            .common_ancestors(
                &[m::CommitId::ROOT],
                &[right_tip],
                &mut WorkBudget::default()
            )
            .unwrap(),
        vec![m::CommitId::ROOT]
    );
    let mut aggregate = WorkBudget::new(100_000, 1_000_000, 64 * 1024 * 1024, 3);
    assert!(matches!(
        scope.common_ancestors(&[a, b], &[c, d], &mut aggregate),
        Err(Error::BudgetExceeded)
    ));
    let mut read_bound = WorkBudget::new(4, 1_000_000, 64 * 1024 * 1024, 100);
    assert!(matches!(
        scope.common_ancestors(&[left_tip], &[right_tip], &mut read_bound),
        Err(Error::BudgetExceeded)
    ));
}

// Fault fixtures edit persistent bytes, while all detection assertions use the
// public reopen/query API. Re-signing isolates structural validation from the
// checksum guard rather than accidentally testing only a mismatched digest.
fn resign_derived_record(key: &[u8], bytes: &mut Vec<u8>) {
    bytes.truncate(bytes.len() - 32);
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-derived-index-record-v2\0");
    hash.update(&(key.len() as u32).to_le_bytes());
    hash.update(key);
    hash.update(bytes);
    bytes.extend(hash.finalize().as_bytes());
}
#[test]
fn compact_envelope_rejects_truncation_counts_reserved_and_key_transplants() {
    for behavior in [
        "truncated",
        "trailing",
        "parent_count_overflow",
        "skip_count_overflow",
        "reserved",
        "missing_conflict_feature",
        "invalid_conflict_flag",
        "conflict_checksum",
        "key_transplant",
    ] {
        let (directory, index) = fixture();
        let a = add(&index, "repo", &[m::CommitId::ROOT], 1, "a");
        let b = add(&index, "repo", &[m::CommitId::ROOT], 2, "b");
        drop(index);
        {
            let db = SingleWriterTxDatabase::builder(directory.path().join("graph/index.fjall"))
                .open()
                .unwrap();
            let table = db
                .keyspace("graph", KeyspaceCreateOptions::default)
                .unwrap();
            let source = key(b'n', &repo_hash("repo").unwrap(), &a.commit.0);
            let destination = if behavior == "key_transplant" {
                key(b'n', &repo_hash("repo").unwrap(), &b.commit.0)
            } else {
                source.clone()
            };
            let mut bytes = table.get(&source).unwrap().unwrap().to_vec();
            match behavior {
                "truncated" => {
                    bytes.pop();
                }
                "trailing" => bytes.push(0),
                "parent_count_overflow" => {
                    bytes[60..64].copy_from_slice(&u32::MAX.to_le_bytes());
                    resign_derived_record(&source, &mut bytes);
                }
                "skip_count_overflow" => {
                    bytes[64] = 65;
                    resign_derived_record(&source, &mut bytes);
                }
                "reserved" => {
                    bytes[5] = 1;
                    resign_derived_record(&source, &mut bytes);
                }
                "missing_conflict_feature" => {
                    bytes[65..68].copy_from_slice(&[0, 0, 0]);
                    resign_derived_record(&source, &mut bytes);
                }
                "invalid_conflict_flag" => {
                    bytes[65] = 2;
                    resign_derived_record(&source, &mut bytes);
                }
                "conflict_checksum" => bytes[65] ^= 1,
                "key_transplant" => {}
                _ => unreachable!(),
            }
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(&table, destination, bytes);
            tx.commit().unwrap();
        }
        assert!(
            matches!(
                Index::open(directory.path().join("graph"), &Limits::default()),
                Err(Error::Corrupt)
            ),
            "{behavior}"
        );
    }
}
#[test]
fn legacy_schema_is_explicitly_unsupported_and_never_rewritten() {
    #[derive(serde::Serialize)]
    struct Legacy {
        schema: u32,
        epoch: [u8; 16],
        repositories: u64,
        nodes: u64,
        bytes: u64,
    }
    let (directory, index) = fixture();
    drop(index);
    let value = Legacy {
        schema: 1,
        epoch: [7; 16],
        repositories: 0,
        nodes: 0,
        bytes: 0,
    };
    let digest = *blake3::hash(&serde_json::to_vec(&value).unwrap()).as_bytes();
    let bytes = serde_json::to_vec(&serde_json::json!({"value":value,"checksum":digest})).unwrap();
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("graph/index.fjall"))
            .open()
            .unwrap();
        let table = db
            .keyspace("graph", KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&table, b"header", &bytes);
        tx.commit().unwrap();
    }
    assert!(matches!(
        Index::open(directory.path().join("graph"), &Limits::default()),
        Err(Error::UnsupportedSchema)
    ));
    let db = SingleWriterTxDatabase::builder(directory.path().join("graph/index.fjall"))
        .open()
        .unwrap();
    let table = db
        .keyspace("graph", KeyspaceCreateOptions::default)
        .unwrap();
    assert_eq!(
        table.get(b"header").unwrap().unwrap().as_ref(),
        bytes.as_slice()
    );
}
#[test]
fn conflict_metadata_negative_time_and_scope_bound_parent_tokens_survive_reopen() {
    let (directory, index) = fixture();
    let base = add(&index, "repo", &[m::CommitId::ROOT], 1, "base");
    let left = add(&index, "repo", &[base.commit], 2, "left");
    let right = add(&index, "repo", &[base.commit], 3, "right");
    let object = canonical(&[right.commit, left.commit], 4, "ordered merge");
    let m::Object::Commit(mut commit) = m::decode(&object.bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    commit.committer.timestamp.millis_since_epoch = -123456789;
    commit.root_tree = m::TreeMerge {
        terms: vec![m::TreeId([1; 32]), m::TreeId([2; 32]), m::TreeId([3; 32])],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };

    let object = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
    let merge = index
        .ingest("repo", object.descriptor, &object.bytes)
        .unwrap();
    assert_eq!(merge.committer_millis, -123456789);
    assert!(merge.has_conflict);
    assert!(!left.has_conflict && !right.has_conflict);
    assert_eq!(
        index
            .ingest("repo", object.descriptor, &object.bytes)
            .unwrap(),
        merge
    );
    drop(index);
    let reopened = Index::open(directory.path().join("graph"), &Limits::default()).unwrap();
    assert_eq!(
        reopened.node("repo", merge.commit).unwrap(),
        Some(merge.clone())
    );
    let scope = Arc::new(reopened.scope("repo", &[merge.commit]).unwrap());
    let token = scope
        .lookup_node(merge.commit, &mut WorkBudget::default())
        .unwrap()
        .unwrap();
    assert_eq!(token.committer_millis(), -123456789);
    assert!(token.has_conflict());
    assert_eq!(
        scope
            .parent_nodes(&token, &mut WorkBudget::default())
            .unwrap()
            .iter()
            .map(|node| node.commit())
            .collect::<Vec<_>>(),
        vec![right.commit, left.commit]
    );
    let root = scope
        .lookup_node(m::CommitId::ROOT, &mut WorkBudget::default())
        .unwrap()
        .unwrap();
    assert!(!root.has_conflict());
    assert!(
        scope
            .parent_nodes(&root, &mut WorkBudget::default())
            .unwrap()
            .is_empty()
    );
    assert!(
        scope
            .lookup_node(m::CommitId([99; 32]), &mut WorkBudget::default())
            .unwrap()
            .is_none()
    );
    let reconstructed = Arc::new(reopened.scope("repo", &[merge.commit]).unwrap());
    assert!(matches!(
        reconstructed.parent_nodes(&token, &mut WorkBudget::default()),
        Err(Error::Invalid(_))
    ));
    let narrower = Arc::new(reopened.scope("repo", &[left.commit]).unwrap());
    assert!(matches!(
        narrower.parent_nodes(&token, &mut WorkBudget::default()),
        Err(Error::Invalid(_))
    ));
    let future = add(&reopened, "repo", &[merge.commit], 5, "future");
    assert!(
        scope
            .lookup_node(future.commit, &mut WorkBudget::default())
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        scope.lookup_node(merge.commit, &mut WorkBudget::new(0, 0, 0, 0)),
        Err(Error::BudgetExceeded)
    ));
    assert!(matches!(
        scope.parent_nodes(&token, &mut WorkBudget::new(100, 100, 1024 * 1024, 1)),
        Err(Error::BudgetExceeded)
    ));
}

#[test]
fn key_binding_and_duplicate_timestamp_checks_reject_conflicting_derived_rows() {
    let (_directory, index) = fixture();
    let object = canonical(&[m::CommitId::ROOT], 1, "a");
    let a = index
        .ingest("repo", object.descriptor, &object.bytes)
        .unwrap();
    let b = add(&index, "repo", &[m::CommitId::ROOT], 2, "b");
    {
        let state = index.state.lock().unwrap();
        let source = key(b'n', &repo_hash("repo").unwrap(), &a.commit.0);
        let destination = key(b'n', &repo_hash("repo").unwrap(), &b.commit.0);
        let bytes = state.table.get(&source).unwrap().unwrap();
        let mut tx = state.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&state.table, destination, bytes);
        tx.commit().unwrap();
    }
    // Direct public lookup must reject the transplanted value without relying
    // on a later whole-index/secondary-index consistency scan.
    assert!(matches!(index.node("repo", b.commit), Err(Error::Corrupt)));
    {
        let state = index.state.lock().unwrap();
        let storage_key = key(b'n', &repo_hash("repo").unwrap(), &a.commit.0);
        let mut bytes = state.table.get(&storage_key).unwrap().unwrap().to_vec();
        bytes[52..60].copy_from_slice(&(-42i64).to_le_bytes());
        resign_derived_record(&storage_key, &mut bytes);
        let mut tx = state.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&state.table, storage_key, bytes);
        tx.commit().unwrap();
    }
    assert!(matches!(
        index.ingest("repo", object.descriptor, &object.bytes),
        Err(Error::Corrupt)
    ));
}

#[test]
fn existing_foreign_or_headerless_databases_are_not_initialized() {
    struct Case {
        name: &'static str,
        keyspace: Option<&'static str>,
        insert_row: bool,
        expect_graph: bool,
    }
    for case in [
        Case {
            name: "foreign keyspace",
            keyspace: Some("foreign"),
            insert_row: true,
            expect_graph: false,
        },
        Case {
            name: "empty graph",
            keyspace: Some("graph"),
            insert_row: false,
            expect_graph: true,
        },
        Case {
            name: "graph with rows without header",
            keyspace: Some("graph"),
            insert_row: true,
            expect_graph: true,
        },
        Case {
            name: "directory without marker",
            keyspace: None,
            insert_row: true,
            expect_graph: false,
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = PrivateDir::create(directory.path().join("graph")).unwrap();
        let data = root.path().join("index.fjall");
        if let Some(name) = case.keyspace {
            let db = SingleWriterTxDatabase::builder(&data).open().unwrap();
            let table = db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
            if case.insert_row {
                let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
                tx.insert(&table, b"unrelated", b"preserved");
                tx.commit().unwrap();
            }
        } else {
            std::fs::create_dir(&data).unwrap();
            if case.insert_row {
                std::fs::write(data.join("unrelated"), b"preserved").unwrap();
            }
        }
        assert!(
            matches!(
                Index::open(root.path(), &Limits::default()),
                Err(Error::Corrupt)
            ),
            "{}",
            case.name
        );
        if let Some(name) = case.keyspace {
            let db = SingleWriterTxDatabase::builder(&data).open().unwrap();
            assert_eq!(
                db.keyspace_exists("graph"),
                case.expect_graph,
                "{}",
                case.name
            );
            let table = db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
            assert!(table.get(b"header").unwrap().is_none(), "{}", case.name);
            if case.insert_row {
                assert_eq!(
                    table.get(b"unrelated").unwrap().unwrap().as_ref(),
                    b"preserved",
                    "{}",
                    case.name
                );
            } else {
                assert!(
                    db.read_tx().prefix(&table, b"").next().is_none(),
                    "{}",
                    case.name
                );
            }
        } else {
            assert!(!data.join("version").exists(), "{}", case.name);
            if case.insert_row {
                assert_eq!(
                    std::fs::read(data.join("unrelated")).unwrap(),
                    b"preserved",
                    "{}",
                    case.name
                );
            }
        }
    }
}

#[test]
fn tree_conflict_flag_is_not_inferred_from_number_of_commit_parents() {
    let (_directory, index) = fixture();
    let clean = add(&index, "repo", &[m::CommitId::ROOT], 1, "clean");
    let other = add(&index, "repo", &[m::CommitId::ROOT], 2, "other");
    let resolved_merge = add(
        &index,
        "repo",
        &[clean.commit, other.commit],
        3,
        "resolved merge",
    );
    assert!(!resolved_merge.has_conflict);
    let value = canonical(&[resolved_merge.commit], 4, "single-parent conflict");
    let m::Object::Commit(mut commit) = m::decode(&value.bytes, Default::default()).unwrap() else {
        unreachable!()
    };
    commit.root_tree = m::TreeMerge {
        terms: vec![m::TreeId([1; 32]), m::TreeId([2; 32]), m::TreeId([3; 32])],
        labels: vec!["one".into(), "base".into(), "two".into()],
    };
    let value = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
    let conflict = index
        .ingest("repo", value.descriptor, &value.bytes)
        .unwrap();
    assert!(conflict.has_conflict);
    let scope = Arc::new(index.scope("repo", &[conflict.commit]).unwrap());
    assert!(
        scope
            .lookup_node(conflict.commit, &mut WorkBudget::default())
            .unwrap()
            .unwrap()
            .has_conflict()
    );
    assert!(
        !scope
            .lookup_node(resolved_merge.commit, &mut WorkBudget::default())
            .unwrap()
            .unwrap()
            .has_conflict()
    );
    // Even a checksummed derived flag cannot replace canonical truth on upsert.
    {
        let state = index.state.lock().unwrap();
        let storage_key = key(b'n', &repo_hash("repo").unwrap(), &conflict.commit.0);
        let mut bytes = state.table.get(&storage_key).unwrap().unwrap().to_vec();
        bytes[65] = 0;
        resign_derived_record(&storage_key, &mut bytes);
        let mut tx = state.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&state.table, storage_key, bytes);
        tx.commit().unwrap();
    }
    assert!(matches!(
        index.ingest("repo", value.descriptor, &value.bytes),
        Err(Error::Corrupt)
    ));
}
#[test]
fn schema_two_without_conflict_summary_is_unsupported_and_not_rewritten() {
    let (directory, index) = fixture();
    drop(index);
    let original = {
        let db = SingleWriterTxDatabase::builder(directory.path().join("graph/index.fjall"))
            .open()
            .unwrap();
        let table = db
            .keyspace("graph", KeyspaceCreateOptions::default)
            .unwrap();
        let bytes = table.get(b"header").unwrap().unwrap();
        let mut header: Header = decode(b"header", &bytes, &Limits::default()).unwrap();
        header.schema = 2;
        let bytes = encode(b"header", &header).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&table, b"header", &bytes);
        tx.commit().unwrap();
        bytes
    };
    assert!(matches!(
        Index::open(directory.path().join("graph"), &Limits::default()),
        Err(Error::UnsupportedSchema)
    ));
    let db = SingleWriterTxDatabase::builder(directory.path().join("graph/index.fjall"))
        .open()
        .unwrap();
    let table = db
        .keyspace("graph", KeyspaceCreateOptions::default)
        .unwrap();
    assert_eq!(
        table.get(b"header").unwrap().unwrap().as_ref(),
        original.as_slice()
    );
}

#[test]
fn lexical_candidate_pages_seek_after_scanned_rows_and_keep_frozen_scope_membership() {
    let (_directory, index) = fixture();
    let frozen = Arc::new(index.scope("repo", &[m::CommitId::ROOT]).unwrap());
    let mut nodes = Vec::new();
    for (change, label) in [
        (0x10, "future first"),
        (0x11, "future divergent left"),
        (0x11, "future divergent right"),
        (0x1f, "future high nibble"),
        (0x2a, "future outside prefix"),
    ] {
        nodes.push(add(&index, "repo", &[m::CommitId::ROOT], change, label));
    }
    let mut cursor = None;
    let mut keys = Vec::new();
    let mut returned = Vec::new();
    let mut empty_progress = 0;
    let mut budget = WorkBudget::default();
    for _ in 0..nodes.len() + 3 {
        let before = budget.nodes_read();
        let page = frozen
            .commit_prefix_candidates("", cursor.as_ref(), 1, &mut budget)
            .unwrap();
        assert!(page.scanned <= 1);
        returned.extend(page.nodes.iter().map(ScopedNode::commit));
        if page.complete {
            assert!(page.cursor.is_none());
            break;
        }
        if page.nodes.is_empty() {
            empty_progress += 1;
            assert_eq!(
                budget.nodes_read() - before,
                1,
                "future rows need one physical seek/read, not replay of earlier keys"
            );
        }
        let next = page.cursor.unwrap();
        keys.push(next.last_scanned());
        cursor = Some(next);
    }
    assert_eq!(returned, vec![m::CommitId::ROOT]);
    assert_eq!(empty_progress, nodes.len());
    assert!(keys.windows(2).all(|p| p[0] < p[1]));
    assert_eq!(keys.len(), nodes.len() + 1);
    let complete = Arc::new(
        index
            .scope("repo", &nodes.iter().map(|n| n.commit).collect::<Vec<_>>())
            .unwrap(),
    );
    let mut expected = nodes
        .iter()
        .map(|n| n.commit)
        .chain([m::CommitId::ROOT])
        .collect::<Vec<_>>();
    expected.sort();
    let mut cursor = None;
    let mut all = Vec::new();
    let mut budget = WorkBudget::default();
    for _ in 0..expected.len() + 2 {
        let page = complete
            .commit_prefix_candidates("", cursor.as_ref(), 2, &mut budget)
            .unwrap();
        // A host can now apply membership/selection facts: no state mutex is
        // retained by the page or its fixed metadata tokens.
        for node in &page.nodes {
            assert!(complete.has_id(node.commit(), &mut budget).unwrap());
        }
        all.extend(page.nodes.iter().map(ScopedNode::commit));
        if page.complete {
            assert!(page.cursor.is_none());
            break;
        }
        cursor = page.cursor;
    }
    assert_eq!(all, expected);
    for prefix in [
        &hex(&nodes[0].commit.0)[..1],
        &hex(&nodes[0].commit.0)[..2],
        &hex(&nodes[0].commit.0),
    ] {
        let page = complete
            .commit_prefix_candidates(
                prefix,
                None,
                MAX_PREFIX_CANDIDATE_SCAN,
                &mut WorkBudget::default(),
            )
            .unwrap();
        let matching = expected
            .iter()
            .copied()
            .filter(|id| hex(&id.0).starts_with(prefix))
            .collect::<Vec<_>>();
        assert_eq!(
            page.nodes
                .iter()
                .map(ScopedNode::commit)
                .collect::<Vec<_>>(),
            matching
        );
        assert_eq!(
            page.scanned,
            matching.len(),
            "odd nibble range must not scan the other half byte"
        );
        assert!(page.complete);
    }
}

#[test]
fn lexical_change_candidates_preserve_divergent_pairs_odd_ranges_and_empty_progress() {
    let (_directory, index) = fixture();
    let left = add(&index, "repo", &[m::CommitId::ROOT], 0x11, "visible left");
    let right = add(&index, "repo", &[m::CommitId::ROOT], 0x11, "hidden right");
    let high = add(
        &index,
        "repo",
        &[m::CommitId::ROOT],
        0x1f,
        "upper odd prefix",
    );
    let outside = add(
        &index,
        "repo",
        &[m::CommitId::ROOT],
        0x2a,
        "outside odd prefix",
    );
    let narrow = Arc::new(index.scope("repo", &[left.commit]).unwrap());
    let mut cursor = None;
    let mut scanned = Vec::new();
    let mut tokens = Vec::new();
    let mut empty = 0;
    let mut budget = WorkBudget::default();
    for _ in 0..4 {
        let page = narrow
            .change_prefix_candidates("11", cursor.as_ref(), 1, &mut budget)
            .unwrap();
        tokens.extend(page.nodes.iter().map(|n| (n.change_id(), n.commit())));
        if page.complete {
            break;
        }
        if page.nodes.is_empty() {
            empty += 1;
        }
        let next = page.cursor.unwrap();
        scanned.push(next.last_scanned());
        cursor = Some(next);
    }
    assert_eq!(tokens, vec![(left.change_id, left.commit)]);
    assert_eq!(empty, 1);
    assert!(scanned.windows(2).all(|p| p[0] < p[1]));
    let mut expected = vec![
        (left.change_id, left.commit),
        (right.change_id, right.commit),
    ];
    expected.sort();
    assert_eq!(scanned, expected);
    let full = Arc::new(
        index
            .scope(
                "repo",
                &[left.commit, right.commit, high.commit, outside.commit],
            )
            .unwrap(),
    );
    let page = full
        .change_prefix_candidates(
            "1",
            None,
            MAX_PREFIX_CANDIDATE_SCAN,
            &mut WorkBudget::default(),
        )
        .unwrap();
    let mut expected = vec![
        (left.change_id, left.commit),
        (right.change_id, right.commit),
        (high.change_id, high.commit),
    ];
    expected.sort();
    assert_eq!(
        page.nodes
            .iter()
            .map(|n| (n.change_id(), n.commit()))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(page.scanned, 3);
    assert!(page.complete);
    let root = full
        .change_prefix_candidates("0", None, 1, &mut WorkBudget::default())
        .unwrap();
    assert_eq!(root.nodes[0].commit(), m::CommitId::ROOT);
    assert_eq!(
        root.cursor.unwrap().last_scanned(),
        (m::ChangeId::ROOT, m::CommitId::ROOT)
    );
    assert_eq!(
        full.resolve_change_prefix("11", &mut WorkBudget::default())
            .unwrap(),
        PrefixResolution::Single(ChangeTargets {
            change_id: left.change_id,
            commits: {
                let mut c = vec![left.commit, right.commit];
                c.sort();
                c
            }
        })
    );
}

#[test]
fn lexical_cursor_policy_and_resource_errors_never_become_absent_prefix_results() {
    let (_directory, index) = fixture();
    let node = add(&index, "repo", &[m::CommitId::ROOT], 0x11, "candidate");
    let scope = Arc::new(index.scope("repo", &[node.commit]).unwrap());
    let first = scope
        .commit_prefix_candidates("", None, 1, &mut WorkBudget::default())
        .unwrap();
    let cursor = first.cursor.unwrap();
    let same_roots = Arc::new(index.scope("repo", &[node.commit]).unwrap());
    assert!(matches!(
        same_roots.commit_prefix_candidates("", Some(&cursor), 1, &mut WorkBudget::default()),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        scope.commit_prefix_candidates("1", Some(&cursor), 1, &mut WorkBudget::default()),
        Err(Error::Invalid(_))
    ));
    for prefix in ["A", "xyz", &"0".repeat(65)] {
        assert!(matches!(
            scope.commit_prefix_candidates(prefix, None, 1, &mut WorkBudget::default()),
            Err(Error::Invalid(_))
        ));
    }
    for limit in [0, MAX_PREFIX_CANDIDATE_SCAN + 1] {
        assert!(matches!(
            scope.change_prefix_candidates("", None, limit, &mut WorkBudget::default()),
            Err(Error::Invalid(_))
        ));
    }
    let mut no_nodes = WorkBudget::new(0, 100, 10000, 100);
    assert!(matches!(
        scope.commit_prefix_candidates("", None, 1, &mut no_nodes),
        Err(Error::BudgetExceeded)
    ));
    assert_eq!(no_nodes.nodes_read(), 0);
    let mut no_bytes = WorkBudget::new(100, 100, 0, 100);
    assert!(matches!(
        scope.commit_prefix_candidates(&hex(&node.commit.0), None, 1, &mut no_bytes),
        Err(Error::BudgetExceeded)
    ));
    assert_eq!(no_bytes.bytes_read(), 0);
    let mut no_frontier = WorkBudget::new(100, 100, 10000, 2);
    assert!(matches!(
        scope.commit_prefix_candidates("", None, 1, &mut no_frontier),
        Err(Error::BudgetExceeded)
    ));
    assert_eq!(no_frontier.nodes_read(), 0);
    let epoch = index.epoch();
    index.state.lock().unwrap().header.epoch[0] ^= 1;
    assert!(matches!(
        scope.commit_prefix_candidates("", Some(&cursor), 1, &mut WorkBudget::default()),
        Err(Error::Unavailable)
    ));
    index.state.lock().unwrap().header.epoch = epoch;
    let retry = scope
        .commit_prefix_candidates("", Some(&cursor), 1, &mut WorkBudget::default())
        .unwrap();
    assert_eq!(retry.nodes[0].commit(), node.commit);
}

#[test]
fn content_manifests_publish_atomically_and_survive_restart_without_graph_changes() {
    use crate::content::{ContentEntry, ContentLimits};
    let (dir, index) = fixture();
    let limits = ContentLimits::default();
    let before = index.stats().unwrap();
    let root = m::TreeId([42; 32]);
    let bytes = b"verified example needle bytes";
    let file = m::file_id(bytes);
    let entries = vec![ContentEntry {
        path: "src/example.rs".into(),
        file,
        size: bytes.len() as u64,
    }];
    assert!(matches!(
        index.put_content_manifest("example", root, &entries, &limits),
        Err(Error::NotReady)
    ));
    assert!(
        index
            .content_manifest("example", root, &limits)
            .unwrap()
            .is_none()
    );
    let mut filter = tinytree_search::FilterBuilder::new();
    filter.update(bytes);
    index
        .put_content_file(
            "example",
            file,
            bytes.len() as u64,
            filter.finish(),
            &limits,
        )
        .unwrap();
    let digest = index
        .put_content_manifest("example", root, &entries, &limits)
        .unwrap();
    assert_eq!(
        index
            .put_content_manifest("example", root, &entries, &limits)
            .unwrap(),
        digest
    );
    assert_eq!(index.stats().unwrap(), before);
    assert!(
        index
            .content_file("other", file, bytes.len() as u64, &limits)
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .content_manifest("other", root, &limits)
            .unwrap()
            .is_none()
    );
    drop(index);
    let index = Index::open(dir.path().join("graph"), &Limits::default()).unwrap();
    let manifest = index
        .content_manifest("example", root, &limits)
        .unwrap()
        .unwrap();
    assert_eq!(manifest.entries, entries);
    assert_eq!(manifest.digest, digest);
    assert!(
        index
            .content_file("example", file, bytes.len() as u64, &limits)
            .unwrap()
            .unwrap()
            .filter
            .may_match(b"needle")
    );
    assert_eq!(index.stats().unwrap(), before);
    let unsorted = vec![entries[0].clone(), entries[0].clone()];
    assert!(matches!(
        index.put_content_manifest("example", m::TreeId([43; 32]), &unsorted, &limits),
        Err(Error::Invalid(_))
    ));
    assert!(
        index
            .content_manifest("example", m::TreeId([43; 32]), &limits)
            .unwrap()
            .is_none()
    );
}

#[test]
fn content_fifo_eviction_cannot_turn_missing_filters_into_negative_results() {
    use crate::content::{ContentEntry, ContentLimits};
    let (dir, index) = fixture();
    let limits = ContentLimits {
        max_bytes: 2048,
        max_records: 2,
        ..Default::default()
    };
    let root = m::TreeId([44; 32]);
    let one = b"first content";
    let two = b"second content";
    let first = m::file_id(one);
    let second = m::file_id(two);
    let put = |id, bytes: &[u8]| {
        let mut filter = tinytree_search::FilterBuilder::new();
        filter.update(bytes);
        index
            .put_content_file("example", id, bytes.len() as u64, filter.finish(), &limits)
            .unwrap();
    };
    put(first, one);
    let entries = vec![ContentEntry {
        path: "one".into(),
        file: first,
        size: one.len() as u64,
    }];
    index
        .put_content_manifest("example", root, &entries, &limits)
        .unwrap();
    put(second, two); // FIFO evicts the filter, not a canonical object or graph row.
    assert!(
        index
            .content_manifest("example", root, &limits)
            .unwrap()
            .is_some()
    );
    assert!(
        index
            .content_file("example", first, one.len() as u64, &limits)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        index.put_content_manifest("example", root, &entries, &limits),
        Err(Error::NotReady)
    ));
    drop(index);
    let index = Index::open(dir.path().join("graph"), &Limits::default()).unwrap();
    assert!(
        index
            .content_file("example", first, one.len() as u64, &limits)
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .content_file("example", second, two.len() as u64, &limits)
            .unwrap()
            .is_some()
    );
}

#[test]
fn content_records_detect_corruption_without_disabling_graph_facts() {
    use crate::content::ContentLimits;
    let (dir, index) = fixture();
    let limits = ContentLimits::default();
    let bytes = b"content whose derived checksum will be corrupted";
    let id = m::file_id(bytes);
    let mut filter = tinytree_search::FilterBuilder::new();
    filter.update(bytes);
    index
        .put_content_file("example", id, bytes.len() as u64, filter.finish(), &limits)
        .unwrap();
    let before = index.stats().unwrap();
    {
        let state = index.state.lock().unwrap();
        let table = state
            .db
            .keyspace("content-search-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let key = key(b'f', &repo_hash("example").unwrap(), &id.0);
        let mut raw = table.get(&key).unwrap().unwrap().to_vec();
        let last = raw.len() - 1;
        raw[last] ^= 1;
        let mut tx = state.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&table, key, raw);
        tx.commit().unwrap();
    }
    assert!(matches!(
        index.content_file("example", id, bytes.len() as u64, &limits),
        Err(Error::Corrupt)
    ));
    assert_eq!(index.stats().unwrap(), before);
    drop(index);
    let index = Index::open(dir.path().join("graph"), &Limits::default()).unwrap();
    assert_eq!(index.stats().unwrap(), before);
    assert!(matches!(
        index.content_file("example", id, bytes.len() as u64, &limits),
        Err(Error::Corrupt)
    ));
}

#[test]
fn permanently_infeasible_content_manifests_are_not_retryable_cache_misses() {
    use crate::content::{ContentEntry, ContentLimits};
    let (_dir, index) = fixture();
    let before = index.stats().unwrap();
    let limits = ContentLimits {
        max_records: 16,
        ..Default::default()
    };
    let root = m::TreeId([45; 32]);
    let entries = (0..17)
        .map(|n| ContentEntry {
            path: format!("file-{n:02}"),
            file: m::file_id(&[n as u8]),
            size: 1,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        limits.check_manifest(root, &entries),
        Err(Error::BudgetExceeded)
    ));
    assert!(matches!(
        index.put_content_manifest("example", root, &entries, &limits),
        Err(Error::BudgetExceeded)
    ));
    assert!(
        index.state.lock().unwrap().content.is_none(),
        "permanent quota rejection must precede header initialization"
    );
    assert_eq!(index.stats().unwrap(), before);
    let feasible = &entries[..15];
    limits.check_manifest(root, feasible).unwrap();
    assert!(matches!(
        index.put_content_manifest("example", root, feasible, &limits),
        Err(Error::NotReady)
    ));
    assert!(
        index
            .content_manifest("example", root, &limits)
            .unwrap()
            .is_none()
    );
    assert_eq!(index.stats().unwrap(), before);

    let bytes = ContentLimits {
        max_bytes: 1024,
        ..Default::default()
    };
    assert!(matches!(
        bytes.check_manifest(root, &entries[..2]),
        Err(Error::BudgetExceeded)
    ));
    let repeated = entries
        .iter()
        .map(|entry| ContentEntry {
            file: entries[0].file,
            ..entry.clone()
        })
        .collect::<Vec<_>>();
    let shared = ContentLimits {
        max_records: 2,
        ..Default::default()
    };
    shared.check_manifest(root, &repeated).unwrap(); // Duplicate paths share one FILE filter.
}

// Spawned only by the public-API recovery case below. process::exit deliberately
// bypasses Index/Fjall Drop so graceful journal flushing cannot establish proof.
#[test]
fn content_durability_child() {
    let Some(path) = std::env::var_os("TINYTREE_INDEX_CONTENT_CHILD_PATH") else {
        return;
    };
    let mode = std::env::var("TINYTREE_INDEX_CONTENT_CHILD_MODE").unwrap();
    let index = Index::open(Path::new(&path), &Limits::default()).unwrap();
    let limits = content::ContentLimits::default();
    let bytes = b"first verified needle content";
    let file = m::file_id(bytes);
    let mut filter = tinytree_search::FilterBuilder::new();
    filter.update(bytes);
    index
        .put_content_file(
            "example",
            file,
            bytes.len() as u64,
            filter.finish(),
            &limits,
        )
        .unwrap();
    let root = m::TreeId([46; 32]);
    let entries = vec![content::ContentEntry {
        path: "first".into(),
        file,
        size: bytes.len() as u64,
    }];
    if mode != "partial" {
        index
            .put_content_manifest("example", root, &entries, &limits)
            .unwrap();
    }
    if mode == "idempotent" {
        let second = b"second verified needle content";
        let id = m::file_id(second);
        let mut filter = tinytree_search::FilterBuilder::new();
        filter.update(second);
        index
            .put_content_file("example", id, second.len() as u64, filter.finish(), &limits)
            .unwrap();
        // The old manifest already exists: this call must nevertheless flush
        // the newly journaled filter rather than return early without syncing.
        index
            .put_content_manifest("example", root, &entries, &limits)
            .unwrap();
    }
    std::process::exit(0);
}

#[test]
fn completed_content_publication_survives_exit_without_destructor_flush() {
    use crate::content::{ContentEntry, ContentLimits};
    for mode in ["partial", "complete", "idempotent"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("graph");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::content_durability_child", "--nocapture"])
            .env("TINYTREE_INDEX_CONTENT_CHILD_PATH", &path)
            .env("TINYTREE_INDEX_CONTENT_CHILD_MODE", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let index = Index::open(&path, &Limits::default()).unwrap();
        let limits = ContentLimits::default();
        let root = m::TreeId([46; 32]);
        let bytes = b"first verified needle content";
        let file = m::file_id(bytes);
        let manifest = index.content_manifest("example", root, &limits).unwrap();
        if mode == "partial" {
            assert!(
                manifest.is_none(),
                "unpublished filters are not a complete revision"
            );
            // A recovered filter is optional before publication; rebuilding it
            // and atomically completing the revision is always safe.
            let mut filter = tinytree_search::FilterBuilder::new();
            filter.update(bytes);
            index
                .put_content_file(
                    "example",
                    file,
                    bytes.len() as u64,
                    filter.finish(),
                    &limits,
                )
                .unwrap();
            index
                .put_content_manifest(
                    "example",
                    root,
                    &[ContentEntry {
                        path: "first".into(),
                        file,
                        size: bytes.len() as u64,
                    }],
                    &limits,
                )
                .unwrap();
        } else {
            assert_eq!(
                manifest.unwrap().entries,
                vec![ContentEntry {
                    path: "first".into(),
                    file,
                    size: bytes.len() as u64
                }]
            );
            assert!(
                index
                    .content_file("example", file, bytes.len() as u64, &limits)
                    .unwrap()
                    .unwrap()
                    .filter
                    .may_match(b"needle")
            );
        }
        if mode == "idempotent" {
            let second = b"second verified needle content";
            assert!(
                index
                    .content_file("example", m::file_id(second), second.len() as u64, &limits)
                    .unwrap()
                    .is_some(),
                "idempotent completion must synchronize preceding buffered filter writes"
            );
        }
        assert_eq!(
            index.stats().unwrap(),
            IndexStats {
                repositories: 0,
                nodes: 0,
                logical_bytes: 0
            }
        );
    }
}

#[test]
#[ignore = "manual paired durability component benchmark"]
fn benchmark_paired_content_filter_durability() {
    use crate::content::{ContentEntry, ContentLimits};
    use std::time::Instant;
    let overall = Instant::now();
    for count in [64usize, 1024] {
        // Verify and derive every input outside the timed publication phases.
        let mut filters = Vec::with_capacity(count);
        let mut entries = Vec::with_capacity(count);
        let mut tree_entries = Vec::with_capacity(count);
        for n in 0..count {
            let mut bytes = vec![b'a'; 4096];
            let label = format!("derived filter fixture {n:05}\n");
            bytes[..label.len()].copy_from_slice(label.as_bytes());
            let descriptor = m::describe_file(&bytes);
            m::verify(&descriptor, &bytes, m::Limits::default()).unwrap();
            let file = m::FileId(descriptor.key.id.0);
            let mut filter = tinytree_search::FilterBuilder::new();
            filter.update(&bytes);
            filters.push((file, descriptor.size_bytes, filter.finish()));
            let path = format!("file-{n:05}");
            entries.push(ContentEntry {
                path: path.clone(),
                file,
                size: descriptor.size_bytes,
            });
            tree_entries.push(m::TreeEntry {
                name: path,
                value: m::TreeValue::File {
                    id: file,
                    executable: false,
                    size_bytes: descriptor.size_bytes,
                    copy_id: None,
                },
            });
        }
        let tree = m::encode(
            &m::Object::Tree(m::Tree {
                entries: tree_entries,
            }),
            m::Limits::default(),
        )
        .unwrap();
        m::verify(&tree.descriptor, &tree.bytes, m::Limits::default()).unwrap();
        let root = m::TreeId(tree.descriptor.key.id.0);
        let limits = ContentLimits::default();
        limits.check_manifest(root, &entries).unwrap();
        for pair in 0..5 {
            let order = if pair % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            let mut pair_metadata = None;
            for (order_index, synced) in order.into_iter().enumerate() {
                assert!(
                    overall.elapsed().as_secs() < 60,
                    "paired component benchmark elapsed budget exceeded"
                );
                let directory = tempfile::tempdir().unwrap();
                let index =
                    Index::open(directory.path().join("graph"), &Limits::default()).unwrap();
                // Cache/keyspace initialization has its own fixed header barrier
                // and is deliberately outside both measured profiles.
                assert!(
                    index
                        .content_manifest("example", root, &limits)
                        .unwrap()
                        .is_none()
                );
                let graph_before = index.stats().unwrap();
                let started = Instant::now();
                for (file, size, filter) in &filters {
                    if synced {
                        index
                            .put_content_file_synced_for_benchmark(
                                "example",
                                *file,
                                *size,
                                filter.clone(),
                                &limits,
                            )
                            .unwrap();
                    } else {
                        index
                            .put_content_file("example", *file, *size, filter.clone(), &limits)
                            .unwrap();
                    }
                }
                let filter_ns = started.elapsed().as_nanos() as u64;
                let complete_started = Instant::now();
                let digest = index
                    .put_content_manifest("example", root, &entries, &limits)
                    .unwrap();
                let complete_ns = complete_started.elapsed().as_nanos() as u64;
                let total_ns = started.elapsed().as_nanos() as u64;
                // Every recorded sample is semantically complete, not a faster
                // incomplete-write profile. Verification is outside its timer.
                let manifest = index
                    .content_manifest("example", root, &limits)
                    .unwrap()
                    .unwrap();
                assert_eq!(manifest.digest, digest);
                assert_eq!(manifest.entries, entries);
                for (file, size, filter) in &filters {
                    assert_eq!(
                        index
                            .content_file("example", *file, *size, &limits)
                            .unwrap()
                            .unwrap()
                            .filter,
                        *filter
                    );
                }
                assert_eq!(index.stats().unwrap(), graph_before);
                let (records, logical_bytes) = {
                    let state = index.state.lock().unwrap();
                    let table = state
                        .db
                        .keyspace("content-search-v1", KeyspaceCreateOptions::default)
                        .unwrap();
                    let mut records = 0;
                    let mut bytes = 0u64;
                    for item in state.db.read_tx().prefix(&table, b"") {
                        let (key, value) = item.into_inner().unwrap();
                        if key.as_ref() != b"header" {
                            records += 1;
                            bytes += (key.len() + value.len()) as u64;
                        }
                    }
                    (records, bytes)
                };
                assert_eq!(records, count + 1);
                if let Some(metadata) = pair_metadata {
                    assert_eq!(metadata, (records, logical_bytes));
                }
                pair_metadata = Some((records, logical_bytes));
                println!(
                    "TINYTREE_CONTENT_FILTER_DURABILITY_PAIR_V1 {}",
                    serde_json::json!({
                        "component":"derived_filter_publication", "files":count,"pair":pair,"order_index":order_index,
                        "profile":if synced {"sync_per_filter"} else {"buffered_filters"},
                        "filter_sync_barriers":if synced {count} else {0},"complete_sync_barriers":1,
                        "filter_phase_ns":filter_ns,"complete_phase_ns":complete_ns,"elapsed_ns":total_ns,
                        "records":records,"logical_metadata_bytes":logical_bytes,"complete":true,
                        "scope":"same action; fresh directories; verified input/filter/root construction and database/cache initialization excluded; derived writes and complete barrier only, not RPC or user latency",
                    })
                );
            }
        }
    }
}
