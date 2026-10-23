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
fn oversized_escaped_repository_record_rejects_without_persisting_bad_state() {
    let directory = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_record_bytes: 1024,
        ..Limits::default()
    };
    let path = directory.path().join("graph");
    let index = Index::open(&path, &limits).unwrap();
    let repository = "\"\\".repeat(256);
    assert_eq!(repository.len(), 512);
    let commit = canonical(&[m::CommitId::ROOT], 1, "small commit");
    assert!(matches!(
        index.ingest(&repository, commit.descriptor, &commit.bytes),
        Err(Error::Invalid(_))
    ));
    assert!(
        index
            .node(&repository, m::CommitId(commit.descriptor.key.id.0))
            .unwrap()
            .is_none()
    );
    let ready = index
        .ingest("repo", commit.descriptor, &commit.bytes)
        .unwrap();
    assert_eq!(ready.position, 1);
    drop(index);
    let reopened = Index::open(&path, &limits).unwrap();
    assert_eq!(reopened.node("repo", ready.commit).unwrap(), Some(ready));
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
                if scope.resolve_commit_prefix(&text[..8], &mut prefix_budget)?
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
            println!(
                "{}",
                serde_json::json!({
                    "benchmark": "tinytree_index_storage_v1", "shape": shape, "commits": size,
                    "canonical_commit_bytes": canonical_bytes, "logical_record_bytes": stats.logical_bytes,
                    "closed_file_bytes": closed_file_bytes, "final_file_bytes": final_file_bytes,
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
    shared.sort();
    assert_eq!(
        scope
            .common_ancestors(&[merges[0]], &[merges[15]], &mut WorkBudget::default())
            .unwrap(),
        shared
    );
}
