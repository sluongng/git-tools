use jj_lib::diff::{ContentDiff, LineDiffBudget, LineDiffError, LineDiffLimits};

fn limits() -> LineDiffLimits {
    LineDiffLimits {
        max_input_bytes: 1 << 24,
        max_lines: 1 << 20,
        max_work: 1 << 28,
        max_allocation_bytes: 1 << 28,
        max_recursion_depth: 64,
    }
}
fn compare(left: &[u8], right: &[u8]) {
    let expected = ContentDiff::by_line([left, right])
        .hunks()
        .collect::<Vec<_>>();
    let actual =
        ContentDiff::by_line_bounded([left, right], &mut LineDiffBudget::new(limits()), || true)
            .unwrap();
    assert_eq!(
        actual.hunks().collect::<Vec<_>>(),
        expected,
        "{left:?} -> {right:?}"
    );
}
#[test]
fn bounded_line_diff_matches_literal_policy() {
    // Exhaust small repeated-line permutations, including all short gaps
    // around rare anchors. Different hash-table seeds must not change ties.
    let mut texts = vec![vec![]];
    for len in 1..=4 {
        for mut value in 0..3usize.pow(len) {
            let mut text = vec![];
            for _ in 0..len {
                text.push(b'a' + (value % 3) as u8);
                text.push(b'\n');
                value /= 3;
            }
            texts.push(text);
        }
    }
    for left in &texts {
        for right in &texts {
            compare(left, right);
        }
    }
    for (left, right) in [
        (&b"a\na\nb\na\n"[..], &b"a\nb\na\na\n"[..]),
        (
            &b"without final newline"[..],
            &b"without final newline\n"[..],
        ),
        (&b"\0\xff\n\xfe"[..], &b"\0\xff\n\xfd"[..]),
    ] {
        compare(left, right);
    }
    compare(&b"a\n".repeat(120), &b"a\n".repeat(121));
    compare(&b"a\n".repeat(120), &b"b\n".repeat(121));
}

#[test]
fn bounded_multi_input_diff_matches_stock_intersections() {
    let mut texts = vec![vec![]];
    for len in 1..=2 {
        for mut value in 0..2usize.pow(len) {
            let mut text = Vec::new();
            for _ in 0..len {
                text.push(b'a' + (value % 2) as u8);
                text.push(b'\n');
                value /= 2;
            }
            texts.push(text);
        }
    }
    let compare = |inputs: &[&[u8]]| {
        let expected = ContentDiff::by_line(inputs.iter().copied())
            .hunks().collect::<Vec<_>>();
        let actual = ContentDiff::by_line_bounded_many(
            inputs, &mut LineDiffBudget::new(limits()), || true,
        ).unwrap();
        assert_eq!(actual.hunks().collect::<Vec<_>>(), expected, "{inputs:?}");
    };
    for a in &texts {
        compare(&[a]);
        for b in &texts {
            for c in &texts {
                compare(&[a, b, c]);
                for d in &texts {
                    compare(&[a, b, c, d]);
                }
            }
        }
    }
    compare(&[b"x\na\nb\nx\n", b"a\nx\nb\n", b"x\nb\na\n", b"a\nb\nx\n", b"x\na\nx\n"]);
    compare(&[b"no newline", b"no newline\n", b"\0\xff\nno newline"]);
    compare(&[&b"same\n".repeat(120), &b"same\n".repeat(121), &b"same\n".repeat(122)]);
}

#[test]
fn bounded_multi_input_diff_refuses_cancellation_and_aggregate_limits() {
    assert_eq!(
        ContentDiff::by_line_bounded_many(&[], &mut LineDiffBudget::new(limits()), || true).unwrap_err(),
        LineDiffError::LimitExceeded,
    );
    let inputs = [b"a\nx\nb\n".as_slice(), b"a\ny\nb\n", b"x\na\nb\n"];
    let mut generous = LineDiffBudget::new(limits());
    ContentDiff::by_line_bounded_many(&inputs, &mut generous, || true).unwrap();
    // A third input must not reuse the pair's input/allocation charge for free.
    // Work includes seeded histogram probes, so use a fixed exhausted budget
    // rather than assuming two independent calls perform identical probe work.
    for constraint in ["input", "work", "allocation"] {
        let mut cap = limits();
        match constraint {
            "input" => cap.max_input_bytes = generous.usage().input_bytes - 1,
            "work" => cap.max_work = 0,
            "allocation" => cap.max_allocation_bytes = generous.usage().allocation_bytes - 1,
            _ => unreachable!(),
        }
        assert_eq!(
            ContentDiff::by_line_bounded_many(&inputs, &mut LineDiffBudget::new(cap), || true).unwrap_err(),
            LineDiffError::LimitExceeded, "{constraint}",
        );
    }
    let text = (0..60).map(|i| format!("line {}\n", i % 23)).collect::<String>();
    let inputs = [text.as_bytes(), text.as_bytes(), text.as_bytes()];
    for stop_at in 1..=512 {
        let mut checks = 0;
        let result = ContentDiff::by_line_bounded_many(&inputs, &mut LineDiffBudget::new(limits()), || {
            checks += 1;
            checks < stop_at
        });
        assert_eq!(result.unwrap_err(), LineDiffError::Cancelled);
        assert_eq!(checks, stop_at);
    }
}
#[test]
fn bounded_line_diff_rejects_before_unadmitted_allocations() {
    let mut l = limits();
    l.max_input_bytes = 0;
    let mut b = LineDiffBudget::new(l);
    assert_eq!(
        ContentDiff::by_line_bounded([b"a", b"b"], &mut b, || true).unwrap_err(),
        LineDiffError::LimitExceeded
    );
    assert_eq!(b.usage().allocation_bytes, 0);
    let mut l = limits();
    l.max_lines = 0;
    let mut b = LineDiffBudget::new(l);
    assert_eq!(
        ContentDiff::by_line_bounded([b"a", b"b"], &mut b, || true).unwrap_err(),
        LineDiffError::LimitExceeded
    );
    assert_eq!(b.usage().allocation_bytes, 0);
    let mut l = limits();
    l.max_allocation_bytes = 0;
    assert_eq!(
        ContentDiff::by_line_bounded([b"a", b"b"], &mut LineDiffBudget::new(l), || true)
            .unwrap_err(),
        LineDiffError::LimitExceeded
    );
}
#[test]
fn bounded_line_diff_shares_work_across_calls() {
    let mut b = LineDiffBudget::new(limits());
    ContentDiff::by_line_bounded([b"same\n", b"same\n"], &mut b, || true).unwrap();
    let work = b.usage().work;
    let mut l = limits();
    l.max_work = work;
    let mut b = LineDiffBudget::new(l);
    ContentDiff::by_line_bounded([b"same\n", b"same\n"], &mut b, || true).unwrap();
    assert_eq!(
        ContentDiff::by_line_bounded([b"same\n", b"same\n"], &mut b, || true).unwrap_err(),
        LineDiffError::LimitExceeded
    );
    assert!(b.usage().work > work);
}
#[test]
fn bounded_line_diff_stops_after_first_cancellation() {
    let text = (0..400)
        .map(|i| format!("line {}\n", i % 200))
        .collect::<String>();
    // Exercise cancellation across scanning, hashing and hash-table probes.
    // A failed probe must not keep asking the predicate about other buckets.
    for stop_at in 1..=1_024 {
        let mut checks = 0;
        let result = ContentDiff::by_line_bounded(
            [text.as_bytes(), text.as_bytes()],
            &mut LineDiffBudget::new(limits()),
            || {
                checks += 1;
                checks < stop_at
            },
        );
        assert_eq!(result.unwrap_err(), LineDiffError::Cancelled);
        assert_eq!(checks, stop_at);
    }
}
#[test]
fn bounded_line_diff_stops_recursion_and_quadratic_work() {
    let mut l = limits();
    l.max_recursion_depth = 1;
    assert_eq!(
        ContentDiff::by_line_bounded(
            [b"a\nx\nc\n", b"a\ny\nc\n"],
            &mut LineDiffBudget::new(l),
            || true
        )
        .unwrap_err(),
        LineDiffError::LimitExceeded
    );
    let left = (0..100).map(|i| format!("{i}\n")).collect::<String>();
    let right = (0..100).rev().map(|i| format!("{i}\n")).collect::<String>();
    let mut l = limits();
    l.max_work = 16_000;
    assert_eq!(
        ContentDiff::by_line_bounded(
            [left.as_bytes(), right.as_bytes()],
            &mut LineDiffBudget::new(l),
            || true
        )
        .unwrap_err(),
        LineDiffError::LimitExceeded
    );
    let mut checks = 0;
    assert_eq!(
        ContentDiff::by_line_bounded(
            [left.as_bytes(), right.as_bytes()],
            &mut LineDiffBudget::new(limits()),
            || {
                checks += 1;
                checks < 2_500
            }
        )
        .unwrap_err(),
        LineDiffError::Cancelled
    );
    assert_eq!(checks, 2_500);
}

/// Algorithm-only quota/cost probe. Run explicitly with `--ignored --nocapture`;
/// this does not measure remote RPCs, filesystem hydration or end-user latency.
#[test]
#[ignore = "manual algorithm quota/cost benchmark"]
fn bounded_line_history_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;

    const VERSIONS: usize = 32;
    for lines in [128usize, 1_024, 4_096] {
        // Alternate repeated lines with stable unique anchors. Each revision
        // changes one anchor, preserving a genuine chain without a pathological
        // reversed sequence that would dominate the unbounded baseline runtime.
        let versions = (0..=VERSIONS)
            .map(|revision| {
                (0..lines)
                    .map(|line| {
                        if line % 2 == 0 {
                            "repeated line\n".to_owned()
                        } else {
                            let changed_at = 2 * (revision % (lines / 2)) + 1;
                            let generation = if line == changed_at { revision } else { 0 };
                            format!("anchor {line:05} generation {generation:02}\n")
                        }
                    })
                    .collect::<String>()
                    .into_bytes()
            })
            .collect::<Vec<_>>();
        let input_bytes = versions
            .windows(2)
            .map(|pair| pair[0].len() + pair[1].len())
            .sum::<usize>();
        let start = Instant::now();
        let mut baseline_matching_bytes = 0usize;
        for pair in versions.windows(2) {
            let diff = ContentDiff::by_line([pair[1].as_slice(), pair[0].as_slice()]);
            baseline_matching_bytes += diff
                .hunks()
                .filter(|hunk| hunk.kind == jj_lib::diff::DiffHunkKind::Matching)
                .map(|hunk| hunk.contents[0].len())
                .sum::<usize>();
        }
        black_box(baseline_matching_bytes);
        println!(
            "{{\"benchmark\":\"bounded_line_history\",\"lines\":{lines},\"versions\":{VERSIONS},\"profile\":\"original\",\"completed_versions\":{VERSIONS},\"attempted_versions\":{VERSIONS},\"complete\":true,\"input_bytes\":{input_bytes},\"work\":null,\"allocation_bytes\":null,\"elapsed_ns\":{},\"matching_bytes\":{baseline_matching_bytes}}}",
            start.elapsed().as_nanos()
        );
        for (profile, memory) in [
            ("default_8mib", 8usize << 20),
            ("memory_32mib", 32usize << 20),
        ] {
            let mut budget = LineDiffBudget::new(LineDiffLimits {
                max_input_bytes: 16 << 20,
                max_lines: 1 << 20,
                max_work: 1_000_000,
                max_allocation_bytes: memory,
                max_recursion_depth: 64,
            });
            let start = Instant::now();
            let mut completed = 0;
            let mut attempted = 0;
            let mut matching_bytes = 0usize;
            let mut failure = "none";
            for pair in versions.windows(2) {
                attempted += 1;
                match ContentDiff::by_line_bounded(
                    [pair[1].as_slice(), pair[0].as_slice()],
                    &mut budget,
                    || true,
                ) {
                    Ok(diff) => {
                        matching_bytes += diff
                            .hunks()
                            .filter(|hunk| hunk.kind == jj_lib::diff::DiffHunkKind::Matching)
                            .map(|hunk| hunk.contents[0].len())
                            .sum::<usize>();
                        completed += 1;
                    }
                    Err(LineDiffError::LimitExceeded) => {
                        failure = "limit_exceeded";
                        break;
                    }
                    Err(LineDiffError::Cancelled) => {
                        panic!("unconditional benchmark check cancelled")
                    }
                }
            }
            black_box(matching_bytes);
            let usage = budget.usage();
            println!(
                "{{\"benchmark\":\"bounded_line_history\",\"lines\":{lines},\"versions\":{VERSIONS},\"profile\":\"{profile}\",\"completed_versions\":{completed},\"attempted_versions\":{attempted},\"complete\":{},\"failure\":\"{failure}\",\"input_bytes\":{},\"work\":{},\"allocation_bytes\":{},\"max_input_bytes\":16777216,\"max_work\":1000000,\"max_allocation_bytes\":{memory},\"elapsed_ns\":{},\"matching_bytes\":{matching_bytes}}}",
                completed == VERSIONS,
                usage.input_bytes,
                usage.work,
                usage.allocation_bytes,
                start.elapsed().as_nanos()
            );
            if completed == VERSIONS {
                assert_eq!(matching_bytes, baseline_matching_bytes);
            }
        }
    }
}

fn line_options(same_change: jj_lib::merge::SameChange) -> jj_lib::tree_merge::MergeOptions {
    jj_lib::tree_merge::MergeOptions {
        hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
        same_change,
    }
}

#[test]
fn bounded_file_merge_preserves_stock_line_policy() {
    use jj_lib::{
        files,
        merge::{Merge, SameChange},
    };
    struct Case {
        name: &'static str,
        terms: Vec<&'static [u8]>,
    }
    let cases = [
        Case {
            name: "resolved input",
            terms: vec![b"one\ntwo\n"],
        },
        Case {
            name: "independent edits",
            terms: vec![
                b"ONE\nmiddle\nthree\n",
                b"one\nmiddle\nthree\n",
                b"one\nmiddle\nTHREE\n",
            ],
        },
        Case {
            name: "same change",
            terms: vec![b"new\n", b"base\n", b"new\n"],
        },
        Case {
            name: "overlapping changes",
            terms: vec![b"left\n", b"base\n", b"right\n"],
        },
        Case {
            name: "repeated line alignment",
            terms: vec![b"a\nb\na\na\n", b"a\na\nb\na\n", b"a\na\nb\na\na\n"],
        },
        Case {
            name: "arbitrary bytes and no final newline",
            terms: vec![b"\0\xff\nleft", b"\0\xff\nbase", b"\0\xff\nright"],
        },
        Case {
            name: "canceling five terms",
            terms: vec![b"left\n", b"base\n", b"right\n", b"right\n", b"base\n"],
        },
        Case {
            name: "multiple remaining sides",
            terms: vec![b"left\n", b"base\n", b"right\n", b"base\n", b"third\n"],
        },
    ];
    for case in cases {
        for same_change in [SameChange::Accept, SameChange::Keep] {
            let input = Merge::from_vec(case.terms.clone());
            let options = line_options(same_change);
            let actual =
                files::merge_bounded(&input, &options, &mut LineDiffBudget::new(limits()), || {
                    true
                })
                .unwrap();
            assert_eq!(
                actual,
                files::merge(&input, &options),
                "{} {same_change:?}",
                case.name
            );
            let actual = files::try_merge_bounded(
                &input,
                &options,
                &mut LineDiffBudget::new(limits()),
                || true,
            )
            .unwrap();
            assert_eq!(
                actual,
                files::try_merge(&input, &options),
                "{} {same_change:?}",
                case.name
            );
        }
    }
    // Exhaust short repeated-content merges; compare public result terms, not
    // a duplicate implementation of the bounded collector.
    let texts: &[&[u8]] = &[b"", b"a", b"a\n", b"a\na\n", b"a\nb\n", b"b\na\n"];
    for left in texts {
        for base in texts {
            for right in texts {
                for same_change in [SameChange::Accept, SameChange::Keep] {
                    let input = Merge::from_vec(vec![*left, *base, *right]);
                    let options = line_options(same_change);
                    assert_eq!(
                        files::merge_bounded(
                            &input,
                            &options,
                            &mut LineDiffBudget::new(limits()),
                            || true
                        )
                        .unwrap(),
                        files::merge(&input, &options)
                    );
                }
            }
        }
    }
}

#[test]
fn bounded_file_merge_enforces_shared_limits_and_cancellation() {
    use jj_lib::{
        files::{self, FileMergeError},
        merge::{Merge, SameChange},
    };
    let input = Merge::from_vec(vec![
        b"ONE\nmiddle\nthree\n".as_slice(),
        b"one\nmiddle\nthree\n",
        b"one\nmiddle\nTHREE\n",
    ]);
    let options = line_options(SameChange::Accept);
    let mut budget = LineDiffBudget::new(limits());
    files::merge_bounded(&input, &options, &mut budget, || true).unwrap();
    let used = budget.usage();
    for (name, bounded) in [
        (
            "work",
            LineDiffLimits {
                max_work: used.work,
                ..limits()
            },
        ),
        (
            "allocation",
            LineDiffLimits {
                max_allocation_bytes: used.allocation_bytes,
                ..limits()
            },
        ),
        (
            "input",
            LineDiffLimits {
                max_input_bytes: used.input_bytes,
                ..limits()
            },
        ),
    ] {
        let mut budget = LineDiffBudget::new(bounded);
        files::merge_bounded(&input, &options, &mut budget, || true).unwrap();
        assert!(
            matches!(
                files::merge_bounded(&input, &options, &mut budget, || true),
                Err(FileMergeError::Budget(LineDiffError::LimitExceeded))
            ),
            "{name}"
        );
    }
    let repeated = b"a\na\nb\na\n".repeat(40);
    let input = Merge::from_vec(vec![
        repeated.as_slice(),
        repeated.as_slice(),
        repeated.as_slice(),
    ]);
    for stop_at in 1..=128 {
        let mut checks = 0;
        let result =
            files::merge_bounded(&input, &options, &mut LineDiffBudget::new(limits()), || {
                checks += 1;
                checks < stop_at
            });
        assert!(matches!(
            result,
            Err(FileMergeError::Budget(LineDiffError::Cancelled))
        ));
        assert_eq!(checks, stop_at);
    }
    let mut word = options.clone();
    word.hunk_level = files::FileMergeHunkLevel::Word;
    let mut budget = LineDiffBudget::new(limits());
    assert!(matches!(
        files::merge_bounded(&input, &word, &mut budget, || true),
        Err(FileMergeError::UnsupportedHunkLevel)
    ));
    assert_eq!(budget.usage(), Default::default());
}

#[test]
fn bounded_selected_value_resolution_preserves_stock_metadata_and_conflicts() {
    use jj_lib::{
        backend::{CopyId, TreeValue},
        merge::{Merge, SameChange},
        repo_path::RepoPath,
        signing::Signer,
        simple_backend::SimpleBackend,
        store::Store,
        tree_merge,
    };
    futures::executor::block_on(async {
        for same_change in [SameChange::Accept, SameChange::Keep] {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::new(
                Box::new(SimpleBackend::init(directory.path())),
                Signer::new(None, vec![]),
                line_options(same_change),
            );
            let path = RepoPath::from_internal_string("selected").unwrap();
            let mut ids = Vec::new();
            for mut content in [
                b"one\nmiddle\nthree\n".as_slice(),
                b"ONE\nmiddle\nthree\n",
                b"one\nmiddle\nTHREE\n",
                b"OTHER\nmiddle\nthree\n",
            ] {
                ids.push(store.write_file(path, &mut content).await.unwrap());
            }
            let value = |at: usize, executable: bool, copy: u8| {
                Some(TreeValue::File {
                    id: ids[at].clone(),
                    executable,
                    copy_id: CopyId::from_bytes(&[copy; 16]),
                })
            };
            let cases = [
                (
                    "text and metadata resolution",
                    Merge::from_vec(vec![
                        value(1, true, 1),
                        value(0, false, 0),
                        value(2, false, 0),
                    ]),
                ),
                (
                    "unresolved text",
                    Merge::from_vec(vec![
                        value(1, false, 0),
                        value(0, false, 0),
                        value(3, false, 0),
                    ]),
                ),
                (
                    "unresolved copy identity",
                    Merge::from_vec(vec![
                        value(1, false, 1),
                        value(0, false, 0),
                        value(2, false, 2),
                    ]),
                ),
                (
                    "absent term",
                    Merge::from_vec(vec![value(1, false, 0), None, value(2, false, 0)]),
                ),
                (
                    "same file identity",
                    Merge::from_vec(vec![
                        value(1, false, 0),
                        value(0, false, 0),
                        value(1, false, 0),
                    ]),
                ),
                (
                    "canceling terms",
                    Merge::from_vec(vec![
                        value(1, false, 0),
                        value(0, false, 0),
                        value(2, false, 0),
                        value(2, false, 0),
                        value(0, false, 0),
                    ]),
                ),
            ];
            for (name, values) in cases {
                let expected = tree_merge::resolve_file_values(&store, path, values.clone())
                    .await
                    .unwrap();
                let actual = tree_merge::resolve_file_values_bounded(
                    &store,
                    path,
                    values,
                    &mut LineDiffBudget::new(limits()),
                    || true,
                )
                .await
                .unwrap();
                assert_eq!(actual, expected, "{name} {same_change:?}");
            }
            let values = Merge::from_vec(vec![
                value(1, false, 0),
                value(0, false, 0),
                value(2, false, 0),
            ]);
            let count = || {
                std::fs::read_dir(directory.path().join("files"))
                    .unwrap()
                    .count()
            };
            let before = count();
            // Three nontrivial terms need rounded collection storage before
            // any body can be read or any derived FILE can be written.
            let mut allocation_limit = limits();
            allocation_limit.max_allocation_bytes =
                3 * std::mem::size_of::<Option<&TreeValue>>();
            assert!(matches!(
                tree_merge::resolve_file_values_bounded(
                    &store,
                    path,
                    values.clone(),
                    &mut LineDiffBudget::new(allocation_limit),
                    || true
                )
                .await,
                Err(tree_merge::BoundedFileResolveError::Budget(
                    LineDiffError::LimitExceeded
                ))
            ));
            assert_eq!(count(), before);
            let mut bounded = limits();
            bounded.max_input_bytes = 1;
            assert!(matches!(
                tree_merge::resolve_file_values_bounded(
                    &store,
                    path,
                    values.clone(),
                    &mut LineDiffBudget::new(bounded),
                    || true
                )
                .await,
                Err(tree_merge::BoundedFileResolveError::Budget(
                    LineDiffError::LimitExceeded
                ))
            ));
            assert_eq!(
                count(),
                before,
                "refused reads must not write a derived file"
            );
            assert!(matches!(
                tree_merge::resolve_file_values_bounded(
                    &store,
                    path,
                    values,
                    &mut LineDiffBudget::new(limits()),
                    || false
                )
                .await,
                Err(tree_merge::BoundedFileResolveError::Budget(
                    LineDiffError::Cancelled
                ))
            ));
            assert_eq!(count(), before);
        }
    });
}
