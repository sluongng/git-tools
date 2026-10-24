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
