use super::*;

fn budget() -> MatchBudget {
    MatchBudget::new(MatchLimits {
        max_input_bytes: 16 * 1024 * 1024,
        max_work: 32 * 1024 * 1024,
        max_output_bytes: 16 * 1024 * 1024,
    })
}

#[test]
fn streamed_filters_never_exclude_present_byte_literals() {
    let mut bytes = b"first\nsecond\0\xffthird repeat repeat\n".to_vec();
    bytes.extend((0..=255).cycle().take(2048));
    for chunk in [1, 2, 3, 7, 31, 512, 4096] {
        let mut builder = FilterBuilder::new();
        for part in bytes.chunks(chunk) {
            builder.update(part);
        }
        let filter = builder.finish();
        for length in [1, 2, 3, 4, 17, 97] {
            for start in 0..bytes.len() - length {
                assert!(
                    filter.may_match(&bytes[start..start + length]),
                    "chunk={chunk} start={start} length={length}"
                );
            }
        }
    }
    let empty = FilterBuilder::new().finish();
    assert!(!empty.may_match(b"needle"));
    assert!(empty.may_match(b"a"));
    assert!(empty.may_match(b"aa"));
}

#[test]
fn paged_literal_positions_match_nonoverlapping_reference_for_arbitrary_bytes() {
    for seed in 0..256usize {
        let alphabet = b"ab\n\0\xff";
        let bytes = (0..80)
            .map(|i| alphabet[(seed.wrapping_mul(13) + i * i + i / 3) % alphabet.len()])
            .collect::<Vec<_>>();
        for pattern in [&b"a"[..], &b"ab"[..], &b"aa"[..], &b"\0\xff"[..]] {
            let mut expected = Vec::new();
            let mut offset = 0;
            while offset + pattern.len() <= bytes.len() {
                if bytes[offset..].starts_with(pattern) {
                    expected.push(offset as u64);
                    offset += pattern.len();
                } else {
                    offset += 1;
                }
            }
            let mut next = 0;
            let mut actual = Vec::new();
            let mut limit = budget();
            loop {
                let page = scan(&bytes, pattern, next, 1, &mut limit, || true).unwrap();
                for hit in page.hits {
                    assert_eq!(
                        hit.line,
                        1 + bytes[..hit.offset as usize]
                            .iter()
                            .filter(|&&b| b == b'\n')
                            .count() as u64
                    );
                    let line_start = bytes[..hit.offset as usize]
                        .iter()
                        .rposition(|&b| b == b'\n')
                        .map_or(0, |n| n + 1);
                    assert_eq!(hit.column, hit.offset - line_start as u64 + 1);
                    assert_eq!(
                        &bytes[hit.offset as usize..hit.offset as usize + hit.length as usize],
                        pattern
                    );
                    let within = (hit.offset - hit.snippet_offset) as usize;
                    assert_eq!(&hit.snippet[within..within + pattern.len()], pattern);
                    actual.push(hit.offset);
                }
                match page.next_offset {
                    Some(value) => {
                        assert!(value > next);
                        next = value;
                    }
                    None => break,
                }
            }
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn snippets_include_long_literals_without_unbounded_line_context() {
    let pattern = vec![b'x'; MAX_PATTERN_BYTES];
    let mut bytes = vec![b'a'; 1000];
    bytes.extend(&pattern);
    bytes.extend(vec![b'z'; 1000]);
    bytes.push(b'\n');
    let result = scan(&bytes, &pattern, 0, 2, &mut budget(), || true).unwrap();
    assert_eq!(result.hits.len(), 1);
    let hit = &result.hits[0];
    assert_eq!(hit.offset, 1000);
    assert_eq!(hit.column, 1001);
    assert_eq!(hit.snippet.len(), MAX_SNIPPET_BYTES);
    assert_eq!(&hit.snippet[128..128 + pattern.len()], pattern);
    assert!(hit.truncated_prefix && hit.truncated_suffix);
    assert!(result.next_offset.is_none());
}

#[test]
fn exhausted_or_cancelled_work_never_returns_a_partial_page() {
    let mut input = MatchBudget::new(MatchLimits {
        max_input_bytes: 3,
        max_work: 100,
        max_output_bytes: 1024,
    });
    assert_eq!(
        scan(b"aaaa", b"a", 0, 1, &mut input, || true),
        Err(Error::Limit)
    );
    let mut work = MatchBudget::new(MatchLimits {
        max_input_bytes: 100,
        max_work: 3,
        max_output_bytes: 1024,
    });
    assert_eq!(
        scan(b"aaaa", b"a", 0, 2, &mut work, || true),
        Err(Error::Limit)
    );
    for stop in 0..30 {
        let mut checks = 0;
        let result = scan(
            b"first needle\nsecond needle",
            b"needle",
            0,
            10,
            &mut budget(),
            || {
                let valid = checks < stop;
                checks += 1;
                valid
            },
        );
        if result == Err(Error::Cancelled) {
            assert_eq!(checks, stop + 1);
        }
    }
    let mut output = MatchBudget::new(MatchLimits {
        max_input_bytes: 100,
        max_work: 1000,
        max_output_bytes: 1,
    });
    assert_eq!(
        scan(b"a", b"a", 0, 1, &mut output, || true),
        Err(Error::Limit)
    );
    assert_eq!(
        scan(b"a", b"", 0, 1, &mut budget(), || true),
        Err(Error::Invalid)
    );
    assert_eq!(
        scan(b"a", b"a\n", 0, 1, &mut budget(), || true),
        Err(Error::Invalid)
    );
}

fn regex(pattern: &str) -> Pattern {
    Pattern::new(pattern.as_bytes(), SearchMode::Regex).unwrap()
}

#[test]
fn regex_alternatives_repetitions_and_flags_report_actual_match_widths() {
    let cases: &[(&str, &[u8], &[(u64, u32)])] = &[
        (
            "foo|bar[0-9]+",
            b"foo bar12 bar3",
            &[(0, 3), (4, 5), (10, 4)],
        ),
        ("ab+", b"abb ab abbbb", &[(0, 3), (4, 2), (7, 5)]),
        ("a.*?b", b"axbyb", &[(0, 3)]),
        ("a.*b", b"axbyb", &[(0, 5)]),
        ("(?i:hello)", b"HeLLo HELLO", &[(0, 5), (6, 5)]),
        (r"\bfoo\b", b"xfoo foo fooz foo", &[(5, 3), (14, 3)]),
        (r"(?-u:\xFF+)", b"x\xff\xffy", &[(1, 2)]),
        ("(?u:é+)", "éé".as_bytes(), &[(0, 4)]),
        ("a.*b", b"a\nb", &[]),
        (r"a\nb", b"a\nb", &[]),
        ("^foo$", b"foo\nxfoo\nfoo", &[(0, 3), (9, 3)]),
        ("a*", b"baab", &[(1, 2)]),
        ("^|$", b"abc\ndef", &[]),
    ];
    for &(pattern, bytes, expected) in cases {
        let result = regex(pattern)
            .scan(bytes, 0, 100, &mut budget(), || true)
            .unwrap();
        let actual: Vec<_> = result
            .hits
            .iter()
            .map(|hit| (hit.offset, hit.length))
            .collect();
        assert_eq!(actual, expected, "{pattern}");
        for hit in result.hits {
            let start = (hit.offset - hit.snippet_offset) as usize;
            assert_eq!(
                &hit.snippet[start..start + hit.length as usize],
                &bytes[hit.offset as usize..hit.offset as usize + hit.length as usize]
            );
            assert_eq!(
                hit.line,
                1 + bytes[..hit.offset as usize]
                    .iter()
                    .filter(|&&b| b == b'\n')
                    .count() as u64
            );
        }
    }
}

#[test]
fn regex_paging_preserves_line_and_word_boundary_context() {
    let bytes = b"foo foo\nfoofoo foo\nfoo";
    for pattern in [
        r"\bfoo\b",
        "^foo",
        "foo$",
        "fo+",
        "foo|foofoo",
        "foofoo|foo",
    ] {
        let plan = regex(pattern);
        let whole = plan.scan(bytes, 0, 100, &mut budget(), || true).unwrap();
        let mut paged = Vec::new();
        let mut start = 0;
        let mut work = budget();
        loop {
            let page = plan.scan(bytes, start, 1, &mut work, || true).unwrap();
            paged.extend(page.hits);
            match page.next_offset {
                Some(next) => {
                    assert!(next > start);
                    start = next;
                }
                None => break,
            }
        }
        assert_eq!(paged, whole.hits, "{pattern}");
    }
}

#[test]
fn regex_filter_never_excludes_present_alternatives_or_optional_literals() {
    let patterns = [
        "needle",
        "needle|second",
        "(needle)?second",
        "needle.*second",
        "(needle|x).*second",
        "(?i:needle)",
        "[a-z]+",
        "needle{1,3}",
        "(needle)*",
        "(?u:é+)second",
    ];
    let inputs: &[&[u8]] = &[
        b"needle",
        b"second",
        b"x second",
        b"needle second",
        b"NEEDLE",
        "éésecond".as_bytes(),
    ];
    for pattern in patterns {
        let plan = regex(pattern);
        for bytes in inputs {
            let mut builder = FilterBuilder::new();
            builder.update(bytes);
            let filter = builder.finish();
            let page = plan.scan(bytes, 0, 100, &mut budget(), || true).unwrap();
            if !page.hits.is_empty() {
                assert!(plan.may_match(&filter), "{pattern} {bytes:?}");
            }
        }
    }
    let empty = FilterBuilder::new().finish();
    assert!(!regex("needle|second").may_match(&empty));
    assert!(!regex("needle.*second").may_match(&empty));
    assert!(regex("needle|x").may_match(&empty));
    assert!(regex("needle?").may_match(&FileFilter::from_bytes([255; FILTER_BYTES])));
    let saturated = FileFilter::from_bytes([255; FILTER_BYTES]);
    let plan = regex("needle.*second");
    assert!(plan.may_match(&saturated));
    assert!(
        plan.scan(b"unrelated", 0, 10, &mut budget(), || true)
            .unwrap()
            .hits
            .is_empty()
    );
}

#[test]
fn regex_complexity_work_output_and_cancellation_fail_without_partial_pages() {
    for pattern in ["[", "(?=foo)", r"(foo)\1", r"(?u:\bfoo\b)", "x{1000000000}"] {
        assert!(
            matches!(
                Pattern::new(pattern.as_bytes(), SearchMode::Regex),
                Err(Error::Invalid)
            ),
            "{pattern}"
        );
    }
    assert!(matches!(
        Pattern::new(b"\xff", SearchMode::Regex),
        Err(Error::Invalid)
    ));
    let plan = regex("a.*z|a");
    let input = vec![b'a'; 10_000];
    let mut limited = MatchBudget::new(MatchLimits {
        max_input_bytes: 20_000,
        max_work: 30_000,
        max_output_bytes: 1_000_000,
    });
    assert_eq!(
        plan.scan(&input, 0, 100, &mut limited, || true),
        Err(Error::Limit)
    );
    let mut checks = 0;
    assert_eq!(
        plan.scan(&input, 0, 100, &mut budget(), || {
            checks += 1;
            checks < 200
        }),
        Err(Error::Cancelled)
    );
    let mut output = MatchBudget::new(MatchLimits {
        max_input_bytes: 20_000,
        max_work: 100_000,
        max_output_bytes: 1,
    });
    assert_eq!(
        regex("a+").scan(b"aaaa", 0, 1, &mut output, || true),
        Err(Error::Limit)
    );
    assert_eq!(
        regex("a+").scan(
            &vec![b'a'; MAX_PATTERN_BYTES + 1],
            0,
            1,
            &mut budget(),
            || true
        ),
        Err(Error::Limit)
    );
    let wide = regex("a+")
        .scan(&vec![b'a'; MAX_PATTERN_BYTES], 0, 1, &mut budget(), || true)
        .unwrap();
    assert_eq!(wide.hits[0].length as usize, MAX_PATTERN_BYTES);
    assert_eq!(wide.hits[0].snippet.len(), MAX_PATTERN_BYTES);
}

#[test]
fn regex_positions_match_reference_dfa_iteration_over_generated_byte_lines() {
    let patterns = [
        "a", "a|ab", "ab|a", "a+", "a+?", "a*", "a*?", "a?|b", "|a", "a|", "(a|b)*", "(a|b)*?",
        "a.*b|a", "a.*?b", "^a", "b$", "^|b", r"\ba+\b", r"\Ba", "a{1,3}", "[ab]+", "(?i:a)b?",
        r"[^\n]+", r"\xFF+",
    ];
    for pattern in patterns {
        let plan = regex(pattern);
        let reference = regex_automata::dfa::regex::Regex::builder()
            .syntax(
                regex_automata::util::syntax::Config::new()
                    .unicode(false)
                    .utf8(false),
            )
            .thompson(regex_automata::nfa::thompson::Config::new().utf8(false))
            .build(pattern)
            .unwrap();
        for seed in 0..256usize {
            let alphabet = b"ab \n\xff";
            let bytes = (0..seed % 37)
                .map(|i| alphabet[(seed.wrapping_mul(17) + i * i + i / 3) % alphabet.len()])
                .collect::<Vec<_>>();
            let mut expected = Vec::new();
            let mut line_start = 0;
            for line in bytes.split(|&byte| byte == b'\n') {
                expected.extend(
                    reference
                        .find_iter(line)
                        .filter(|hit| !hit.is_empty())
                        .map(|hit| ((line_start + hit.start()) as u64, hit.len() as u32)),
                );
                line_start += line.len() + 1;
            }
            let actual = plan.scan(&bytes, 0, 100, &mut budget(), || true).unwrap();
            assert_eq!(
                actual
                    .hits
                    .iter()
                    .map(|hit| (hit.offset, hit.length))
                    .collect::<Vec<_>>(),
                expected,
                "pattern={pattern} bytes={bytes:?}"
            );
            let mut builder = FilterBuilder::new();
            builder.update(&bytes);
            if !expected.is_empty() {
                assert!(plan.may_match(&builder.finish()), "{pattern} {bytes:?}");
            }
        }
    }
}

#[test]
fn regex_preflight_validates_syntax_without_expanding_large_programs() {
    assert_eq!(validate_input(b"x{1000000000}", SearchMode::Regex), Ok(()));
    assert!(matches!(
        Pattern::new(b"x{1000000000}", SearchMode::Regex),
        Err(Error::Invalid)
    ));
    assert_eq!(validate_input(b"[", SearchMode::Regex), Err(Error::Invalid));
    assert_eq!(
        validate_input(b"\xff", SearchMode::Regex),
        Err(Error::Invalid)
    );
    assert_eq!(validate_input(b"[\xff", SearchMode::Literal), Ok(()));
}
