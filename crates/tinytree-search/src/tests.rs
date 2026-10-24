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
