//! ByteStream acknowledgments after successful, verified producer completion.
//!
//! Compressed-write completion counts are underspecified by REAPI. Match Bazel's
//! compatibility policy: accept the encoded count, logical size, or dedup marker.
//! See `ByteStreamUploader.checkCommittedSize` at bazelbuild/bazel db13307829.

/// Validate an acknowledgment only after the producer has successfully verified
/// the complete logical content and emitted its final encoded frame.
///
/// This does not authorize early completion based on a partial wire count or a
/// compressed upload's logical size. Callers must handle REAPI's early-dedup
/// responses separately (`-1` for compressed, logical size for identity).
pub fn valid_completed_write_ack(
    compressed: bool,
    committed_size: i64,
    logical_size: i64,
    wire_size: u64,
) -> bool {
    if logical_size < 0 {
        return false;
    }
    if !compressed {
        return committed_size == logical_size;
    }
    committed_size == -1
        || committed_size == logical_size
        || u64::try_from(committed_size).ok() == Some(wire_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_write_counts_follow_encoding_contract() {
        for (name, compressed, committed, logical, wire, valid) in [
            ("compressed wire count", true, 20, 100, 20, true),
            ("compressed logical size", true, 100, 100, 20, true),
            ("compressed dedup marker", true, -1, 100, 20, true),
            ("compressed unknown negative", true, -2, 100, 20, false),
            ("compressed short count", true, 19, 100, 20, false),
            ("compressed unrelated count", true, 101, 100, 20, false),
            ("compressed empty logical size", true, 0, 0, 9, true),
            ("compressed empty wire count", true, 9, 0, 9, true),
            ("identity logical size", false, 100, 100, 100, true),
            ("identity short count", false, 99, 100, 100, false),
            ("identity rejects dedup marker", false, -1, 100, 100, false),
            (
                "identity cannot use wire alternative",
                false,
                20,
                100,
                20,
                false,
            ),
            ("negative logical size", true, -1, -1, 20, false),
            (
                "wire count must not wrap",
                true,
                -2,
                100,
                u64::MAX - 1,
                false,
            ),
        ] {
            assert_eq!(
                valid_completed_write_ack(compressed, committed, logical, wire),
                valid,
                "{name}"
            );
        }
    }
}
