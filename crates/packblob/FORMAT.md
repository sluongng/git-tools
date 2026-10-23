# Indexed BLAKE3 pack format v1

An independent, indexed container inspired by packblob's dictionary compression
and selective blob access. It is not Mononoke's `StorageEnvelope` wire format.
All integers below are unsigned 64-bit little-endian values. Blob identity is
the pair `(ordinary unkeyed BLAKE3 of raw bytes, raw byte length)`.

## Layout

| Region | Size | Content |
| --- | --- | --- |
| Header | 8 bytes | ASCII `PBLBB3I1` |
| Payloads | Variable | Raw bytes, zstd frames or raw-prefix zstd frames |
| Index | 72 bytes per entry | Strictly sorted by raw hash, then raw length |
| Footer | 32 bytes | `PBLBB3E1`, index offset, entry count, record size `72` |

Offsets start at the first header byte. The index ends immediately before the
footer. The footer is emitted only by successful `Writer::finish()`. An empty
pack is valid. Files are immutable after publication; there is no append or
interrupted-write recovery protocol. Durability and pack naming are external.

## Index record

| Byte offset | Width | Field |
| --- | --- | --- |
| 0 | 32 | Raw BLAKE3 digest |
| 32 | 8 | Raw byte length |
| 40 | 8 | Payload offset |
| 48 | 8 | Encoded payload length |
| 56 | 8 | Anchor ordinal in the sorted index, or `u64::MAX` |
| 64 | 8 | Codec: `0` raw, `1` zstd, `2` raw-prefix zstd |

Each distinct digest/length pair occurs once. Codecs 0 and 1 have no anchor.
Codec 2 refers to an independent entry in the same pack; chains, self references
and cross-pack references are invalid. The writer remaps ordinals after sorting.
Payload insertion order need not match index order. A zero-length raw payload
is valid. Encoded ranges must lie entirely between header and index.

Zstd payloads contain exactly one ordinary zstd frame, without trailing data.
Codec 2 uses the complete raw anchor as a **raw prefix**, not a trained or
auto-detected formatted dictionary. Arbitrary binary anchors, including bytes
matching zstd's formatted-dictionary magic, remain raw content. The writer
selects raw or independent zstd first, then uses a prefix only when smaller.
The anchor must already have an independent representation when added.

## Validation and resource boundaries

`Reader::open` reads the header/footer and checks entry count, pack size and
index extent. It deliberately does not scan or certify every index record.
Lookup validates accessed records before using their offsets; malformed or
out-of-order unrelated records can make lookup fail, never authorize content
under a different independently expected digest.

Extraction checks encoded, decoded, anchor and zstd-window limits before their
allocations, reads at most the target and one independent anchor, and verifies
both reconstructed byte lengths and BLAKE3 hashes. Unknown codecs, invalid
ranges/ordinals, dependency chains, excessive limits and corrupt frames fail.
A caller must provide its independently expected target digest/size.

`read_encoded` returns a selected frame and anchor identity without reading the
anchor. It does not prove the decoded target or authenticate a network peer;
a receiver must independently verify the expected target after reconstruction.
Metadata lookup does not apply per-payload allocation limits to unrelated
entries. Applications separately bound concurrent readers and retain backing
files while in use. `Limits::decode_memory_bound()` provides a conservative
payload/window reservation estimate, not an allocator accounting guarantee.

The format's checksum boundary is each logical blob. Applications may name a
sealed pack by a separate hash of the complete file; selective readers do not
rehash the entire container. Canonical repository object kinds and authorization
belong to the external object catalog, not this physical byte container.
