use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn noise(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}
fn delta_pack() -> (Vec<u8>, Digest, Vec<u8>, Digest, Vec<u8>, Summary) {
    let base = noise(32768, 71);
    let mut target = base.clone();
    target[13] ^= 99;
    target[20000] ^= 18;
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let anchor = writer.add(&base, None).unwrap();
    let digest = writer.add(&target, Some((anchor, &base))).unwrap();
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.delta_entries, 1);
    (bytes, anchor, base, digest, target, summary)
}
#[test]
fn roundtrip_raw_zstd_delta_and_duplicate_logical_blobs() {
    let (bytes, anchor, base, digest, target, summary) = delta_pack();
    assert!(summary.encoded_bytes < summary.raw_bytes);
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert_eq!(reader.read_blob(anchor).unwrap(), base);
    assert_eq!(reader.read_blob(digest).unwrap(), target);
    assert_eq!(reader.lookup(digest).unwrap().unwrap().codec, Codec::Delta);
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let empty = writer.add(b"", None).unwrap();
    let raw = writer.add(&noise(512, 10), None).unwrap();
    let zstd = writer.add(&vec![b'a'; 8192], None).unwrap();
    assert_eq!(writer.add(&vec![b'a'; 8192], None).unwrap(), zstd);
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.entries, 3);
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert_eq!(reader.lookup(raw).unwrap().unwrap().codec, Codec::Raw);
    assert_eq!(reader.lookup(zstd).unwrap().unwrap().codec, Codec::Zstd);
    assert_eq!(reader.read_blob(empty).unwrap(), b"");
    assert_eq!(reader.read_blob(zstd).unwrap(), vec![b'a'; 8192]);
}
#[test]
fn binary_dictionary_magic_is_raw_content_and_delta_chains_are_not_written() {
    let mut base = noise(16000, 88);
    base[..4].copy_from_slice(&[0x37, 0xa4, 0x30, 0xec]);
    let mut target = base.clone();
    target[100] ^= 42;
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let anchor = writer.add(&base, None).unwrap();
    let digest = writer.add(&target, Some((anchor, &base))).unwrap();
    let mut third = target.clone();
    third[101] ^= 43;
    assert!(matches!(
        writer.add(&third, Some((digest, &target))),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        writer.add(&third, Some((anchor, b"wrong"))),
        Err(Error::Integrity)
    ));
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.delta_entries, 1);
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert_eq!(reader.read_blob(digest).unwrap(), target);
}
#[derive(Clone)]
struct Counted {
    cursor: Cursor<Vec<u8>>,
    bytes: Arc<AtomicUsize>,
    ranges: Arc<Mutex<Vec<(u64, usize)>>>,
}
impl Read for Counted {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let position = self.cursor.position();
        let size = self.cursor.read(output)?;
        self.bytes.fetch_add(size, Ordering::SeqCst);
        self.ranges.lock().unwrap().push((position, size));
        Ok(size)
    }
}
impl Seek for Counted {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.cursor.seek(position)
    }
}
#[test]
fn selective_metadata_delta_and_content_reads_do_not_fetch_the_pack_or_other_payloads() {
    let base = noise(32768, 71);
    let mut target = base.clone();
    target[800] ^= 42;
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let anchor = writer.add(&base, None).unwrap();
    let digest = writer.add(&target, Some((anchor, &base))).unwrap();
    for n in 1..=100 {
        writer.add(&noise(4096, 1000 + n), None).unwrap();
    }
    let (pack, _) = writer.finish().unwrap();
    let total = pack.len();
    let bytes = Arc::new(AtomicUsize::new(0));
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let mut reader = Reader::open(
        Counted {
            cursor: Cursor::new(pack),
            bytes: bytes.clone(),
            ranges: ranges.clone(),
        },
        Limits::default(),
    )
    .unwrap();
    assert_eq!(bytes.load(Ordering::SeqCst), 40);
    let entry = reader.lookup(digest).unwrap().unwrap();
    assert_eq!(entry.codec, Codec::Delta);
    assert!(bytes.load(Ordering::SeqCst) < 1024);
    bytes.store(0, Ordering::SeqCst);
    ranges.lock().unwrap().clear();
    let encoded = reader.read_encoded(digest).unwrap();
    assert_eq!(encoded.base, Some(anchor));
    assert!(encoded.bytes.len() < 256);
    assert!(bytes.load(Ordering::SeqCst) < 2048);
    assert!(
        ranges.lock().unwrap().iter().all(
            |(offset, length)| *offset >= 8 + base.len() as u64 || offset + *length as u64 <= 8
        )
    );
    bytes.store(0, Ordering::SeqCst);
    assert_eq!(reader.read_blob(digest).unwrap(), target);
    assert!(bytes.load(Ordering::SeqCst) < base.len() + 4096);
    assert!(bytes.load(Ordering::SeqCst) < total / 4);
}
#[test]
fn accessed_corrupt_payload_and_anchor_fail_without_returning_content() {
    let (mut bytes, anchor, _, digest, _, _) = delta_pack();
    let mut reader = Reader::open(Cursor::new(bytes.clone()), Limits::default()).unwrap();
    let anchor_entry = reader.lookup(anchor).unwrap().unwrap();
    bytes[anchor_entry.offset as usize] ^= 1;
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert!(reader.read_blob(digest).is_err());
    let (mut bytes, _, _, digest, _, _) = delta_pack();
    let mut reader = Reader::open(Cursor::new(bytes.clone()), Limits::default()).unwrap();
    let entry = reader.lookup(digest).unwrap().unwrap();
    bytes[entry.offset as usize] ^= 1;
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert!(reader.read_blob(digest).is_err());
}
#[test]
fn malformed_headers_extents_codecs_anchors_and_trailing_frames_are_rejected() {
    let (bytes, _, _, digest, _, _) = delta_pack();
    for length in [0, 7, 39, bytes.len() - 1] {
        assert!(Reader::open(Cursor::new(bytes[..length].to_vec()), Limits::default()).is_err());
    }
    let mut wrong = bytes.clone();
    wrong[0] ^= 1;
    assert!(Reader::open(Cursor::new(wrong), Limits::default()).is_err());
    let mut wrong = bytes.clone();
    let footer = wrong.len() - 32;
    wrong[footer + 8..footer + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(Reader::open(Cursor::new(wrong), Limits::default()).is_err());
    let footer = bytes.len() - 32;
    let index = integer(&bytes[footer..], 8) as usize;
    let mut reader = Reader::open(Cursor::new(bytes.clone()), Limits::default()).unwrap();
    let target_ordinal = (0..reader.entry_count())
        .find(|n| reader.entry_at(*n).unwrap().digest == digest)
        .unwrap();
    let anchor_ordinal = reader
        .lookup(digest)
        .unwrap()
        .unwrap()
        .base_ordinal
        .unwrap();
    let mut wrong = bytes.clone();
    wrong[index + anchor_ordinal as usize * 72 + 64..index + anchor_ordinal as usize * 72 + 72]
        .copy_from_slice(&2u64.to_le_bytes());
    wrong[index + anchor_ordinal as usize * 72 + 56..index + anchor_ordinal as usize * 72 + 64]
        .copy_from_slice(&target_ordinal.to_le_bytes());
    let mut reader = Reader::open(Cursor::new(wrong), Limits::default()).unwrap();
    assert!(reader.read_blob(digest).is_err());
    let mut wrong = bytes.clone();
    wrong[index + target_ordinal as usize * 72 + 64..index + target_ordinal as usize * 72 + 72]
        .copy_from_slice(&99u64.to_le_bytes());
    let mut reader = Reader::open(Cursor::new(wrong), Limits::default()).unwrap();
    assert!(reader.read_blob(digest).is_err());
    // One otherwise valid independent frame followed by an unused extra byte.
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let id = writer.add(&vec![b'x'; 1000], None).unwrap();
    let (mut packed, _) = writer.finish().unwrap();
    let footer = packed.len() - 32;
    let index = integer(&packed[footer..], 8) as usize;
    let length = integer(&packed[index..index + 72], 48);
    packed.insert(index, 0);
    packed[index + 1 + 48..index + 1 + 56].copy_from_slice(&(length + 1).to_le_bytes());
    let footer = packed.len() - 32;
    packed[footer + 8..footer + 16].copy_from_slice(&(index as u64 + 1).to_le_bytes());
    let mut reader = Reader::open(Cursor::new(packed), Limits::default()).unwrap();
    assert!(reader.read_blob(id).is_err());
}
#[test]
fn budgets_apply_before_payload_reads_and_metadata_lookup_tolerates_unrelated_larger_entries() {
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let small = writer.add(b"small", None).unwrap();
    let large = writer.add(&noise(32768, 8), None).unwrap();
    let (bytes, _) = writer.finish().unwrap();
    let mut limits = Limits::default();
    limits.max_blob_bytes = 128;
    limits.max_anchor_bytes = 128;
    let mut reader = Reader::open(Cursor::new(bytes), limits).unwrap();
    assert!(reader.lookup(large).unwrap().is_some());
    assert_eq!(reader.read_blob(small).unwrap(), b"small");
    assert!(matches!(reader.read_blob(large), Err(Error::Limit(_))));
    let (bytes, _, _, digest, _, _) = delta_pack();
    let mut limits = Limits::default();
    limits.max_anchor_bytes = 100;
    let mut reader = Reader::open(Cursor::new(bytes), limits).unwrap();
    assert!(matches!(reader.read_blob(digest), Err(Error::Limit(_))));
    let mut limits = Limits::default();
    limits.max_entries = 1;
    let mut writer = Writer::new(
        Vec::new(),
        WriteConfig {
            limits,
            ..Default::default()
        },
    )
    .unwrap();
    writer.add(b"one", None).unwrap();
    assert!(matches!(writer.add(b"two", None), Err(Error::Limit(_))));
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let id = writer.add(&vec![b'a'; 32768], None).unwrap();
    let (bytes, _) = writer.finish().unwrap();
    let mut limits = Limits::default();
    limits.window_log_max = 10;
    let mut reader = Reader::open(Cursor::new(bytes), limits).unwrap();
    assert!(reader.read_blob(id).is_err());
}
#[test]
fn failed_output_is_not_sealed_or_reused() {
    struct Fail;
    impl Write for Fail {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("fixture write failure"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert!(Writer::new(Fail, WriteConfig::default()).is_err());
    struct Partial {
        left: usize,
    }
    impl Write for Partial {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.left == 0 {
                return Err(io::Error::other("fixture interrupted payload"));
            }
            let count = self.left.min(bytes.len());
            self.left -= count;
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut interrupted = Writer::new(Partial { left: 10 }, WriteConfig::default()).unwrap();
    assert!(interrupted.add(b"uncompressible", None).is_err());
    assert!(matches!(interrupted.add(b"x", None), Err(Error::Poisoned)));
    assert!(matches!(interrupted.finish(), Err(Error::Poisoned)));
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    writer.add(b"ok", None).unwrap();
    assert!(writer.add(&vec![0; 65537], None).is_err());
    let (bytes, _) = writer.finish().unwrap();
    let mut reader = Reader::open(Cursor::new(bytes), Limits::default()).unwrap();
    assert_eq!(reader.read_blob(Digest::of(b"ok")).unwrap(), b"ok");
}

#[test]
fn fixed_format_vector_uses_raw_blake3_and_little_endian_offsets() {
    let mut writer = Writer::new(Vec::new(), WriteConfig::default()).unwrap();
    let digest = writer.add(b"abc", None).unwrap();
    assert_eq!(
        digest.hash,
        [
            0x64, 0x37, 0xb3, 0xac, 0x38, 0x46, 0x51, 0x33, 0xff, 0xb6, 0x3b, 0x75, 0x27, 0x3a,
            0x8d, 0xb5, 0x48, 0xc5, 0x58, 0x46, 0x5d, 0x79, 0xdb, 0x03, 0xfd, 0x35, 0x9c, 0x6c,
            0xd5, 0xbd, 0x9d, 0x85
        ]
    );
    let (bytes, summary) = writer.finish().unwrap();
    assert_eq!(summary.encoded_bytes, 115);
    assert_eq!(&bytes[..11], b"PBLBB3I1abc");
    assert_eq!(&bytes[11..43], &digest.hash);
    assert_eq!(&bytes[43..51], &3u64.to_le_bytes());
    assert_eq!(&bytes[51..59], &8u64.to_le_bytes());
    assert_eq!(&bytes[59..67], &3u64.to_le_bytes());
    assert_eq!(&bytes[67..75], &u64::MAX.to_le_bytes());
    assert_eq!(&bytes[75..83], &0u64.to_le_bytes());
    assert_eq!(&bytes[83..91], b"PBLBB3E1");
    assert_eq!(&bytes[91..99], &11u64.to_le_bytes());
    assert_eq!(&bytes[99..107], &1u64.to_le_bytes());
    assert_eq!(&bytes[107..115], &72u64.to_le_bytes());
}
