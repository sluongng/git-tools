use super::*;
use std::io::Write;

struct Fixture {
    _root: tempfile::TempDir,
    directory: Arc<PrivateDir>,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("capture-pack-")
            .tempdir_in(std::env::temp_dir().canonicalize().unwrap())
            .unwrap();
        let directory = Arc::new(PrivateDir::create(root.path().join("packs")).unwrap());
        Self {
            _root: root,
            directory,
        }
    }
    fn save(&self, bytes: &[u8]) -> [u8; 32] {
        let hash = *blake3::hash(bytes).as_bytes();
        let mut file = self
            .directory
            .open_lock(format!("{}.pack", hex(&hash)))
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        hash
    }
    fn open(&self, bytes: &[u8], limits: Limits) -> io::Result<VerifiedPack> {
        let hash = self.save(bytes);
        VerifiedPack::open(
            self.directory.clone(),
            hash,
            bytes.len() as u64,
            limits,
            Arc::new(()),
        )
    }
}
fn limits() -> Limits {
    Limits {
        max_entries: 16,
        max_table_bytes: 8192,
        max_memory_bytes: 1024 * 1024,
        ..Limits::default()
    }
}
fn payloads() -> Vec<(u8, String, Vec<u8>)> {
    vec![
        (1, "1".repeat(128), b"operation TTLTABLE".to_vec()),
        (2, "2".repeat(128), b"view".to_vec()),
        (3, "3".repeat(64), b"root".to_vec()),
        (4, "4".repeat(64), b"node".to_vec()),
        (5, "5".repeat(64), b"capsule".to_vec()),
        (
            128,
            hex(blake3::hash(b"PRIVATE WC").as_bytes()),
            b"PRIVATE WC".to_vec(),
        ),
        (
            129,
            hex(blake3::hash(b"PRIVATE SIDECAR").as_bytes()),
            b"PRIVATE SIDECAR".to_vec(),
        ),
    ]
}
fn encode(payloads: &[(u8, String, Vec<u8>)]) -> (Vec<u8>, Vec<Entry>) {
    let mut out = MAGIC.to_vec();
    let mut entries = Vec::new();
    for (kind, id, bytes) in payloads {
        let hash = *blake3::hash(bytes).as_bytes();
        out.extend(frame_header(*kind, id, bytes.len() as u64, hash).unwrap());
        entries.push(Entry {
            kind: *kind,
            id: id.clone(),
            blake3: hash,
            size: bytes.len() as u64,
            offset: out.len() as u64,
        });
        out.extend(bytes);
    }
    entries.sort_unstable_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    out.extend(TABLE_MAGIC);
    let table = encode_table(&entries).unwrap();
    out.extend((table.len() as u64).to_le_bytes());
    out.extend(table);
    (out, entries)
}
#[test]
fn verified_portable_payloads_exclude_private_sections_and_retain_admission() {
    struct Owner(Arc<AtomicU64>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let fixture = Fixture::new();
    let (bytes, _) = encode(&payloads());
    let hash = fixture.save(&bytes);
    let dropped = Arc::new(AtomicU64::new(0));
    let pack = VerifiedPack::open(
        fixture.directory.clone(),
        hash,
        bytes.len() as u64,
        limits(),
        Arc::new(Owner(dropped.clone())),
    )
    .unwrap();
    let leases: Vec<_> = pack.portable_entries().collect();
    assert_eq!(
        leases
            .iter()
            .map(|l| l.descriptor().kind)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    for (lease, (_, _, expected)) in leases.iter().zip(payloads()) {
        assert_eq!(lease.read_verified().unwrap().bytes(), expected);
    }
    let owned = leases[0].read_verified().unwrap();
    drop(leases);
    drop(pack);
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    assert_eq!(owned.bytes(), payloads()[0].2);
    drop(owned);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
}
#[test]
fn physical_order_is_independent_of_sorted_canonical_table_and_payload_markers() {
    let fixture = Fixture::new();
    let mut data = payloads();
    data.reverse();
    data.iter_mut().find(|v| v.0 == 1).unwrap().2 = b"TTLTABLE TTLOCP01".to_vec();
    let (bytes, _) = encode(&data);
    assert!(fixture.open(&bytes, limits()).is_ok());
}
#[test]
fn malformed_frames_tables_identity_completion_and_tail_fail_closed() {
    let fixture = Fixture::new();
    let (valid, entries) = encode(&payloads());
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let mut bad = valid.clone();
    bad[0] = 0;
    cases.push(("magic", bad));
    let mut bad = valid.clone();
    bad[8] = 99;
    cases.push(("unknown kind", bad));
    let mut bad = valid.clone();
    bad[9] = 64;
    cases.push(("wrong typed width", bad));
    let mut bad = valid.clone();
    bad[10] = b'A';
    cases.push(("noncanonical ID", bad));
    let mut bad = valid.clone();
    bad[entries[0].offset as usize] ^= 1;
    cases.push(("section hash", bad));
    let mut bad = valid.clone();
    bad.push(0);
    cases.push(("extra tail", bad));
    cases.push(("truncated", valid[..valid.len() - 1].to_vec()));
    let mut data = payloads();
    data.push(data[0].clone());
    cases.push(("duplicate", encode(&data).0));
    let data: Vec<_> = payloads().into_iter().filter(|v| v.0 != 128).collect();
    cases.push(("missing private completion", encode(&data).0));
    let data: Vec<_> = payloads().into_iter().filter(|v| v.0 != 5).collect();
    cases.push(("missing capsule", encode(&data).0));
    let mut data = payloads();
    data.iter_mut().find(|v| v.0 == 128).unwrap().1 = "a".repeat(64);
    cases.push(("private raw ID", encode(&data).0));
    let mut bad = valid.clone();
    let marker = entries.iter().map(|e| e.offset + e.size).max().unwrap() as usize;
    // A canonical-looking table with an offset disagreement is still rejected.
    let mut wrong = entries.clone();
    wrong[0].offset += 1;
    let table = encode_table(&wrong).unwrap();
    bad.truncate(marker + 8);
    bad.extend((table.len() as u64).to_le_bytes());
    bad.extend(table);
    cases.push(("table offset", bad));
    let mut bad = valid.clone();
    let table_start = marker + 16;
    bad.insert(table_start, b' ');
    let newlen = (bad.len() - table_start) as u64;
    bad[marker + 8..table_start].copy_from_slice(&newlen.to_le_bytes());
    cases.push(("noncanonical JSON", bad));
    for (name, bad) in cases {
        assert!(fixture.open(&bad, limits()).is_err(), "{name}");
    }
    // Keep every frame/table/section hash valid, but publish those bytes under
    // a different expected outer digest. This must reach final whole-pack hash
    // verification, not fail while opening a missing digest-named file.
    let wrong = [0; 32];
    let mut file = fixture
        .directory
        .open_lock(format!("{}.pack", hex(&wrong)))
        .unwrap();
    file.write_all(&valid).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let error = VerifiedPack::open(
        fixture.directory.clone(),
        wrong,
        valid.len() as u64,
        limits(),
        Arc::new(()),
    )
    .err()
    .expect("wrong outer digest must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "capture pack digest differs");
}
#[test]
fn declared_limits_and_post_verification_mutation_are_observable() {
    let fixture = Fixture::new();
    let (bytes, entries) = encode(&payloads());
    for policy in [
        Limits {
            max_entries: 6,
            ..limits()
        },
        Limits {
            max_scan_bytes: 1,
            ..limits()
        },
        Limits {
            max_table_bytes: 1,
            ..limits()
        },
        Limits {
            max_memory_bytes: 1,
            ..limits()
        },
        Limits {
            max_pack_bytes: 1,
            ..limits()
        },
    ] {
        assert!(fixture.open(&bytes, policy).is_err());
    }
    let pack = fixture
        .open(
            &bytes,
            Limits {
                max_read_bytes: payloads()[0].2.len() as u64,
                ..limits()
            },
        )
        .unwrap();
    let lease = pack.portable_entries().next().unwrap();
    assert!(lease.read_verified().is_ok());
    assert!(lease.read_verified().is_err(), "read work cumulative");
    let pack = fixture
        .open(
            &bytes,
            Limits {
                max_entry_bytes: 1,
                ..limits()
            },
        )
        .unwrap();
    assert!(
        pack.portable_entries()
            .next()
            .unwrap()
            .read_verified()
            .is_err()
    );
    let pack = fixture.open(&bytes, limits()).unwrap();
    let lease = pack.portable_entries().next().unwrap();
    let hash = *blake3::hash(&bytes).as_bytes();
    let mut writer = fixture
        .directory
        .open_lock(format!("{}.pack", hex(&hash)))
        .unwrap();
    writer.seek(SeekFrom::Start(entries[0].offset)).unwrap();
    writer.write_all(b"X").unwrap();
    assert!(
        lease.read_verified().is_err(),
        "mutable FD payload must be rehashed before exposure"
    );
}
#[cfg(unix)]
#[test]
fn pack_open_does_not_follow_symlinks_or_adopt_public_files() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let fixture = Fixture::new();
    let (bytes, _) = encode(&payloads());
    let hash = *blake3::hash(&bytes).as_bytes();
    let target = fixture.directory.path().join("elsewhere");
    std::fs::write(&target, &bytes).unwrap();
    let path = fixture
        .directory
        .path()
        .join(format!("{}.pack", hex(&hash)));
    symlink(&target, &path).unwrap();
    assert!(
        VerifiedPack::open(
            fixture.directory.clone(),
            hash,
            bytes.len() as u64,
            limits(),
            Arc::new(())
        )
        .is_err()
    );
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(&target, &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        VerifiedPack::open(
            fixture.directory.clone(),
            hash,
            bytes.len() as u64,
            limits(),
            Arc::new(())
        )
        .is_err()
    );
}

#[test]
fn v1_header_and_table_bytes_stay_compatible_and_read_memory_is_shared() {
    let id = "a".repeat(64);
    let header = frame_header(3, &id, 7, [0; 32]).unwrap();
    let mut expected = vec![3, 64];
    expected.extend(id.as_bytes());
    expected.extend(7u64.to_le_bytes());
    expected.extend([0; 32]);
    assert_eq!(header, expected);
    let table = encode_table(&[Entry {
        kind: 3,
        id: id.clone(),
        blake3: [0; 32],
        size: 7,
        offset: 114,
    }])
    .unwrap();
    let expected = format!(
        "[{{\"kind\":3,\"id\":\"{id}\",\"blake3\":[{}],\"size\":7,\"offset\":114}}]",
        ["0"; 32].join(",")
    );
    assert_eq!(table, expected.as_bytes());
    let fixture = Fixture::new();
    let mut payloads = payloads();
    payloads[0].2 = vec![7; 90_000];
    let (bytes, _) = encode(&payloads);
    let pack = fixture
        .open(
            &bytes,
            Limits {
                max_memory_bytes: 160_000,
                ..limits()
            },
        )
        .unwrap();
    let lease = pack.portable_entries().next().unwrap();
    let first = lease.read_verified().unwrap();
    assert!(
        lease.read_verified().is_err(),
        "simultaneous immutable buffers share the admitted pool"
    );
    drop(first);
    assert!(
        lease.read_verified().is_ok(),
        "deallocation releases the physical byte charge"
    );
}

/// Component-only measurement: checksum baseline is NOT a prior structural
/// verifier. Both operations read the same warm-cache file; no startup, network,
/// filesystem cold-cache, RSS or cloud-upload performance claim is implied.
#[test]
#[ignore = "manual warm-cache capture-pack component benchmark"]
fn warm_capture_pack_verification_benchmark() {
    use std::time::Instant;
    const TRIALS: usize = 7;
    fn checksum(directory: &PrivateDir, hash: [u8; 32], size: u64) -> [u8; 32] {
        let mut file = directory
            .open_existing_file(format!("{}.pack", hex(&hash)), size)
            .unwrap();
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let n = file.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        *hasher.finalize().as_bytes()
    }
    let fixture = Fixture::new();
    for (shape, extra_nodes) in [("small_queued_shape", 0), ("many_sections", 128)] {
        let mut data = payloads();
        for ((kind, id, bytes), size) in
            data.iter_mut().zip([4096, 2048, 1024, 1024, 512, 512, 512])
        {
            *bytes = vec![*kind; size];
            if *kind >= 128 {
                *id = hex(blake3::hash(bytes).as_bytes());
            }
        }
        for n in 0..extra_nodes {
            data.push((4, format!("{:064x}", n + 1024), vec![n as u8; 32 * 1024]));
        }
        let (bytes, entries) = encode(&data);
        let hash = fixture.save(&bytes);
        let size = bytes.len() as u64;
        let policy = Limits {
            max_entries: 256,
            max_table_bytes: 256 * 512,
            max_memory_bytes: 4 * 1024 * 1024,
            max_read_bytes: 64 * 1024 * 1024,
            ..Limits::default()
        };
        let portable_bytes: u64 = entries
            .iter()
            .filter(|e| (1..=5).contains(&e.kind))
            .map(|e| e.size)
            .sum();
        // Explicit warm-cache preparation, excluded from every timed phase.
        assert_eq!(checksum(&fixture.directory, hash, size), hash);
        let mut baseline_ns = Vec::new();
        let mut verify_ns = Vec::new();
        let mut portable_read_ns = Vec::new();
        for trial in 0..TRIALS {
            let baseline = || {
                let start = Instant::now();
                assert_eq!(checksum(&fixture.directory, hash, size), hash);
                start.elapsed().as_nanos() as u64
            };
            // Alternate checksum/full-verification ordering to avoid always
            // granting one phase the immediately preceding sequential read.
            if trial % 2 == 0 {
                baseline_ns.push(baseline());
            }
            let start = Instant::now();
            let pack = VerifiedPack::open(
                fixture.directory.clone(),
                hash,
                size,
                policy.clone(),
                Arc::new(()),
            )
            .unwrap();
            verify_ns.push(start.elapsed().as_nanos() as u64);
            let start = Instant::now();
            let mut read_bytes = 0;
            let mut count = 0;
            for lease in pack.portable_entries() {
                let payload = lease.read_verified().unwrap();
                read_bytes += payload.bytes().len() as u64;
                count += 1;
            }
            portable_read_ns.push(start.elapsed().as_nanos() as u64);
            assert_eq!(read_bytes, portable_bytes);
            assert_eq!(
                count,
                entries.iter().filter(|e| (1..=5).contains(&e.kind)).count()
            );
            drop(pack);
            if trial % 2 != 0 {
                baseline_ns.push(baseline());
            }
        }
        println!(
            "{}",
            serde_json::json!({"benchmark":"native_capture_pack_component_v1",
            "shape":shape,"trials":TRIALS,"method":"warm OS file cache; untimed sequential checksum prewarm; alternating checksum/verification ordering",
            "baseline":"streaming whole-file BLAKE3 only, not an old full verifier",
            "pack_bytes":size,"section_count":entries.len(),"portable_bytes":portable_bytes,
            "portable_sections":entries.iter().filter(|e|(1..=5).contains(&e.kind)).count(),
            "configured_logical_memory_limit_bytes":policy.max_memory_bytes,
            "checksum_ns":baseline_ns,"structural_verify_ns":verify_ns,"portable_read_and_rehash_ns":portable_read_ns})
        );
    }
}
