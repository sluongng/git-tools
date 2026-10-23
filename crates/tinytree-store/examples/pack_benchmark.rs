//! Component measurements, not a remote-transfer or end-to-end VFS benchmark.
//! Corpus files and result artifacts are supplied externally, not embedded here.
use packblob::{Digest, Limits, Reader, WriteConfig, Writer};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Cursor, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tinytree_model::{Descriptor, Kind, ObjectId, ObjectKey};
use tinytree_store::{PackOptions, Store};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_OBJECTS: u64 = 10_000;
const PACK_OBJECT_BYTES: u64 = 64 * 1024;
const SEED: u64 = 0x72c5_4ac7_18e9_b603;
const LEVEL: i32 = 3;

struct Blob {
    group: String,
    bytes: Vec<u8>,
    kind: Kind,
    digest: Digest,
}
impl Blob {
    fn new(group: String, bytes: Vec<u8>, kind: Kind) -> Self {
        let digest = Digest::of(&bytes);
        Self {
            group,
            bytes,
            kind,
            digest,
        }
    }
    fn digest(&self) -> Digest {
        self.digest
    }
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            key: ObjectKey {
                kind: self.kind,
                id: ObjectId(self.digest().hash),
            },
            size_bytes: self.bytes.len() as u64,
            format_version: tinytree_model::FORMAT_VERSION,
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::other(message)
}
fn checked(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message).into())
    }
}
fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn random_bytes(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..size).map(|_| next(&mut state) as u8).collect()
}

fn synthetic() -> Vec<Blob> {
    let mut blobs = Vec::new();
    for group in 0..8 {
        for revision in 0..16 {
            let mut text = format!("// component {group}\nconst VERSION: usize = {revision};\n");
            for line in 0..256 {
                text.push_str(&format!(
                    "fn component_{group}_rule_{line}() -> usize {{ {} }}\n",
                    line + if line == revision { revision } else { 0 }
                ));
            }
            blobs.push(Blob::new(
                format!("source-{group}"),
                text.into_bytes(),
                Kind::File,
            ));
        }
    }
    for directory in 0..8 {
        for revision in 0..64 {
            let entries = (0..16)
                .map(|entry| tinytree_model::TreeEntry {
                    name: format!("source-{entry:02}"),
                    value: tinytree_model::TreeValue::File {
                        id: tinytree_model::FileId(
                            *blake3::hash(
                                format!(
                                    "directory-{directory}/entry-{entry}/revision-{}",
                                    if entry == revision % 16 { revision } else { 0 }
                                )
                                .as_bytes(),
                            )
                            .as_bytes(),
                        ),
                        executable: false,
                        size_bytes: 128,
                        copy_id: None,
                    },
                })
                .collect();
            let object = tinytree_model::encode(
                &tinytree_model::Object::Tree(tinytree_model::Tree { entries }),
                Default::default(),
            )
            .unwrap();
            blobs.push(Blob::new(
                format!("metadata-{directory}"),
                object.bytes,
                Kind::Tree,
            ));
        }
    }
    for index in 0..16 {
        let mut bytes = random_bytes(8192, SEED ^ (index + 1));
        if index == 0 {
            bytes[..4].copy_from_slice(&[0x37, 0xa4, 0x30, 0xec]);
        }
        blobs.push(Blob::new(format!("binary-{index}"), bytes, Kind::File));
    }
    blobs.push(Blob::new(
        "large-binary-control".into(),
        random_bytes(80 * 1024, SEED ^ 100),
        Kind::File,
    ));
    blobs
}

fn read_u64(input: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}
fn corpus(path: &Path) -> Result<Vec<Blob>> {
    let mut input = File::open(path)?;
    checked(
        input.metadata()?.len() <= MAX_INPUT_BYTES + MAX_OBJECTS * 4108 + 16,
        "corpus file exceeds limit",
    )?;
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    checked(&magic == b"PBCRPS01", "unsupported corpus format")?;
    let count = read_u64(&mut input)?;
    checked(
        count > 0 && count <= MAX_OBJECTS,
        "invalid corpus object count",
    )?;
    let mut total = 0u64;
    let mut result = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut length = [0; 4];
        input.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;
        checked(length > 0 && length <= 4096, "invalid corpus group length")?;
        let mut group = vec![0; length];
        input.read_exact(&mut group)?;
        let group =
            String::from_utf8(group).map_err(|_| invalid("invalid corpus group encoding"))?;
        let size = read_u64(&mut input)?;
        total = total
            .checked_add(size)
            .ok_or_else(|| invalid("corpus size overflow"))?;
        checked(
            total <= MAX_INPUT_BYTES,
            "corpus contents exceed byte limit",
        )?;
        let mut bytes = vec![0; size as usize];
        input.read_exact(&mut bytes)?;
        result.push(Blob::new(group, bytes, Kind::File));
    }
    let mut extra = [0];
    checked(input.read(&mut extra)? == 0, "trailing corpus bytes")?;
    Ok(result)
}

fn unique(blobs: Vec<Blob>) -> Vec<Blob> {
    let mut by_digest = BTreeMap::new();
    for blob in blobs {
        by_digest.entry(blob.digest()).or_insert(blob);
    }
    by_digest.into_values().collect()
}
fn sample_order(count: usize) -> Vec<usize> {
    let mut indices: Vec<_> = (0..count).collect();
    let mut state = SEED;
    for i in (1..count).rev() {
        indices.swap(i, next(&mut state) as usize % (i + 1));
    }
    indices.truncate(512);
    indices
}
fn distribution(mut values: Vec<u64>) -> Value {
    if values.is_empty() {
        return json!({"samples":0,"p50":null,"p95":null});
    }
    values.sort_unstable();
    let at = |percent: usize| values[(values.len() * percent).div_ceil(100).saturating_sub(1)];
    json!({"samples":values.len(),"p50":at(50),"p95":at(95)})
}

struct CountedCursor {
    inner: Cursor<Vec<u8>>,
    bytes: Arc<AtomicU64>,
}
impl Read for CountedCursor {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(output)?;
        self.bytes.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}
impl Seek for CountedCursor {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.inner.seek(position)
    }
}

fn limits() -> Limits {
    Limits {
        max_entries: MAX_OBJECTS,
        max_pack_bytes: 128 * 1024 * 1024,
        ..Default::default()
    }
}
fn packed(blobs: &[Blob], dictionaries: bool) -> Result<Value> {
    let eligible: Vec<_> = blobs
        .iter()
        .filter(|b| b.bytes.len() as u64 <= PACK_OBJECT_BYTES)
        .collect();
    let left_loose_bytes: u64 = blobs
        .iter()
        .filter(|b| b.bytes.len() as u64 > PACK_OBJECT_BYTES)
        .map(|b| b.bytes.len() as u64)
        .sum();
    let mut groups: BTreeMap<&str, Vec<&Blob>> = BTreeMap::new();
    for blob in &eligible {
        groups.entry(&blob.group).or_default().push(blob);
    }
    let started = Instant::now();
    let mut writer = Writer::new(
        Vec::new(),
        WriteConfig {
            limits: limits(),
            compression_level: LEVEL,
        },
    )?;
    for versions in groups.values_mut() {
        versions.sort_by(|a, b| {
            b.bytes
                .len()
                .cmp(&a.bytes.len())
                .then_with(|| a.digest().cmp(&b.digest()))
        });
        let Some(anchor) = versions.first() else {
            continue;
        };
        let digest = writer.add(&anchor.bytes, None)?;
        for blob in &versions[1..] {
            writer.add(
                &blob.bytes,
                dictionaries.then_some((digest, anchor.bytes.as_slice())),
            )?;
        }
    }
    let (bytes, summary) = writer.finish()?;
    let encode_ns = nanos(started);
    let pack_bytes = bytes.len() as u64;
    let counter = Arc::new(AtomicU64::new(0));
    let mut reader = Reader::open(
        CountedCursor {
            inner: Cursor::new(bytes),
            bytes: counter.clone(),
        },
        limits(),
    )?;
    let metadata_open_bytes = counter.load(Ordering::Relaxed);
    for blob in &eligible {
        checked(
            reader.read_blob(blob.digest())? == blob.bytes,
            "packed correctness mismatch",
        )?;
    }
    let mut latencies = Vec::new();
    let mut requested = Vec::new();
    for _ in 0..3 {
        for index in sample_order(eligible.len()) {
            let blob = eligible[index];
            let before = counter.load(Ordering::Relaxed);
            let start = Instant::now();
            let decoded = reader.read_blob(blob.digest())?;
            let elapsed = nanos(start);
            checked(decoded == blob.bytes, "packed timed read mismatch")?;
            latencies.push(elapsed);
            requested.push(counter.load(Ordering::Relaxed) - before);
        }
    }
    let mut encoded_payload_bytes = 0;
    let mut delta_payload_bytes = 0;
    let mut delta_raw_bytes = 0;
    let mut selected_frame_requested = 0;
    for blob in &eligible {
        let before = counter.load(Ordering::Relaxed);
        let encoded = reader.read_encoded(blob.digest())?;
        selected_frame_requested += counter.load(Ordering::Relaxed) - before;
        encoded_payload_bytes += encoded.bytes.len() as u64;
        if encoded.base.is_some() {
            delta_payload_bytes += encoded.bytes.len() as u64;
            delta_raw_bytes += blob.bytes.len() as u64;
        }
    }
    Ok(json!({
        "mode":if dictionaries {"indexed_related_anchor"} else {"indexed_independent"},
        "pack_bytes_including_index":pack_bytes,"left_loose_bytes":left_loose_bytes,
        "total_payload_storage_bytes":pack_bytes+left_loose_bytes,
        "index_and_framing_bytes":pack_bytes-encoded_payload_bytes,
        "eligible_objects":eligible.len(),"raw_entries":summary.raw_entries,
        "zstd_entries":summary.zstd_entries,"delta_entries":summary.delta_entries,
        "encode_ns":encode_ns,"metadata_open_requested_bytes":metadata_open_bytes,
        "verified_buffer_read_ns":distribution(latencies),
        "selective_requested_bytes":distribution(requested),
        "base_cached_codec_estimate_only":{
            "delta_entries":summary.delta_entries,"delta_raw_bytes":delta_raw_bytes,
            "delta_encoded_frame_bytes":delta_payload_bytes,
            "all_selected_frames_requested_bytes":selected_frame_requested,
            "includes_protocol_framing":false,"implemented_wire_protocol":false
        }
    }))
}

#[derive(Default)]
struct Disk {
    files: u64,
    apparent: u64,
    allocated: Option<u64>,
}
fn disk(root: &Path) -> Result<Value> {
    fn visit(root: &Path, result: &mut Disk) -> Result<()> {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                visit(&entry.path(), result)?;
            } else if metadata.is_file() {
                result.files += 1;
                result.apparent += metadata.len();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    *result.allocated.get_or_insert(0) += metadata.blocks() * 512;
                }
            } else {
                return Err(invalid("unexpected store filesystem entry").into());
            }
        }
        Ok(())
    }
    let mut result = Disk::default();
    visit(root, &mut result)?;
    Ok(
        json!({"regular_files":result.files,"apparent_bytes":result.apparent,"allocated_bytes":result.allocated}),
    )
}
fn store_reads(store: &Store, blobs: &[Blob]) -> Result<Value> {
    for blob in blobs {
        let mut lease = store.read("benchmark", blob.descriptor().key)?;
        checked(
            lease.descriptor() == blob.descriptor(),
            "store descriptor changed",
        )?;
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes)?;
        checked(bytes == blob.bytes, "store correctness mismatch")?;
    }
    let mut times = Vec::new();
    for _ in 0..3 {
        for index in sample_order(blobs.len()) {
            let blob = &blobs[index];
            let start = Instant::now();
            let mut lease = store.read("benchmark", blob.descriptor().key)?;
            let mut bytes = Vec::new();
            lease.read_to_end(&mut bytes)?;
            let elapsed = nanos(start);
            checked(bytes == blob.bytes, "store timed read mismatch")?;
            times.push(elapsed);
        }
    }
    Ok(distribution(times))
}
fn store_component(blobs: &[Blob]) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("store");
    let store = Store::open(&root, Default::default())?;
    store.create_repository("benchmark")?;
    let start = Instant::now();
    for blob in blobs {
        store.put("benchmark", blob.descriptor(), &blob.bytes, 0)?;
    }
    let initial_write_ns = nanos(start);
    drop(store);
    // Match recovery/checkpoint lifecycle on both sides: measure reads after a
    // neutral reopen, then close before inspecting all Store filesystem state.
    // In particular, a fresh sparse journal's apparent length is not blob data.
    let store = Store::open_existing(&root, Default::default())?;
    let before_reads = store_reads(&store, blobs)?;
    drop(store);
    let before_disk = disk(&root)?;
    let store = Store::open_existing(&root, Default::default())?;
    let start = Instant::now();
    let mut reports = Vec::new();
    let mut scan_complete = false;
    let mut maintenance_calls = 0;
    for _ in 0..MAX_OBJECTS {
        let report = store.pack("benchmark", PackOptions::default())?;
        maintenance_calls += 1;
        scan_complete = report.scan_complete;
        if report.packed_objects != 0 {
            reports.push(json!({"packed_objects":report.packed_objects,"logical_bytes":report.logical_bytes,
                "pack_bytes":report.pack_bytes,"removed_loose_files":report.removed_loose_files,
                "raw_entries":report.raw_entries,"zstd_entries":report.zstd_entries,"delta_entries":report.delta_entries}));
        }
        // A bounded page can contain only large/already-packed objects. Only
        // the explicit sweep-completion signal means maintenance is finished.
        if scan_complete {
            break;
        }
    }
    checked(scan_complete, "store packing exceeded bounded sweep calls")?;
    let packing_ns = nanos(start);
    drop(store);
    let store = Store::open_existing(&root, Default::default())?;
    let after_reads = store_reads(&store, blobs)?;
    drop(store);
    let after_disk = disk(&root)?;
    let reopened = Store::open_existing(&root, Default::default())?;
    for blob in blobs {
        let mut read = reopened.read("benchmark", blob.descriptor().key)?;
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes)?;
        checked(bytes == blob.bytes, "reopened packed store mismatch")?;
    }
    Ok(json!({"before":before_disk,"after":after_disk,
        "initial_write_ns":initial_write_ns,"packing_ns":packing_ns,"packs":reports,
        "maintenance_calls":maintenance_calls,"scan_complete":scan_complete,
        "verified_read_ns_before":before_reads,"verified_read_ns_after":after_reads,
        "includes_fjall_and_staging_metadata":true,"storage_previously_verified_and_os_cache_uncontrolled":true,
        "disk_snapshot_lifecycle":"mutation, close, neutral reopen, verified reads, close, snapshot",
        "disk_metadata_caveat":"allocated bytes include database and journal lifecycle; no independent no-pack control",
        "rpc_payload_change":"none"}))
}

fn run(args: Vec<std::ffi::OsString>) -> Result<()> {
    checked(
        args.len() <= 2,
        "usage: pack_benchmark [corpus-file [public-revision-label]]",
    )?;
    let external = args
        .first()
        .map(|path| corpus(Path::new(path)))
        .transpose()?;
    let label = args
        .get(1)
        .map(|value| {
            value
                .to_str()
                .ok_or_else(|| invalid("invalid revision label"))
        })
        .transpose()?;
    if let Some(label) = label {
        checked(
            label.len() <= 128
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b)),
            "invalid public revision label",
        )?;
    }
    let input = external.unwrap_or_else(synthetic);
    let input_objects = input.len();
    let blobs = unique(input);
    checked(!blobs.is_empty(), "empty benchmark corpus")?;
    let raw_bytes: u64 = blobs.iter().map(|b| b.bytes.len() as u64).sum();
    checked(
        raw_bytes <= MAX_INPUT_BYTES && blobs.len() as u64 <= MAX_OBJECTS,
        "benchmark corpus exceeds limits",
    )?;
    let start = Instant::now();
    let mut zstd_bytes = 0u64;
    let mut independent_best_bytes = 0u64;
    for blob in &blobs {
        let compressed = zstd::bulk::compress(&blob.bytes, LEVEL)?;
        zstd_bytes += compressed.len() as u64;
        independent_best_bytes += (compressed.len() as u64).min(blob.bytes.len() as u64);
    }
    let independent_encode_ns = nanos(start);
    let independent = packed(&blobs, false)?;
    let dictionary = packed(&blobs, true)?;
    let store = store_component(&blobs)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema":1,"corpus_kind":if args.is_empty(){"synthetic"}else{"external"},"public_revision":label,
            "seed":SEED,"compression_level":LEVEL,"input_objects":input_objects,
            "codec_limits":{"max_blob_bytes":PACK_OBJECT_BYTES,"max_anchor_bytes":PACK_OBJECT_BYTES,
                "max_pack_bytes":limits().max_pack_bytes,"max_entries":MAX_OBJECTS,"maximum_delta_depth":1},
            "store_pack_options":{"max_objects":4096,"max_object_bytes":PACK_OBJECT_BYTES,
                "max_pack_bytes":8*1024*1024,"max_scan_objects":16384,"compression_level":LEVEL,"use_dictionaries":true},
            "unique_blobs":blobs.len(),"unique_groups":blobs.iter().map(|b| &b.group).collect::<std::collections::BTreeSet<_>>().len(),
            "raw_loose_bytes":raw_bytes,"raw_loose_files":blobs.len(),
            "independent_zstd_payload_bytes":zstd_bytes,"independent_raw_or_zstd_payload_bytes":independent_best_bytes,
            "independent_encode_ns":independent_encode_ns,"codec_cases":[independent,dictionary],"store_component":store,
            "boundaries":{
                "codec_timing":"verified in-memory compressed-buffer reads; not cold disk or network",
                "requested_bytes":"Read calls satisfied by an instrumented seekable buffer; not physical disk IO",
                "store_timing":"full public Store read including integrity checks; OS cache uncontrolled",
                "wire":"current repository RPC payloads unchanged; encoded delta frames are a codec estimate only",
                "not_measured":["end-to-end Bazel latency","process RSS","remote object-store request overhead","RPC bytes"]
            }
        }))?
    );
    Ok(())
}
fn main() {
    if let Err(error) = run(std::env::args_os().skip(1).collect()) {
        eprintln!("pack benchmark failed: {error}");
        std::process::exit(1);
    }
}

#[test]
fn matched_benchmark() {
    let mut args = Vec::new();
    if let Some(path) = std::env::var_os("PACKBLOB_BENCH_CORPUS") {
        args.push(path);
        if let Some(revision) = std::env::var_os("PACKBLOB_BENCH_REVISION") {
            args.push(revision);
        }
    }
    run(args).expect("matched benchmark failed integrity or bounds validation");
}
