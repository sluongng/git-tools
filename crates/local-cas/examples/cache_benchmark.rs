//! Deterministic, network-free public-API benchmark. Run an optimized binary:
//! cargo run --release -p local-cas --example cache_benchmark -- OUTPUT_DIR LABEL
//! OUTPUT_DIR must not exist. Receipts are JSON; keep them outside the checkout.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use local_cas::{Artifact, BlobSource, CacheConfig, CacheMetrics, LocalCas};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

struct Source {
    data: HashMap<String, Vec<u8>>,
    fetches: AtomicU64,
    bytes: AtomicU64,
}
#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()> {
        let bytes = self
            .data
            .get(&artifact.hash)
            .context("unknown fixture digest")?;
        target.write_all(bytes)?;
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
}

fn fixture(count: usize, size: usize) -> (Arc<Source>, Vec<Artifact>) {
    let mut data = HashMap::new();
    let artifacts = (0..count)
        .map(|index| {
            let mut bytes = vec![0; size];
            // Each fixture is distinct, reproducible, and contains nonconstant data.
            for (block, chunk) in bytes.chunks_mut(32).enumerate() {
                let hash = blake3::hash(format!("local-cas-bench:{index}:{block}").as_bytes());
                chunk.copy_from_slice(&hash.as_bytes()[..chunk.len()]);
            }
            let hash = blake3::hash(&bytes).to_hex().to_string();
            data.insert(hash.clone(), bytes);
            Artifact {
                backend: "synthetic-benchmark".into(),
                instance: String::new(),
                digest_function: 9,
                hash,
                size: size as u64,
            }
        })
        .collect();
    (
        Arc::new(Source {
            data,
            fetches: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }),
        artifacts,
    )
}
fn config(bytes: u64) -> CacheConfig {
    CacheConfig {
        max_bytes: bytes,
        max_inflight_bytes: bytes,
        max_concurrent_fetches: 8,
        operation_timeout: Duration::from_secs(120),
        fetch_timeout: Duration::from_secs(120),
    }
}
fn disk(path: &Path) -> Result<Value> {
    let mut payload_bytes = 0;
    let mut temporary_bytes = 0;
    let mut payload_files = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let len = entry.metadata()?.len();
        if entry.path().extension().is_some_and(|ext| ext == "tmp") {
            temporary_bytes += len;
        } else if entry.file_name().to_string_lossy().len() == 64 {
            payload_bytes += len;
            payload_files += 1;
        }
    }
    Ok(
        json!({"payload_bytes": payload_bytes, "payload_files": payload_files, "temporary_bytes": temporary_bytes}),
    )
}
fn metrics_delta(before: CacheMetrics, after: CacheMetrics) -> Value {
    json!({ "fetches": after.fetches - before.fetches, "hits": after.hits - before.hits,
        "downloaded_verified_bytes": after.verified_bytes - before.verified_bytes,
        "verification_bytes": after.verification_bytes - before.verification_bytes,
        "admission_wait_ns": after.admission_wait_ns - before.admission_wait_ns })
}
fn result(
    name: &str,
    retained: usize,
    iteration: usize,
    elapsed: Duration,
    mut samples: Vec<u64>,
    before: CacheMetrics,
    cache: &LocalCas,
    dir: &Path,
) -> Result<Value> {
    samples.sort_unstable();
    let n = samples.len();
    let percentile = |p: usize| samples.get((n.saturating_sub(1) * p) / 100).copied();
    Ok(
        json!({"scenario": name, "retained_blobs": retained, "iteration": iteration,
        "operations": n, "wall_seconds": elapsed.as_secs_f64(), "operations_per_second": n as f64 / elapsed.as_secs_f64(),
        "latency_ns": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
        "metrics": metrics_delta(before, cache.metrics()), "disk_after": disk(dir)? }),
    )
}
async fn accesses(
    cache: Arc<LocalCas>,
    artifacts: Arc<Vec<Artifact>>,
    operations: usize,
    offset: usize,
) -> Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(operations);
    for index in 0..operations {
        let artifact = artifacts[(offset + index * 7919) % artifacts.len()].clone();
        let start = Instant::now();
        let lease = cache.clone().content(artifact).await?;
        std::hint::black_box(lease.path());
        drop(lease);
        samples.push(start.elapsed().as_nanos() as u64);
    }
    Ok(samples)
}

async fn warm(root: &Path, retained: usize, rows: &mut Vec<Value>) -> Result<()> {
    let setup = Instant::now();
    let (source, artifacts) = fixture(retained, 1024);
    let artifacts = Arc::new(artifacts);
    let dir = root.join(format!("warm-{retained}"));
    let mut cache = LocalCas::open(&dir, source.clone(), config(retained as u64 * 1024))?;
    for artifact in artifacts.iter() {
        drop(cache.clone().content(artifact.clone()).await?);
    }
    rows.push(json!({"scenario": "fixture_setup", "retained_blobs": retained, "wall_seconds": setup.elapsed().as_secs_f64(), "disk_after": disk(&dir)?}));
    ensure!(
        source.fetches.load(Ordering::Relaxed) == retained as u64,
        "setup fetch count mismatch"
    );
    for iteration in 0..3 {
        let before = cache.metrics();
        let start = Instant::now();
        let samples = accesses(cache.clone(), artifacts.clone(), 8192, iteration).await?;
        rows.push(result(
            "warm_hit_serial",
            retained,
            iteration,
            start.elapsed(),
            samples,
            before,
            &cache,
            &dir,
        )?);
        let before = cache.metrics();
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for reader in 0..8 {
            tasks.spawn(accesses(
                cache.clone(),
                artifacts.clone(),
                1024,
                reader * 997 + iteration,
            ));
        }
        let mut samples = Vec::new();
        while let Some(task) = tasks.join_next().await {
            samples.extend(task??);
        }
        rows.push(result(
            "warm_hit_8_readers",
            retained,
            iteration,
            start.elapsed(),
            samples,
            before,
            &cache,
            &dir,
        )?);
        ensure!(
            source.fetches.load(Ordering::Relaxed) == retained as u64,
            "warm hit unexpectedly fetched"
        );
    }
    for iteration in 0..3 {
        drop(cache);
        let start = Instant::now();
        cache = LocalCas::open(&dir, source.clone(), config(retained as u64 * 1024))?;
        rows.push(json!({"scenario": "restart_open", "retained_blobs": retained, "iteration": iteration, "wall_seconds": start.elapsed().as_secs_f64()}));
        let before = cache.metrics();
        let start = Instant::now();
        let samples = accesses(cache.clone(), artifacts.clone(), retained, 0).await?;
        rows.push(result(
            "restart_first_verification",
            retained,
            iteration,
            start.elapsed(),
            samples,
            before,
            &cache,
            &dir,
        )?);
        ensure!(
            source.fetches.load(Ordering::Relaxed) == retained as u64,
            "restart unexpectedly fetched"
        );
        ensure!(
            cache.metrics().verification_bytes == retained as u64 * 1024,
            "restart did not verify all retained bytes"
        );
    }
    Ok(())
}

async fn churn(root: &Path, retained: usize, rows: &mut Vec<Value>) -> Result<()> {
    const ARRIVALS: usize = 2048;
    const PINNED: usize = 64;
    let setup = Instant::now();
    let (source, artifacts) = fixture(retained + ARRIVALS * 3, 4096);
    let dir = root.join(format!("churn-{retained}"));
    let cache = LocalCas::open(&dir, source.clone(), config(retained as u64 * 4096))?;
    let mut pinned = Vec::new();
    for (index, artifact) in artifacts[..retained].iter().enumerate() {
        let lease = cache.clone().content(artifact.clone()).await?;
        if index < PINNED {
            pinned.push(lease);
        }
    }
    rows.push(json!({"scenario": "churn_setup", "retained_blobs": retained, "wall_seconds": setup.elapsed().as_secs_f64(), "disk_after": disk(&dir)?}));
    for iteration in 0..3 {
        let before = cache.metrics();
        let start = Instant::now();
        let mut samples = Vec::new();
        for artifact in
            &artifacts[retained + iteration * ARRIVALS..retained + (iteration + 1) * ARRIVALS]
        {
            let one = Instant::now();
            drop(cache.clone().content(artifact.clone()).await?);
            samples.push(one.elapsed().as_nanos() as u64);
        }
        rows.push(result(
            "quota_churn_pinned_hotset",
            retained,
            iteration,
            start.elapsed(),
            samples,
            before,
            &cache,
            &dir,
        )?);
        ensure!(
            cache.metrics().fetches - before.fetches == ARRIVALS as u64,
            "churn fetch count mismatch"
        );
        let on_disk = disk(&dir)?;
        ensure!(
            on_disk["payload_bytes"].as_u64() == Some(retained as u64 * 4096),
            "quota or pin accounting mismatch"
        );
        ensure!(
            on_disk["temporary_bytes"].as_u64() == Some(0),
            "temporary files leaked"
        );
        for (index, lease) in pinned.iter().enumerate() {
            ensure!(
                fs::read(lease.path())? == source.data[&artifacts[index].hash],
                "pinned content reclaimed or changed"
            );
        }
    }
    Ok(())
}

async fn large_admission(root: &Path, retained: usize, rows: &mut Vec<Value>) -> Result<()> {
    let setup = Instant::now();
    let (mut source, artifacts) = fixture(retained, 1024);
    let (large_source, large_artifacts) = fixture(3, retained / 4 * 1024);
    Arc::get_mut(&mut source)
        .unwrap()
        .data
        .extend(Arc::try_unwrap(large_source).ok().unwrap().data);
    let dir = root.join(format!("large-admission-{retained}"));
    let cache = LocalCas::open(&dir, source.clone(), config(retained as u64 * 1024))?;
    let mut pinned = Vec::new();
    for (index, artifact) in artifacts.iter().enumerate() {
        let lease = cache.clone().content(artifact.clone()).await?;
        if index < 64 {
            pinned.push(lease);
        }
    }
    rows.push(json!({"scenario": "large_admission_setup", "retained_blobs": retained, "wall_seconds": setup.elapsed().as_secs_f64(), "disk_after": disk(&dir)?}));
    for (iteration, artifact) in large_artifacts.into_iter().enumerate() {
        let before = cache.metrics();
        let start = Instant::now();
        drop(cache.clone().content(artifact).await?);
        let elapsed = start.elapsed();
        let mut row = result(
            "large_blob_many_victims",
            retained,
            iteration,
            elapsed,
            vec![elapsed.as_nanos() as u64],
            before,
            &cache,
            &dir,
        )?;
        row["expected_evicted_small_blobs"] = json!(retained / 4);
        rows.push(row);
        let on_disk = disk(&dir)?;
        ensure!(
            on_disk["payload_bytes"].as_u64() == Some(retained as u64 * 1024),
            "large admission exceeded quota"
        );
        ensure!(
            on_disk["temporary_bytes"].as_u64() == Some(0),
            "large admission left temporary files"
        );
        ensure!(
            on_disk["payload_files"].as_u64()
                == Some((retained - (iteration + 1) * (retained / 4) + iteration + 1) as u64),
            "unexpected large admission victim count"
        );
        for (index, lease) in pinned.iter().enumerate() {
            ensure!(
                fs::read(lease.path())? == source.data[&artifacts[index].hash],
                "large admission reclaimed pinned hot content"
            );
        }
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .context("usage: cache_benchmark OUTPUT_DIR LABEL")?,
    );
    let label = args.next().context("missing receipt label")?;
    ensure!(!root.exists(), "OUTPUT_DIR already exists");
    fs::create_dir_all(&root)?;
    let start = Instant::now();
    let mut rows = Vec::new();
    for retained in [1000, 10000, 30000] {
        eprintln!("{label}: warm/restart retained={retained}");
        warm(&root, retained, &mut rows).await?;
    }
    for retained in [1000, 10000, 30000] {
        eprintln!("{label}: quota churn retained={retained}");
        churn(&root, retained, &mut rows).await?;
    }
    for retained in [1000, 10000, 30000] {
        eprintln!("{label}: large admission retained={retained}");
        large_admission(&root, retained, &mut rows).await?;
    }
    let receipt = json!({"schema_version": 1, "label": label, "wall_seconds": start.elapsed().as_secs_f64(),
        "fixture": {"digest": "BLAKE3", "network": false, "warm_blob_bytes": 1024, "churn_blob_bytes": 4096,
            "readers": 8, "repetitions": 3, "churn_arrivals_per_repetition": 2048, "pinned_hot_blobs": 64,
            "note": "Warm calls measure obtaining and releasing verified leases, not reading file payloads. Restart is process-cache cold with warm OS page cache. Disk snapshots include temporary bytes after each phase, not peak allocated disk blocks. Churn eviction count equals fetched arrivals because fixtures have equal sizes and start at quota."},
        "results": rows});
    let encoded = serde_json::to_string_pretty(&receipt)?;
    fs::write(root.join("receipt.json"), &encoded)?;
    println!("{encoded}");
    Ok(())
}
