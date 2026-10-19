//! Bounded incremental history and an independent conservative index-bootstrap set.
use super::{Handles, Query, Response, number};
use anyhow::{Context, Result, ensure};
use fjall::{PersistMode, Readable, SingleWriterWriteTx};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const GENERATION: &[u8] = b"journal_generation";
const STATE: &[u8] = b"journal_bounds";
const MAX_RECORDS: u64 = 8192;
const MAX_RECORD_BYTES: u64 = 4 * 1024 * 1024;
const GENERATION_WINDOW: u64 = 4096;
const MAX_PATHS: usize = 4096;
const MAX_PATH_BYTES: usize = 1024 * 1024;
const MAX_WIRE_BYTES: usize = 8 * 1024 * 1024;
// Covers JSON field names, fixed identity, generation, separators and error:null.
const RESPONSE_OVERHEAD: usize = 256;

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bounds {
    floor: u64,
    records: u64,
    bytes: u64,
    dirty_count: u64,
    dirty_bytes: u64,
    dirty_wire: u64,
}
#[derive(Debug, Serialize)]
pub(crate) struct JournalStats {
    pub generation: u64,
    pub retained_floor: u64,
    pub retained_records: u64,
    pub retained_bytes: u64,
    pub dirty_paths: u64,
    pub dirty_path_bytes: u64,
    pub dirty_wire_bytes: u64,
}

pub(super) fn generation(tx: &impl Readable, h: &Handles) -> Result<u64> {
    let value = match tx.get(&h.meta, GENERATION)? {
        Some(value) => value,
        None => tx
            .get(&h.meta, b"generation")?
            .context("overlay generation missing")?,
    };
    number(&value)
}
fn bounds(tx: &impl Readable, h: &Handles) -> Result<Bounds> {
    let value = tx
        .get(&h.meta, STATE)?
        .context("overlay journal bounds missing")?;
    ensure!(value.len() <= 512, "oversized overlay journal bounds");
    let state: Bounds = serde_json::from_slice(&value)?;
    ensure!(
        state.floor <= generation(tx, h)?
            && state.records <= MAX_RECORDS
            && state.bytes <= MAX_RECORD_BYTES
            && state.dirty_count <= MAX_PATHS as u64
            && state.dirty_bytes <= MAX_PATH_BYTES as u64
            && state.dirty_wire <= (MAX_WIRE_BYTES - RESPONSE_OVERHEAD) as u64,
        "invalid overlay journal bounds"
    );
    Ok(state)
}
fn save(tx: &mut SingleWriterWriteTx<'_>, h: &Handles, state: &Bounds) -> Result<()> {
    tx.insert(&h.meta, STATE, serde_json::to_vec(state)?);
    Ok(())
}

/// Called only with the exclusive owner lock, after legacy pending redo recovery.
pub(super) fn initialize(h: &Handles) -> Result<()> {
    let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
    if tx.get(&h.meta, GENERATION)?.is_none() {
        let generation = generation(&tx, h)?;
        ensure!(
            tx.iter(&h.journal_v3).next().is_none() && tx.iter(&h.dirty_paths).next().is_none(),
            "uninitialized bounded journal contains records"
        );
        let mut state = Bounds {
            floor: generation,
            ..Bounds::default()
        };
        // Old history may be enormous. A single conservative marker preserves
        // interrupted index setup without reading it into memory or one big tx.
        if generation > 0 {
            tx.insert(&h.dirty_paths, b"/", b"");
            state.dirty_count = 1;
            state.dirty_bytes = 1;
            state.dirty_wire = wire_path(b"/") as u64;
        }
        tx.insert(&h.meta, GENERATION, generation.to_be_bytes());
        tx.remove(&h.meta, b"generation");
        save(&mut tx, h, &state)?;
    } else {
        bounds(&tx, h)?;
        ensure!(
            tx.get(&h.meta, b"generation")?.is_none(),
            "legacy generation survived bounded journal activation"
        );
    }
    tx.commit()?;
    // Activation is already durable. Restarting midway resumes this disposable
    // legacy-history cleanup, with a fixed maximum transaction size.
    loop {
        let read = h.db.read_tx();
        let keys = read
            .iter(&h.journal)
            .take(128)
            .map(|item| item.key().map(|key| key.to_vec()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(read);
        if keys.is_empty() {
            break;
        }
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        for key in keys {
            tx.remove(&h.journal, key);
        }
        tx.commit()?;
    }
    Ok(())
}

// Vec<u8> is encoded as numeric JSON, not a string: 255 costs three bytes.
fn wire_path(path: &[u8]) -> usize {
    2 + path
        .iter()
        .map(|byte| match byte {
            0..=9 => 1,
            10..=99 => 2,
            _ => 3,
        })
        .sum::<usize>()
        + path.len().saturating_sub(1)
        + 1
}
#[derive(Default)]
struct Paths {
    values: BTreeSet<Vec<u8>>,
    bytes: usize,
    wire: usize,
    overflow: bool,
}
impl Paths {
    fn add(&mut self, path: &[u8]) {
        if self.overflow || self.values.contains(path) {
            return;
        }
        // Check raw length before inspecting bytes or allocating a copy.
        if path.is_empty()
            || path == b"/"
            || self.values.len() == MAX_PATHS
            || path.len() > MAX_PATH_BYTES.saturating_sub(self.bytes)
        {
            self.collapse();
            return;
        }
        let wire = wire_path(path);
        if wire > (MAX_WIRE_BYTES - RESPONSE_OVERHEAD).saturating_sub(self.wire) {
            self.collapse();
            return;
        }
        self.bytes += path.len();
        self.wire += wire;
        self.values.insert(path.to_vec());
    }
    fn collapse(&mut self) {
        self.values.clear();
        self.values.insert(b"/".to_vec());
        self.bytes = 1;
        self.wire = wire_path(b"/");
        self.overflow = true;
    }
    fn into_vec(self) -> Vec<Vec<u8>> {
        self.values.into_iter().collect()
    }
}
fn dirty_append(
    tx: &mut SingleWriterWriteTx<'_>,
    h: &Handles,
    state: &mut Bounds,
    paths: &Paths,
) -> Result<()> {
    if tx.contains_key(&h.dirty_paths, b"/")? {
        return Ok(());
    }
    for path in &paths.values {
        if !path.is_empty()
            && path.len() <= u16::MAX as usize
            && tx.contains_key(&h.dirty_paths, path)?
        {
            continue;
        }
        let wire = wire_path(path) as u64;
        if path == b"/"
            || path.is_empty()
            || path.len() > u16::MAX as usize
            || state.dirty_count >= MAX_PATHS as u64
            || path.len() as u64 > (MAX_PATH_BYTES as u64).saturating_sub(state.dirty_bytes)
            || wire > ((MAX_WIRE_BYTES - RESPONSE_OVERHEAD) as u64).saturating_sub(state.dirty_wire)
        {
            let keys = tx
                .iter(&h.dirty_paths)
                .take(MAX_PATHS + 1)
                .map(|item| item.key().map(|key| key.to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ensure!(keys.len() <= MAX_PATHS, "oversized persistent dirty set");
            for key in keys {
                tx.remove(&h.dirty_paths, key);
            }
            tx.insert(&h.dirty_paths, b"/", b"");
            state.dirty_count = 1;
            state.dirty_bytes = 1;
            state.dirty_wire = wire_path(b"/") as u64;
            return Ok(());
        }
        tx.insert(&h.dirty_paths, path, b"");
        state.dirty_count += 1;
        state.dirty_bytes += path.len() as u64;
        state.dirty_wire += wire;
    }
    Ok(())
}

/// Caller commits entry changes, history trimming and floor in this same tx.
pub(super) fn append(
    tx: &mut SingleWriterWriteTx<'_>,
    h: &Handles,
    paths: &[Vec<u8>],
) -> Result<u64> {
    // Recovery before first activation must still publish the legacy intent.
    if tx.get(&h.meta, GENERATION)?.is_none() {
        let generation = generation(tx, h)?
            .checked_add(1)
            .context("overlay generation overflow")?;
        let mut unique = Paths::default();
        for path in paths {
            unique.add(path);
            if unique.overflow {
                break;
            }
        }
        for (ordinal, path) in unique.values.iter().enumerate() {
            let mut key = [0; 16];
            key[..8].copy_from_slice(&generation.to_be_bytes());
            key[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
            tx.insert(&h.journal, key, path.as_slice());
        }
        tx.insert(&h.meta, b"generation", generation.to_be_bytes());
        return Ok(generation);
    }
    let mut state = bounds(tx, h)?;
    let generation = generation(tx, h)?
        .checked_add(1)
        .context("overlay generation overflow")?;
    let mut unique = Paths::default();
    for path in paths {
        unique.add(path);
        if unique.overflow {
            break;
        }
    }
    dirty_append(tx, h, &mut state, &unique)?;
    for (ordinal, path) in unique.values.iter().enumerate() {
        let mut key = [0; 16];
        key[..8].copy_from_slice(&generation.to_be_bytes());
        key[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
        tx.insert(&h.journal_v3, key, path.as_slice());
        state.records += 1;
        state.bytes += 16 + path.len() as u64;
    }
    let window_floor = generation.saturating_sub(GENERATION_WINDOW);
    // Delete complete generations only. The previous retained set is bounded;
    // adding one bounded batch cannot create an unbounded iterator/transaction.
    let mut removed = Vec::new();
    let mut evict_generation = None;
    // Seek past the durable floor instead of rescanning deleted keys from the
    // start. LSM tombstones can remain in memory long after logical eviction.
    let first_retained = state
        .floor
        .checked_add(1)
        .context("journal floor overflow")?;
    for item in tx.range(&h.journal_v3, first_retained.to_be_bytes().to_vec()..) {
        let (key, path) = item.into_inner()?;
        ensure!(key.len() == 16, "invalid bounded journal key");
        let record_generation = number(&key[..8])?;
        let needs_eviction = state.records > MAX_RECORDS
            || state.bytes > MAX_RECORD_BYTES
            || record_generation <= window_floor;
        if !needs_eviction && evict_generation != Some(record_generation) {
            break;
        }
        evict_generation = Some(record_generation);
        state.floor = state.floor.max(record_generation);
        state.records = state
            .records
            .checked_sub(1)
            .context("journal record accounting underflow")?;
        state.bytes = state
            .bytes
            .checked_sub(16 + path.len() as u64)
            .context("journal byte accounting underflow")?;
        removed.push(key.to_vec());
    }
    for key in removed {
        tx.remove(&h.journal_v3, key);
    }
    state.floor = state.floor.max(window_floor);
    tx.insert(&h.meta, GENERATION, generation.to_be_bytes());
    save(tx, h, &state)?;
    Ok(generation)
}

fn identity(tx: &impl Readable, h: &Handles) -> Result<String> {
    let raw = tx
        .get(&h.meta, b"identity")?
        .context("overlay identity missing")?;
    ensure!(
        raw.len() == 32 && raw.iter().all(u8::is_ascii_hexdigit),
        "invalid overlay identity"
    );
    Ok(std::str::from_utf8(&raw)?.to_owned())
}
pub(super) fn query(h: &Handles, request: Query) -> Result<Response> {
    let tx = h.db.read_tx();
    let generation = generation(&tx, h)?;
    let identity = identity(&tx, h)?;
    let token = format!("v3:{identity}:{generation}");
    let state = bounds(&tx, h)?;
    let Some(previous) = request.token else {
        let mut paths = Paths::default();
        for item in tx.iter(&h.dirty_paths) {
            let (key, _) = item.into_inner()?;
            paths.add(&key);
            if paths.overflow {
                break;
            }
        }
        return Ok(Response {
            token,
            paths: paths.into_vec(),
            error: None,
        });
    };
    let mut parts = previous.split(':');
    let after = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("v3"), Some(id), Some(value), None) if id == identity => value.parse::<u64>().ok(),
        _ => None,
    };
    let after = after.filter(|n| *n >= state.floor && *n <= generation);
    let mut paths = Paths::default();
    if let Some(after) = after {
        if let Some(first) = after.checked_add(1) {
            for (scanned, item) in tx
                .range(&h.journal_v3, first.to_be_bytes().to_vec()..)
                .enumerate()
            {
                if scanned >= MAX_RECORDS as usize {
                    paths.collapse();
                    break;
                }
                let (key, path) = item.into_inner()?;
                ensure!(key.len() == 16, "invalid bounded journal key");
                paths.add(&path);
                if paths.overflow {
                    break;
                }
            }
        }
    } else {
        paths.collapse();
    }
    Ok(Response {
        token,
        paths: paths.into_vec(),
        error: None,
    })
}
pub(super) fn stats(h: &Handles) -> Result<JournalStats> {
    let tx = h.db.read_tx();
    let state = bounds(&tx, h)?;
    Ok(JournalStats {
        generation: generation(&tx, h)?,
        retained_floor: state.floor,
        retained_records: state.records,
        retained_bytes: state.bytes,
        dirty_paths: state.dirty_count,
        dirty_path_bytes: state.dirty_bytes,
        dirty_wire_bytes: state.dirty_wire,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{GitRepo, SizeFallback},
        filesystem::Snapshot,
        overlay::{self, Kind, Overlay},
    };
    use std::{ffi::OsStr, path::PathBuf, process::Command};

    fn fixture() -> (tempfile::TempDir, Snapshot, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let git = temp.path().join("git");
        std::fs::create_dir(&git).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Fixture"],
            vec!["config", "user.email", "fixture@example.test"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&git)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&git)
                .args([
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "Fixture"
                ])
                .status()
                .unwrap()
                .success()
        );
        let bare = temp.path().join("bare.git");
        assert!(
            Command::new("git")
                .args(["clone", "--bare", "--quiet"])
                .arg(&git)
                .arg(&bare)
                .status()
                .unwrap()
                .success()
        );
        let git = bare;
        let repo = GitRepo::open(&git, "git", "origin", SizeFallback::Error).unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        let directory = temp.path().join("overlay");
        (temp, snapshot, directory, git)
    }
    fn append_batch(h: &Handles, paths: &[Vec<u8>]) {
        let mut tx = h.db.write_tx();
        append(&mut tx, h, paths).unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn expired_floor_current_and_recent_tokens_preserve_complete_changes() {
        let (_temp, snapshot, directory, git) = fixture();
        let mut fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        fs.create(1, OsStr::new("first-edit"), Kind::File, 0o600, Some(vec![]))
            .unwrap();
        let (old, _) = overlay::changes_since(&directory, "").unwrap();
        let h = fs.database.handles.as_ref().unwrap();
        // Cross several retention windows while old deleted keys still exist
        // physically; floor seeks must preserve the exact retained suffix.
        for _ in 0..GENERATION_WINDOW * 3 {
            append_batch(h, &[b"repeated".to_vec()]);
        }
        let (floor_token, _) = overlay::changes_since(&directory, "").unwrap();
        append_batch(h, &[b"last".to_vec()]);
        assert_eq!(
            overlay::changes_since(&directory, &old).unwrap().1,
            vec![b"/".to_vec()]
        );
        assert_eq!(
            overlay::changes_since(&directory, &floor_token).unwrap().1,
            vec![b"last".to_vec()]
        );
        let state = stats(h).unwrap();
        assert_eq!(state.retained_records, GENERATION_WINDOW);
        let identity = old.split(':').nth(1).unwrap();
        let exact_floor = format!("v3:{identity}:{}", state.retained_floor);
        let (current, paths) = overlay::changes_since(&directory, &exact_floor).unwrap();
        assert_eq!(paths, vec![b"last".to_vec(), b"repeated".to_vec()]);
        assert!(
            overlay::changes_since(&directory, &current)
                .unwrap()
                .1
                .is_empty()
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![
                b"first-edit".to_vec(),
                b"last".to_vec(),
                b"repeated".to_vec()
            ]
        );
        fs.sync().unwrap();
        drop(fs);
        assert_eq!(
            overlay::changes_since(&directory, &current).unwrap(),
            (current, vec![])
        );
        assert!(
            overlay::dirty_paths(&directory)
                .unwrap()
                .contains(&b"first-edit".to_vec())
        );
    }

    #[test]
    fn record_eviction_is_whole_generation_and_overflow_is_sticky_full_invalidation() {
        let (_temp, snapshot, directory, git) = fixture();
        let mut fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        let (start, _) = overlay::changes_since(&directory, "").unwrap();
        let h = fs.database.handles.as_ref().unwrap();
        let batch = |prefix| {
            (0..MAX_PATHS)
                .map(|n| format!("{prefix}-{n:04}").into_bytes())
                .collect::<Vec<_>>()
        };
        append_batch(h, &batch("first"));
        let (first, _) = overlay::changes_since(&directory, "").unwrap();
        append_batch(h, &batch("second"));
        // Exact histories larger than the query path budget never become a
        // successful partial response; bootstrap overflow persists separately.
        assert_eq!(
            overlay::changes_since(&directory, &start).unwrap().1,
            vec![b"/".to_vec()]
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
        append_batch(h, &batch("third"));
        let state = stats(h).unwrap();
        assert_eq!(state.retained_floor, 1);
        assert_eq!(state.retained_records, MAX_RECORDS);
        assert_eq!(
            overlay::changes_since(&directory, &start).unwrap().1,
            vec![b"/".to_vec()]
        );
        assert_eq!(
            overlay::changes_since(&directory, &first).unwrap().1,
            vec![b"/".to_vec()]
        );
        append_batch(h, &[vec![255; MAX_PATH_BYTES + 1]]);
        let (current, _) = overlay::changes_since(&directory, "").unwrap();
        assert!(
            overlay::changes_since(&directory, &current)
                .unwrap()
                .1
                .is_empty()
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
        fs.sync().unwrap();
        drop(fs);
        let fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
        drop(fs);
    }

    #[test]
    fn legacy_activation_invalidates_old_helpers_and_retires_history_in_batches() {
        let (_temp, snapshot, directory, git) = fixture();
        drop(Overlay::open(&directory, &snapshot, &git).unwrap());
        let h = Handles::open(&directory, false).unwrap();
        let id = identity(&h.db.read_tx(), &h).unwrap();
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.remove(&h.meta, GENERATION);
        tx.remove(&h.meta, STATE);
        tx.insert(&h.meta, b"generation", 7u64.to_be_bytes());
        for n in 0u64..260 {
            let mut key = [0; 16];
            key[..8].copy_from_slice(&7u64.to_be_bytes());
            key[8..].copy_from_slice(&n.to_be_bytes());
            tx.insert(&h.journal, key, b"legacy-edit");
        }
        tx.commit().unwrap();
        drop(h);
        let fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        let h = fs.database.handles.as_ref().unwrap();
        assert!(
            h.db.read_tx()
                .get(&h.meta, b"generation")
                .unwrap()
                .is_none()
        );
        assert!(h.db.read_tx().iter(&h.journal).next().is_none());
        let (token, paths) = overlay::changes_since(&directory, &format!("v2:{id}:7")).unwrap();
        assert_eq!(token, format!("v3:{id}:7"));
        assert_eq!(paths, vec![b"/".to_vec()]);
        assert!(
            overlay::changes_since(&directory, &token)
                .unwrap()
                .1
                .is_empty()
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
    }

    #[test]
    fn legacy_pending_payload_redo_recovers_before_bounded_history_activation() {
        use crate::overlay::{
            codec,
            payload::{Files, Intent, Operation, Payload, fail_next},
        };
        use sha1::{Digest, Sha1};

        let (_temp, snapshot, directory, git) = fixture();
        let original = vec![7; 128 * 1024];
        let patch = vec![9; 96 * 1024];
        let offset = 4096;
        let mut expected = original.clone();
        expected[offset..offset + patch.len()].copy_from_slice(&patch);
        let mut fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        let inode = fs
            .create(
                1,
                OsStr::new("recover-edit"),
                Kind::File,
                0o600,
                Some(original.clone()),
            )
            .unwrap();
        let before = fs.entry(inode).unwrap().clone();
        let Payload::File {
            size,
            generation: content_generation,
        } = before.payload
        else {
            panic!("fixture must use an owned payload");
        };
        let mut after = before.clone();
        after.payload = Payload::File {
            size,
            generation: content_generation + 1,
        };
        let (prior_token, _) = overlay::changes_since(&directory, "").unwrap();
        let id = prior_token.split(':').nth(1).unwrap().to_owned();
        fs.sync().unwrap();
        drop(fs);

        let h = Handles::open(&directory, false).unwrap();
        let before_generation = generation(&h.db.read_tx(), &h).unwrap();
        let files = Files::open(&directory).unwrap();
        files.stage(&patch).unwrap();
        let operation = Operation::Write {
            offset: offset as u64,
            length: patch.len() as u64,
            hash: blake3::hash(&patch).to_hex().to_string(),
        };
        let intent = Intent {
            before_generation,
            before_size: size,
            before_entry: codec::encode(&before),
            after_entry: codec::encode(&after),
            paths: vec![b"recover-edit".to_vec()],
            operation: operation.clone(),
        };
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.remove(&h.meta, GENERATION);
        tx.remove(&h.meta, STATE);
        tx.insert(&h.meta, b"generation", before_generation.to_be_bytes());
        let old_history = tx
            .iter(&h.journal_v3)
            .map(|item| item.into_inner().unwrap())
            .collect::<Vec<_>>();
        for (key, value) in old_history {
            tx.insert(&h.journal, &*key, &*value);
            tx.remove(&h.journal_v3, key);
        }
        let dirty = tx
            .iter(&h.dirty_paths)
            .map(|item| item.key().unwrap())
            .collect::<Vec<_>>();
        for key in dirty {
            tx.remove(&h.dirty_paths, key);
        }
        tx.insert(
            &h.meta,
            b"payload_intent",
            serde_json::to_vec(&intent).unwrap(),
        );
        tx.commit().unwrap();
        // Leave a real partially applied range, not merely a pending metadata
        // record: recovery must replay all staged bytes before migrating history.
        fail_next("after_first_write");
        assert!(files.apply(inode, size, size, &operation).is_err());
        assert_eq!(files.read(inode, size, offset as u64, 1).unwrap(), vec![9]);
        assert_eq!(
            files
                .read(inode, size, offset as u64 + 64 * 1024, 1)
                .unwrap(),
            vec![7]
        );
        drop(files);
        drop(h);

        let mut repo = GitRepo::open(&git, "git", "origin", SizeFallback::Error).unwrap();
        let mut fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        assert_eq!(fs.read(inode, &mut repo).unwrap(), expected);
        let mut sha1 = Sha1::new();
        sha1.update(format!("blob {}\0", expected.len()));
        sha1.update(&expected);
        let expected_sha1 = sha1.finalize().to_vec();
        assert_eq!(fs.digest(inode).unwrap(), expected_sha1);
        assert_eq!(
            fs.blake3_digest(inode).unwrap(),
            *blake3::hash(&expected).as_bytes()
        );
        assert_eq!(fs.stats().recovered_operations, 1);
        let h = fs.database.handles.as_ref().unwrap();
        let state = stats(h).unwrap();
        assert_eq!(state.generation, before_generation + 1);
        assert_eq!(state.retained_floor, state.generation);
        assert_eq!(state.retained_records, 0);
        let tx = h.db.read_tx();
        assert!(tx.get(&h.meta, b"payload_intent").unwrap().is_none());
        assert!(tx.get(&h.meta, b"generation").unwrap().is_none());
        assert!(tx.iter(&h.journal).next().is_none());
        drop(tx);
        let expected_token = format!("v3:{id}:{}", before_generation + 1);
        let old_v2 = format!("v2:{id}:{before_generation}");
        assert_eq!(
            overlay::changes_since(&directory, &old_v2).unwrap(),
            (expected_token.clone(), vec![b"/".to_vec()])
        );
        assert_eq!(
            overlay::changes_since(&directory, &expected_token).unwrap(),
            (expected_token.clone(), vec![])
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
        fs.sync().unwrap();
        drop(fs);

        let mut fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        assert_eq!(fs.stats().recovered_operations, 0);
        assert_eq!(fs.read(inode, &mut repo).unwrap(), expected);
        assert_eq!(fs.digest(inode).unwrap(), expected_sha1);
        assert_eq!(
            fs.blake3_digest(inode).unwrap(),
            *blake3::hash(&expected).as_bytes()
        );
        assert_eq!(
            overlay::changes_since(&directory, &expected_token).unwrap(),
            (expected_token, vec![])
        );
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
    }

    #[test]
    fn long_paths_keep_exact_history_without_exceeding_fjall_key_limit() {
        let (_temp, snapshot, directory, git) = fixture();
        let fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        let (before, _) = overlay::changes_since(&directory, "").unwrap();
        let h = fs.database.handles.as_ref().unwrap();
        let path = vec![255; u16::MAX as usize + 1];
        append_batch(h, std::slice::from_ref(&path));
        let (after, changed) = overlay::changes_since(&directory, &before).unwrap();
        assert_eq!(changed, vec![path]);
        assert_eq!(
            overlay::dirty_paths(&directory).unwrap(),
            vec![b"/".to_vec()]
        );
        append_batch(h, &[vec![255; MAX_PATH_BYTES + 1]]);
        assert_eq!(
            overlay::changes_since(&directory, &after).unwrap().1,
            vec![b"/".to_vec()]
        );
        let (before_empty, _) = overlay::changes_since(&directory, "").unwrap();
        append_batch(h, &[vec![]]);
        assert_eq!(
            overlay::changes_since(&directory, &before_empty).unwrap().1,
            vec![b"/".to_vec()]
        );
    }

    #[test]
    fn byte_limit_evicts_whole_generation_and_invalid_counters_fail_closed() {
        let (_temp, snapshot, directory, git) = fixture();
        let fs = Overlay::open(&directory, &snapshot, &git).unwrap();
        let h = fs.database.handles.as_ref().unwrap();
        for generation in 0..5 {
            let paths = (0..1024)
                .map(|n| {
                    let mut path = format!("{generation}-{n:04}-").into_bytes();
                    path.resize(1000, 255);
                    path
                })
                .collect::<Vec<_>>();
            append_batch(h, &paths);
        }
        let state = stats(h).unwrap();
        assert_eq!(state.retained_floor, 1);
        assert_eq!(state.retained_records, 4096);
        assert!(state.retained_bytes <= MAX_RECORD_BYTES);
        // Numeric-byte JSON accounting is deliberately not UTF-8/string size.
        assert_eq!(
            wire_path(&[255, 0, 10]),
            serde_json::to_vec(&vec![255u8, 0, 10]).unwrap().len() + 1
        );
        let mut tx = h.db.write_tx();
        let mut invalid = bounds(&tx, h).unwrap();
        invalid.dirty_count = MAX_PATHS as u64 + 1;
        save(&mut tx, h, &invalid).unwrap();
        tx.commit().unwrap();
        assert!(overlay::changes_since(&directory, "").is_err());
        assert!(overlay::dirty_paths(&directory).is_err());
    }
}
