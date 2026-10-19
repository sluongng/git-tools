//! Durable Fjall overlay and a read-only journal service for Git subprocesses.
#[path = "overlay_journal.rs"]
mod journal;
#[path = "overlay_retirement.rs"]
mod retirement;
use super::{
    BaseKind, Entry, Kind, Payload, Snapshot,
    codec::{decode_exact, decode_for_open, encode},
    payload::{Files, Intent, Operation, failure},
};
use crate::storage::open_fjall;
use anyhow::{Context, Result, ensure};
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Clone)]
struct Handles {
    entries: SingleWriterTxKeyspace,
    meta: SingleWriterTxKeyspace,
    journal: SingleWriterTxKeyspace,
    journal_v3: SingleWriterTxKeyspace,
    dirty_paths: SingleWriterTxKeyspace,
    retired_payloads: SingleWriterTxKeyspace,
    db: Arc<SingleWriterTxDatabase>,
}
pub(super) struct Store {
    handles: Option<Handles>,
    service: Option<Service>,
    pub(super) reclaimed_payload_bytes: u64,
    pub(super) files: Files,
    poisoned: std::cell::Cell<bool>,
    pub(super) metadata_syncs: std::cell::Cell<u64>,
    pub(super) highwater: std::cell::Cell<u64>,
    retirements: Option<retirement::Retirements>,
}
fn number(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().context("invalid overlay integer")?,
    ))
}
fn encode_entry(entry: &Entry) -> Result<Vec<u8>> {
    let lengths = [
        75usize,
        entry.name.as_bytes().len(),
        entry.base_oid.as_ref().map_or(0, String::len),
        match &entry.payload {
            Payload::Inline(bytes) => bytes.len(),
            _ => 17,
        },
        super::timestamp(entry.mtime).len(),
        super::timestamp(entry.ctime).len(),
    ];
    let length = lengths.into_iter().try_fold(0usize, usize::checked_add);
    if length.and_then(|n| u32::try_from(n).ok()).is_none() {
        return Err(super::error(libc::EFBIG));
    }
    Ok(encode(entry))
}
impl Handles {
    fn open(directory: &Path, initialize: bool) -> Result<Self> {
        let db = open_fjall(&directory.join("overlay.fjall"))?;
        if !initialize {
            for name in ["entries", "meta", "journal"] {
                ensure!(db.keyspace_exists(name), "overlay keyspace missing");
            }
        }
        let entries = db.keyspace("entries", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        let journal = db.keyspace("journal", KeyspaceCreateOptions::default)?;
        let journal_v3 = db.keyspace("journal_v3", KeyspaceCreateOptions::default)?;
        let dirty_paths = db.keyspace("dirty_paths", KeyspaceCreateOptions::default)?;
        let retired_payloads = db.keyspace("retired_payloads", KeyspaceCreateOptions::default)?;
        Ok(Self {
            entries,
            meta,
            journal,
            journal_v3,
            dirty_paths,
            retired_payloads,
            db,
        })
    }
    fn query(&self, request: Query) -> Result<Response> {
        journal::query(self, request)
    }
}
impl Store {
    pub(super) fn open(
        directory: &Path,
        snapshot: &Snapshot,
        gitdir: &Path,
        mut visit: impl FnMut(&Entry) -> Result<()>,
    ) -> Result<Self> {
        let endpoint = Endpoint::new(directory)?;
        let mut lock = endpoint.lock()?;
        ensure!(
            try_lock(&mut lock)?,
            "Fjall overlay already has an active owner"
        );
        let handles = Handles::open(directory, true)?;
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        if let Some(commit) = tx.get(&handles.meta, b"commit")? {
            ensure!(
                &*commit == snapshot.commit.as_bytes(),
                "overlay belongs to a different pinned commit"
            );
        } else {
            ensure!(
                tx.iter(&handles.entries).next().is_none()
                    && tx.iter(&handles.journal).next().is_none(),
                "uninitialized overlay contains records"
            );
            for n in snapshot.nodes() {
                ensure!(
                    n.parent != 1 || !n.name.as_bytes().eq_ignore_ascii_case(b".git"),
                    "Git tree conflicts with protected .git entry"
                );
                let kind = match n.kind {
                    BaseKind::File => Kind::File,
                    BaseKind::Directory => Kind::Directory,
                    BaseKind::Symlink => Kind::Symlink,
                    BaseKind::Gitlink => Kind::Gitlink,
                };
                let mode = match kind {
                    Kind::Directory | Kind::Gitlink => 0o755,
                    Kind::Symlink => 0o777,
                    Kind::File => {
                        if n.mode & 0o111 != 0 {
                            0o755
                        } else {
                            0o644
                        }
                    }
                };
                let e = Entry {
                    inode: n.inode,
                    parent: n.parent,
                    name: n.name.clone(),
                    kind,
                    mode,
                    base_oid: n.oid.clone(),
                    payload: if n.oid.is_some() && !kind.is_dir() {
                        Payload::Base
                    } else {
                        Payload::None
                    },
                    mtime: snapshot.timestamp(),
                    ctime: snapshot.timestamp(),
                    linked: true,
                    control: false,
                };
                tx.insert(&handles.entries, e.inode.to_be_bytes(), encode_entry(&e)?);
            }
            let mut identity = [0u8; 16];
            File::open("/dev/urandom")?.read_exact(&mut identity)?;
            tx.insert(
                &handles.meta,
                b"identity",
                hex::encode(identity).into_bytes(),
            );
            tx.insert(&handles.meta, b"generation", 0u64.to_be_bytes());
            tx.insert(&handles.meta, b"commit", snapshot.commit.as_bytes());
        }
        // Finish initialization before maintenance. Fresh open holds the exclusive
        // owner lock, so no live FUSE/open-file references can reach these inodes.
        // Root is always decoded by the old binary; upgrading it first makes
        // mixed-format migration fail closed before any external-file reference.
        let root = tx
            .get(&handles.entries, 1u64.to_be_bytes())?
            .context("overlay root missing")?;
        tx.insert(
            &handles.entries,
            1u64.to_be_bytes(),
            encode_entry(&decode_exact(&root)?)?,
        );
        if let Some(format) = tx.get(&handles.meta, b"payload_format")? {
            ensure!(&*format == b"2", "unsupported overlay payload format");
        }
        tx.insert(&handles.meta, b"payload_format", b"2");
        tx.commit()?;
        let files = Files::open(directory)?;
        Self::recover(&handles, &files)?;
        journal::initialize(&handles)?;
        retirement::Retirements::drain(&handles, &files)?;
        let mut control_entry = None;
        let mut maximum_inode = 0;
        let mut reclaimed_payload_bytes = 0u64;
        let mut cursor: Option<Vec<u8>> = None;
        const MAINTENANCE_RECORDS: usize = 128;
        loop {
            let read = handles.db.read_tx();
            let lower = cursor.as_ref().map_or(std::ops::Bound::Unbounded, |key| {
                std::ops::Bound::Excluded(key.clone())
            });
            let mut maintenance = Vec::new();
            let mut discarded = Vec::new();
            let mut count = 0;
            for item in read
                .range(&handles.entries, (lower, std::ops::Bound::Unbounded))
                .take(MAINTENANCE_RECORDS)
            {
                let (key, value) = item.into_inner()?;
                let (mut e, reclaimed) = decode_for_open(&value)?;
                ensure!(number(&key)? == e.inode, "overlay entry key mismatch");
                if let Some(bytes) = reclaimed {
                    reclaimed_payload_bytes = reclaimed_payload_bytes
                        .checked_add(bytes)
                        .context("overlay reclaimed byte count overflow")?;
                    // A zero-length inline payload also needs its marker removed.
                    // Already-pruned records need no second durable write.
                    maintenance.push((key.to_vec(), encode_entry(&e)?));
                }
                if e.linked && !e.control && e.kind == Kind::File {
                    if let Payload::Inline(bytes) = &e.payload {
                        files.install(e.inode, bytes)?;
                        e.payload = Payload::File {
                            size: bytes.len() as u64,
                            generation: 0,
                        };
                        maintenance.push((key.to_vec(), encode_entry(&e)?));
                    } else if let Payload::File { size, .. } = e.payload {
                        files.file(e.inode, size)?;
                    }
                }
                if !e.linked && !e.control {
                    discarded.push(e.inode);
                }
                cursor = Some(key.to_vec());
                maximum_inode = maximum_inode.max(e.inode);
                if e.control {
                    ensure!(control_entry.is_none(), "multiple overlay control records");
                    control_entry = Some(e);
                } else {
                    visit(&e)?;
                }
                count += 1;
            }
            // Do not pin old LSM versions across maintenance commits.
            drop(read);
            if !maintenance.is_empty() {
                let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
                for (key, value) in maintenance {
                    let inode = number(&key)?;
                    let old = tx
                        .get(&handles.entries, &key)?
                        .context("maintenance inode missing")?;
                    if let Payload::File { size, .. } = decode_exact(&old)?.payload {
                        tx.insert(
                            &handles.retired_payloads,
                            inode.to_be_bytes(),
                            size.to_be_bytes(),
                        );
                    }
                    tx.insert(&handles.entries, key, value);
                }
                tx.commit()?;
            }
            // References were durably cleared before any physical deletion.
            // Preserve unknown files; only the retirement ledger authorizes unlink.
            let _ = discarded;
            retirement::Retirements::drain(&handles, &files)?;
            if count < MAINTENANCE_RECORDS {
                break;
            }
        }
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        let absolute = gitdir
            .canonicalize()
            .context("resolve worktree Git directory")?;
        ensure!(
            !absolute.as_os_str().as_bytes().contains(&b'\n'),
            "Git directory path contains newline"
        );
        let mut pointer = b"gitdir: ".to_vec();
        pointer.extend_from_slice(absolute.as_os_str().as_bytes());
        pointer.push(b'\n');
        let mut control = if let Some(control) = control_entry {
            control
        } else {
            Entry {
                inode: maximum_inode
                    .max(
                        tx.get(&handles.meta, b"allocation_highwater")?
                            .as_deref()
                            .map(number)
                            .transpose()?
                            .unwrap_or(0),
                    )
                    .checked_add(1)
                    .context("overlay inode overflow")?,
                parent: 1,
                name: OsString::from(".git"),
                kind: Kind::File,
                mode: 0o444,
                base_oid: None,
                payload: Payload::None,
                mtime: snapshot.timestamp(),
                ctime: snapshot.timestamp(),
                linked: true,
                control: true,
            }
        };
        control.payload = Payload::Inline(pointer);
        tx.insert(
            &handles.entries,
            control.inode.to_be_bytes(),
            encode_entry(&control)?,
        );
        maximum_inode = maximum_inode.max(control.inode);
        visit(&control)?;
        let highwater = maximum_inode.max(
            tx.get(&handles.meta, b"allocation_highwater")?
                .as_deref()
                .map(number)
                .transpose()?
                .unwrap_or(0),
        );
        tx.insert(
            &handles.meta,
            b"allocation_highwater",
            highwater.to_be_bytes(),
        );
        tx.commit()?;
        let retirements = retirement::Retirements::start(handles.clone(), files.worker_copy())?;
        // Keep the guard local until setup succeeds. An early service error
        // must stop workers/drop database handles before releasing ownership.
        let mut service = Service::start(endpoint, handles.clone())?;
        service._lock = Some(lock);
        Ok(Self {
            handles: Some(handles),
            service: Some(service),
            reclaimed_payload_bytes,
            files,
            poisoned: std::cell::Cell::new(false),
            metadata_syncs: std::cell::Cell::new(0),
            highwater: std::cell::Cell::new(highwater),
            retirements: Some(retirements),
        })
    }
    pub(super) fn read_entry(&self, inode: u64) -> Result<Option<Entry>> {
        self.check_healthy()?;
        let h = self.handles.as_ref().context("overlay store closed")?;
        h.db.read_tx()
            .get(&h.entries, inode.to_be_bytes())?
            .map(|bytes| decode_exact(&bytes))
            .transpose()
    }
    fn append_history(
        tx: &mut fjall::SingleWriterWriteTx,
        h: &Handles,
        paths: &[Vec<u8>],
    ) -> Result<u64> {
        if tx.get(&h.meta, b"journal_generation")?.is_some() {
            return journal::append(tx, h, paths);
        }
        // Legacy Stage B redo is recovered before bounded-history activation.
        let generation = journal::generation(tx, h)?
            .checked_add(1)
            .context("overlay generation overflow")?;
        for (ordinal, path) in paths
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
        {
            let mut key = [0; 16];
            key[..8].copy_from_slice(&generation.to_be_bytes());
            key[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
            tx.insert(&h.journal, key, path.as_slice());
        }
        tx.insert(&h.meta, b"generation", generation.to_be_bytes());
        Ok(generation)
    }
    pub(super) fn journal_stats(&self) -> Result<journal::JournalStats> {
        journal::stats(self.handles.as_ref().context("overlay closed")?)
    }
    pub(super) fn retirement_stats(&self) -> (u64, u64, u64, u64, u64) {
        self.retirements
            .as_ref()
            .map_or((0, 0, 0, 0, 0), retirement::Retirements::stats)
    }
    pub(super) fn drain_maintenance(&mut self) -> Result<()> {
        let result = self
            .retirements
            .as_ref()
            .map_or(Ok(()), retirement::Retirements::wait);
        if result.is_err()
            && let Some(retirements) = self.retirements.as_mut()
        {
            retirements.stop();
        }
        result
    }
    pub(super) fn retire(&self, entry: &Entry, remove: bool) -> Result<()> {
        self.check_healthy()?;
        let h = self.handles.as_ref().context("overlay closed")?;
        let bytes = match entry.payload {
            Payload::File { size, .. } => Some(size),
            _ => None,
        };
        let admission = bytes
            .map(|bytes| {
                self.retirements
                    .as_ref()
                    .context("retirement worker missing")?
                    .reserve(bytes)
            })
            .transpose()?;
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        if remove {
            tx.remove(&h.entries, entry.inode.to_be_bytes());
        } else {
            let mut tombstone = entry.clone();
            tombstone.payload = Payload::None;
            tx.insert(
                &h.entries,
                entry.inode.to_be_bytes(),
                encode_entry(&tombstone)?,
            );
        }
        if let Some(bytes) = bytes {
            tx.insert(
                &h.retired_payloads,
                entry.inode.to_be_bytes(),
                bytes.to_be_bytes(),
            );
        }
        self.poisoned.set(true);
        let committed = (|| -> Result<()> {
            failure("before_retirement_commit")?;
            tx.commit()?;
            failure("after_retirement_commit")?;
            Ok(())
        })();
        if let Err(error) = committed {
            if let Some(admission) = admission {
                self.retirements.as_ref().unwrap().uncertain(admission);
            }
            return Err(error);
        }
        self.poisoned.set(false);
        if let Some(admission) = admission {
            self.retirements
                .as_ref()
                .unwrap()
                .submit(admission, h, &self.files, entry.inode);
        }
        Ok(())
    }
    pub(super) fn check_healthy(&self) -> Result<()> {
        ensure!(
            self.retirements
                .as_ref()
                .is_none_or(retirement::Retirements::healthy),
            "payload retirement failed; reopen for recovery"
        );
        ensure!(
            !self.poisoned.get(),
            "overlay requires recovery after uncertain payload mutation"
        );
        Ok(())
    }
    fn publish(handles: &Handles, intent: &Intent) -> Result<()> {
        let entry = decode_exact(&intent.after_entry)?;
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        let generation = journal::generation(&tx, handles)?;
        ensure!(
            generation == intent.before_generation,
            "redo journal generation conflict"
        );
        let current = tx
            .get(&handles.entries, entry.inode.to_be_bytes())?
            .context("redo inode missing")?;
        ensure!(
            encode(&decode_exact(&current)?) == intent.before_entry,
            "redo entry generation conflict"
        );
        tx.insert(
            &handles.entries,
            entry.inode.to_be_bytes(),
            intent.after_entry.as_slice(),
        );
        Self::append_history(&mut tx, handles, &intent.paths)?;
        tx.remove(&handles.meta, b"payload_intent");
        failure("before_final_commit")?;
        tx.commit()?;
        failure("after_final_commit")?;
        Ok(())
    }
    fn validate_intent(intent: &Intent) -> Result<(Entry, u64)> {
        let before = decode_exact(&intent.before_entry)?;
        let after = decode_exact(&intent.after_entry)?;
        ensure!(
            before.inode == after.inode
                && before.parent == after.parent
                && before.name == after.name
                && before.linked == after.linked
                && before.kind == after.kind
                && before.base_oid == after.base_oid
                && !before.control
                && !before.kind.is_dir(),
            "invalid payload intent namespace"
        );
        if let Payload::File { size, .. } = before.payload {
            ensure!(
                size == intent.before_size,
                "redo pre-backing size conflicts with file metadata"
            );
        }
        let pre_generation = match before.payload {
            Payload::File { generation, .. } => generation,
            _ => 0,
        };
        let Payload::File { size, generation } = after.payload else {
            anyhow::bail!("redo target is not file-backed")
        };
        ensure!(
            pre_generation.checked_add(1) == Some(generation),
            "invalid redo content generation"
        );
        if let Operation::Write {
            offset,
            length,
            ref hash,
        } = intent.operation
        {
            ensure!(
                size >= intent.before_size
                    && length > 0
                    && offset.checked_add(length).is_some_and(|end| end <= size)
                    && hash.len() == 64
                    && hex::decode(hash)?.len() == 32,
                "invalid redo write bounds"
            );
        }
        ensure!(size <= i64::MAX as u64, "redo file too large");
        Ok((after, size))
    }
    fn recover(handles: &Handles, files: &Files) -> Result<()> {
        let tx = handles.db.read_tx();
        let raw = tx.get(&handles.meta, b"payload_intent")?;
        if let Some(raw) = raw {
            let intent: Intent =
                serde_json::from_slice(&raw).context("invalid payload redo intent")?;
            let (entry, size) = Self::validate_intent(&intent)?;
            let generation = journal::generation(&tx, handles)?;
            let current = tx
                .get(&handles.entries, entry.inode.to_be_bytes())?
                .context("redo inode missing")?;
            let current = encode(&decode_exact(&current)?);
            // Publication atomically replaces the entry/generation and removes
            // the intent. A pending intent can therefore only name pre-state;
            // accepting post-state would bless corruption and discard evidence.
            ensure!(
                generation == intent.before_generation,
                "redo journal generation conflict"
            );
            ensure!(current == intent.before_entry, "redo pre-state conflict");
            drop(tx);
            files.apply(entry.inode, intent.before_size, size, &intent.operation)?;
            Self::publish(handles, &intent)?;
            files
                .counters
                .recovered
                .set(files.counters.recovered.get() + 1);
        } else {
            drop(tx);
        }
        // Only this fixed scratch name is disposable after tracing the intent.
        files.retire_redo()
    }
    pub(super) fn copy_up(
        &self,
        before: &Entry,
        after: &Entry,
        paths: &[Vec<u8>],
        old: &[u8],
        write: Option<(u64, &[u8])>,
    ) -> Result<()> {
        self.check_healthy()?;
        ensure!(
            self.read_entry(before.inode)?.as_ref().map(encode).as_ref() == Some(&encode(before)),
            "copy-up entry generation conflict"
        );
        ensure!(
            !matches!(before.payload, Payload::File { .. }),
            "copy-up would overwrite referenced payload"
        );
        let Payload::File {
            size,
            generation: 1,
        } = after.payload
        else {
            anyhow::bail!("invalid initial copy-up generation")
        };
        self.files
            .prepare_unpublished(before.inode, old, size, write)?;
        // Until this durable publication, the old Base/Inline record remains
        // authoritative. Failure before it needs no redo and loses no edits.
        failure("before_copyup_commit")?;
        self.commit(std::slice::from_ref(after), paths)
    }
    pub(super) fn mutate(
        &self,
        before: &Entry,
        after: &Entry,
        paths: &[Vec<u8>],
        write: Option<(u64, &[u8])>,
    ) -> Result<()> {
        self.check_healthy()?;
        let h = self.handles.as_ref().context("overlay closed")?;
        let tx = h.db.read_tx();
        ensure!(
            tx.get(&h.meta, b"payload_intent")?.is_none(),
            "payload intent already pending"
        );
        let current = tx
            .get(&h.entries, before.inode.to_be_bytes())?
            .context("mutation inode missing")?;
        ensure!(
            encode(&decode_exact(&current)?) == encode(before),
            "mutation entry generation conflict"
        );
        let before_generation = journal::generation(&tx, h)?;
        before_generation
            .checked_add(1)
            .context("overlay generation overflow")?;
        drop(tx);
        let operation = if let Some((offset, bytes)) = write {
            self.files.stage(bytes)?;
            Operation::Write {
                offset,
                length: bytes.len() as u64,
                hash: blake3::hash(bytes).to_hex().to_string(),
            }
        } else {
            Operation::Truncate
        };
        let intent = Intent {
            before_generation,
            before_size: self.files.len(before.inode)?,
            before_entry: encode_entry(before)?,
            after_entry: encode_entry(after)?,
            paths: paths.to_vec(),
            operation,
        };
        let (entry, size) = Self::validate_intent(&intent)?;
        let encoded = serde_json::to_vec(&intent)?;
        // A failed durable commit may nevertheless have reached storage. No
        // content or mutation can proceed until an exclusive recovery open.
        self.poisoned.set(true);
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&h.meta, b"payload_intent", encoded);
        self.metadata_syncs.set(self.metadata_syncs.get() + 1);
        tx.commit()?;
        failure("after_intent")?;
        self.files
            .apply(entry.inode, intent.before_size, size, &intent.operation)?;
        self.metadata_syncs.set(self.metadata_syncs.get() + 1);
        Self::publish(h, &intent)?;
        self.files.retire_redo()?;
        self.poisoned.set(false);
        Ok(())
    }
    pub(super) fn commit(&self, changed: &[Entry], paths: &[Vec<u8>]) -> Result<()> {
        self.check_healthy()?;
        let h = self.handles.as_ref().context("overlay closed")?;
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        for entry in changed {
            tx.insert(&h.entries, entry.inode.to_be_bytes(), encode_entry(entry)?);
        }
        Self::append_history(&mut tx, h, paths)?;
        let highwater = changed
            .iter()
            .map(|e| e.inode)
            .max()
            .unwrap_or(0)
            .max(self.highwater.get());
        tx.insert(&h.meta, b"allocation_highwater", highwater.to_be_bytes());
        self.poisoned.set(true);
        failure("before_namespace_commit")?;
        self.metadata_syncs.set(self.metadata_syncs.get() + 1);
        tx.commit()?;
        failure("after_namespace_commit")?;
        self.highwater.set(highwater);
        self.poisoned.set(false);
        Ok(())
    }
    pub(super) fn sync(&self) -> Result<()> {
        self.check_healthy()?;
        self.handles
            .as_ref()
            .context("overlay closed")?
            .db
            .persist(PersistMode::SyncAll)?;
        Ok(())
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        if let Some(mut service) = self.service.take() {
            service.stop();
            if let Some(retirements) = self.retirements.as_mut() {
                retirements.stop();
            }
            // The service lock stays held until both the serving thread and all
            // database/keyspace handles are gone, so offline readers cannot race.
            drop(self.handles.take());
            drop(service);
        }
    }
}

const MAX_REQUEST: usize = 4096;
const MAX_RESPONSE: usize = 64 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    token: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Response {
    token: String,
    paths: Vec<Vec<u8>>,
    error: Option<String>,
}
struct Endpoint {
    socket: PathBuf,
    lock: PathBuf,
}
impl Endpoint {
    fn new(directory: &Path) -> Result<Self> {
        let uid = unsafe { libc::geteuid() };
        let runtime = PathBuf::from(format!("/tmp/git-vfs-fjall-{uid}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&runtime) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let metadata = std::fs::symlink_metadata(&runtime)?;
        ensure!(
            metadata.is_dir()
                && metadata.uid() == uid
                && metadata.permissions().mode() & 0o777 == 0o700,
            "unsafe Fjall journal runtime directory"
        );
        let absolute = directory.canonicalize()?;
        let hash = hex::encode(Sha1::digest(absolute.as_os_str().as_bytes()));
        Ok(Self {
            socket: runtime.join(format!("{hash}.sock")),
            lock: runtime.join(format!("{hash}.lock")),
        })
    }
    fn lock(&self) -> Result<OwnerLock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&self.lock)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "unsafe Fjall journal owner lock"
        );
        Ok(OwnerLock {
            file,
            acquired: false,
        })
    }
}
struct OwnerLock {
    file: File,
    acquired: bool,
}
impl Drop for OwnerLock {
    fn drop(&mut self) {
        if self.acquired {
            // CLOEXEC does not close a descriptor between fork and exec. A
            // subprocess may still share this open-file description after our
            // File closes, so release ownership explicitly before closing it.
            loop {
                if unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) } == 0 {
                    break;
                }
                if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                    break;
                }
            }
        }
    }
}
fn try_lock(lock: &mut OwnerLock) -> Result<bool> {
    loop {
        if unsafe { libc::flock(lock.file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            lock.acquired = true;
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EWOULDBLOCK) => return Ok(false),
            _ => return Err(error.into()),
        }
    }
}
#[cfg(target_os = "macos")]
fn check_peer(stream: &UnixStream) -> Result<()> {
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: The connected socket is live and both output pointers are valid.
    if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    ensure!(
        uid == unsafe { libc::geteuid() },
        "journal peer has a different user identity"
    );
    Ok(())
}
#[cfg(not(target_os = "macos"))]
fn check_peer(stream: &UnixStream) -> Result<()> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: The output buffer and its length match SO_PEERCRED's ucred ABI.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut size,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    ensure!(
        size as usize == std::mem::size_of::<libc::ucred>()
            && credentials.uid == unsafe { libc::geteuid() },
        "journal peer has a different user identity"
    );
    Ok(())
}
fn configure(stream: &UnixStream) -> Result<()> {
    // macOS accepts inherit the listener's nonblocking flag. Framed journal
    // reads and writes must instead wait for their configured timeouts.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    check_peer(stream)
}
fn read_frame(stream: &mut UnixStream, maximum: usize) -> Result<Vec<u8>> {
    let mut length = [0u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    ensure!(length <= maximum, "journal message exceeds protocol limit");
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body)?;
    Ok(body)
}
fn write_frame(stream: &mut UnixStream, body: &[u8], maximum: usize) -> Result<()> {
    ensure!(
        body.len() <= maximum,
        "journal message exceeds protocol limit"
    );
    stream.write_all(&u32::try_from(body.len())?.to_be_bytes())?;
    stream.write_all(body)?;
    Ok(())
}
struct Service {
    thread: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    wake: UnixStream,
    endpoint: Endpoint,
    _lock: Option<OwnerLock>,
}
impl Service {
    fn start(endpoint: Endpoint, handles: Handles) -> Result<Self> {
        match std::fs::remove_file(&endpoint.socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let listener = UnixListener::bind(&endpoint.socket)?;
        std::fs::set_permissions(&endpoint.socket, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let (wake, wake_read) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::Builder::new()
            .name("git-vfs-journal".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    let mut fds = [
                        libc::pollfd {
                            fd: listener.as_raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        },
                        libc::pollfd {
                            fd: wake_read.as_raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        },
                    ];
                    // SAFETY: Both descriptors remain owned by this thread and
                    // the array contains exactly the advertised pollfd count.
                    let ready =
                        unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
                    if ready < 0 {
                        if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                        {
                            continue;
                        }
                        break;
                    }
                    if fds[1].revents != 0 || thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let (mut stream, _) = match listener.accept() {
                        Ok(stream) => stream,
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => break,
                    };
                    if thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    if configure(&stream).is_err() {
                        continue;
                    }
                    let result = (|| -> Result<Response> {
                        let body = read_frame(&mut stream, MAX_REQUEST)?;
                        let request: Query = serde_json::from_slice(&body)?;
                        handles.query(request)
                    })();
                    let response = result.unwrap_or_else(|_| Response {
                        token: String::new(),
                        paths: Vec::new(),
                        error: Some("Fjall journal query failed".into()),
                    });
                    if let Ok(body) = serde_json::to_vec(&response) {
                        let _ = write_frame(&mut stream, &body, MAX_RESPONSE);
                    }
                }
            });
        match thread {
            Ok(thread) => Ok(Self {
                thread: Some(thread),
                stop,
                wake,
                endpoint,
                _lock: None,
            }),
            Err(e) => {
                let _ = std::fs::remove_file(&endpoint.socket);
                Err(e.into())
            }
        }
    }
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            // A private wakeup descriptor still works if the socket pathname
            // was removed, unlike connecting to that pathname during shutdown.
            let _ = self.wake.write(&[1]);
            let _ = thread.join();
        }
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.endpoint.socket);
    }
}
fn query(directory: &Path, request: Query) -> Result<Response> {
    let endpoint = Endpoint::new(directory)?;
    match UnixStream::connect(&endpoint.socket) {
        Ok(mut stream) => {
            configure(&stream)?;
            write_frame(&mut stream, &serde_json::to_vec(&request)?, MAX_REQUEST)?;
            let response: Response =
                serde_json::from_slice(&read_frame(&mut stream, MAX_RESPONSE)?)?;
            ensure!(
                response.error.is_none(),
                "Fjall journal owner rejected query"
            );
            Ok(response)
        }
        Err(connect_error) => {
            let mut lock = endpoint.lock()?;
            ensure!(
                try_lock(&mut lock)?,
                "Fjall journal owner is active but unavailable: {connect_error}"
            );
            let handles = Handles::open(directory, false).context(
                "open offline Fjall journal; an active database owner may still be closing",
            )?;
            let response = handles.query(request)?;
            drop(handles);
            drop(lock);
            Ok(response)
        }
    }
}
pub(super) fn changes_since(directory: &Path, token: &str) -> Result<(String, Vec<Vec<u8>>)> {
    let response = query(
        directory,
        Query {
            token: Some(token.to_owned()),
        },
    )?;
    Ok((response.token, response.paths))
}
pub(super) fn dirty_paths(directory: &Path) -> Result<Vec<Vec<u8>>> {
    Ok(query(directory, Query { token: None })?.paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{GitRepo, SizeFallback};
    use crate::overlay::{Overlay, payload::fail_next};
    use std::ffi::OsStr;
    use std::process::Command;

    #[test]
    fn owner_lock_releases_while_forked_child_holds_inherited_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = Endpoint::new(directory.path()).unwrap();
        let mut owner = endpoint.lock().unwrap();
        assert!(try_lock(&mut owner).unwrap());
        let mut denied = endpoint.lock().unwrap();
        assert!(!try_lock(&mut denied).unwrap());
        drop(denied);
        let mut still_denied = endpoint.lock().unwrap();
        assert!(
            !try_lock(&mut still_denied).unwrap(),
            "dropping an unsuccessful contender must not unlock the owner"
        );
        drop(still_denied);

        let (mut parent, child) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        parent
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        child
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let parent_fd = parent.as_raw_fd();
        let child_fd = child.as_raw_fd();
        // Other tests may have threads alive. Between fork and _exit the child
        // uses only async-signal-safe syscalls, with no Rust destructors/locks.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", std::io::Error::last_os_error());
        if pid == 0 {
            unsafe {
                libc::close(parent_fd);
                let ready = [1u8];
                if libc::write(child_fd, ready.as_ptr().cast(), 1) != 1 {
                    libc::_exit(2);
                }
                let mut release = [0u8];
                let received = libc::read(child_fd, release.as_mut_ptr().cast(), 1);
                libc::close(child_fd);
                libc::_exit(if received == 1 && release[0] == 1 {
                    0
                } else {
                    3
                });
            }
        }
        drop(child);
        let mut ready = [0u8];
        let barrier = parent.read_exact(&mut ready);
        // The child is still paused with the inherited owner descriptor open.
        // Merely closing File here would leave flock held until child exit.
        drop(owner);
        let reacquired = endpoint.lock().and_then(|mut lock| try_lock(&mut lock));
        let released = parent.write_all(&[1]);
        drop(parent);
        let mut status = 0;
        let reaped = loop {
            let result = unsafe { libc::waitpid(pid, &mut status, 0) };
            if result >= 0 {
                break result;
            }
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break result;
            }
        };
        // Assert only after releasing and reaping the child, including failure
        // paths, so a failed lock assertion cannot leak a blocked subprocess.
        assert!(barrier.is_ok(), "child readiness: {barrier:?}");
        assert_eq!(ready, [1]);
        assert!(released.is_ok(), "child release: {released:?}");
        assert_eq!(reaped, pid);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);
        assert!(
            reacquired.unwrap(),
            "inherited descriptor retained owner flock"
        );
    }

    fn fixture() -> (tempfile::TempDir, GitRepo, Snapshot, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let gitdir = temp.path().join("repo");
        std::fs::create_dir(&gitdir).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Fixture"],
            vec!["config", "user.email", "fixture@example.test"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&gitdir)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        std::fs::write(gitdir.join("base"), b"base bytes").unwrap();
        std::fs::create_dir(gitdir.join("base-dir")).unwrap();
        std::fs::write(gitdir.join("base-dir/child"), b"child").unwrap();
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&gitdir)
                .args(["add", "."])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&gitdir)
                .args(["-c", "commit.gpgsign=false", "commit", "-qm", "Fixture"])
                .status()
                .unwrap()
                .success()
        );
        let bare = temp.path().join("bare.git");
        assert!(
            Command::new("git")
                .args(["clone", "--bare", "--quiet"])
                .arg(&gitdir)
                .arg(&bare)
                .status()
                .unwrap()
                .success()
        );
        let gitdir = bare;
        let repo = GitRepo::open(&gitdir, "git", "origin", SizeFallback::Error).unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        let overlay = temp.path().join("overlay");
        (temp, repo, snapshot, overlay, gitdir)
    }

    #[test]
    fn bounded_metadata_reload_preserves_namespace_open_leases_and_remount() {
        use crate::overlay::{MetadataCacheConfig, PinKind};
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let limits = MetadataCacheConfig {
            entries: 2,
            bytes: 4096,
        };
        let mut overlay =
            Overlay::open_with_metadata_cache(&directory, &snapshot, &gitdir, limits).unwrap();
        let base = overlay.lookup(1, OsStr::new("base")).unwrap();
        let held = overlay.entry(base).unwrap();
        let original_name = held.name.clone();
        let parent = overlay
            .create(1, OsStr::new("parent"), Kind::Directory, 0o755, None)
            .unwrap();
        let child = overlay
            .create(
                parent,
                OsStr::new("child"),
                Kind::File,
                0o644,
                Some(b"original".to_vec()),
            )
            .unwrap();
        for index in 0..20 {
            let name = OsString::from(format!("file-{index}"));
            let inode = overlay
                .create(parent, &name, Kind::File, 0o644, None)
                .unwrap();
            assert_eq!(overlay.entry(inode).unwrap().name, name);
        }
        let before_reload = overlay.stats();
        assert!(before_reload.metadata_cache_evictions > 0);
        assert!(before_reload.metadata_resident_entries <= 2);
        assert!(before_reload.metadata_resident_estimated_bytes <= 4096);
        assert!(before_reload.entry_count > before_reload.metadata_resident_entries);
        assert_eq!(
            held.name, original_name,
            "eviction must not invalidate active guards"
        );
        assert_eq!(overlay.entry(base).unwrap().name, original_name);
        assert!(overlay.stats().metadata_cache_misses > before_reload.metadata_cache_misses);
        overlay
            .rename(1, OsStr::new("parent"), 1, OsStr::new("renamed"), 0)
            .unwrap();
        assert_eq!(overlay.lookup(1, OsStr::new("renamed")).unwrap(), parent);
        assert_eq!(overlay.lookup(parent, OsStr::new("child")).unwrap(), child);
        // Reload an evicted mutable record, then publish its new generation.
        overlay.write(child, 0, b"changed!", &mut repo).unwrap();
        assert_eq!(overlay.read(child, &mut repo).unwrap(), b"changed!");
        let lease = overlay.pin(vec![child], PinKind::Open).unwrap();
        overlay.unlink(parent, OsStr::new("child"), false).unwrap();
        // A linked record scan continually evicts the open-but-unlinked record.
        for inode in overlay.children(parent).unwrap() {
            overlay.entry(inode).unwrap();
        }
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.read(child, &mut repo).unwrap(), b"changed!");
        drop(lease);
        overlay.reclaim_unreferenced().unwrap();
        overlay.drain_maintenance().unwrap();
        assert!(overlay.entry(child).is_err());
        assert_eq!(overlay.stats().online_reclaimed_payload_bytes, 8);
        assert_eq!(overlay.stats().dead_payload_bytes, 0);
        let logical_entries = overlay.stats().entry_count;
        drop(held);
        drop(overlay);
        let mut overlay =
            Overlay::open_with_metadata_cache(&directory, &snapshot, &gitdir, limits).unwrap();
        assert_eq!(overlay.stats().entry_count, logical_entries);
        assert_eq!(overlay.lookup(1, OsStr::new("renamed")).unwrap(), parent);
        assert!(overlay.lookup(parent, OsStr::new("child")).is_err());
        assert_eq!(overlay.read(base, &mut repo).unwrap(), b"base bytes");
        assert!(overlay.stats().metadata_resident_entries <= 2);
        let next = overlay
            .create(parent, OsStr::new("next"), Kind::File, 0o644, None)
            .unwrap();
        assert!(next > child, "reclaimed identity must not be reused");
    }

    #[test]
    fn disabled_or_undersized_metadata_cache_keeps_durable_entries_accessible() {
        use crate::overlay::MetadataCacheConfig;
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        for limits in [
            MetadataCacheConfig {
                entries: 0,
                bytes: 4096,
            },
            MetadataCacheConfig {
                entries: 32,
                bytes: 1,
            },
        ] {
            let mut overlay =
                Overlay::open_with_metadata_cache(&directory, &snapshot, &gitdir, limits).unwrap();
            let base = overlay.lookup(1, OsStr::new("base")).unwrap();
            assert_eq!(overlay.read(base, &mut repo).unwrap(), b"base bytes");
            overlay
                .setattr(base, Some(0o600), None, None, &mut repo)
                .unwrap();
            assert_eq!(overlay.entry(base).unwrap().mode, 0o600);
            assert_eq!(overlay.entry(1).unwrap().inode, 1);
            let control = overlay.lookup(1, OsStr::new(".git")).unwrap();
            assert!(overlay.entry(control).unwrap().control);
            assert_eq!(overlay.stats().metadata_resident_entries, 0);
            assert_eq!(overlay.stats().metadata_resident_estimated_bytes, 0);
            assert!(overlay.stats().metadata_cache_misses > 0);
        }
    }

    fn legacy_record(entry: &Entry) -> Vec<u8> {
        fn bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
            out.extend_from_slice(&value.map_or(u64::MAX, |v| v.len() as u64).to_be_bytes());
            if let Some(value) = value {
                out.extend_from_slice(value);
            }
        }
        let mut out = vec![1];
        for n in [
            entry.inode,
            entry.parent,
            entry.kind.code() as u64,
            entry.mode as u64,
        ] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.push(u8::from(entry.linked));
        out.push(u8::from(entry.control));
        bytes(&mut out, Some(entry.name.as_bytes()));
        bytes(&mut out, entry.base_oid.as_deref().map(str::as_bytes));
        bytes(
            &mut out,
            match &entry.payload {
                Payload::Inline(data) => Some(data),
                _ => None,
            },
        );
        bytes(
            &mut out,
            Some(crate::overlay::timestamp(entry.mtime).as_bytes()),
        );
        bytes(
            &mut out,
            Some(crate::overlay::timestamp(entry.ctime).as_bytes()),
        );
        out
    }

    #[test]
    fn interrupted_legacy_migration_preserves_bytes_journal_and_downgrade_guard() {
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let ino = overlay
            .create(
                1,
                OsStr::new("legacy-edit"),
                Kind::File,
                0o600,
                Some(vec![4; 128 * 1024]),
            )
            .unwrap();
        let mut entry = overlay.entry(ino).unwrap().clone();
        entry.payload = Payload::Inline(vec![4; 128 * 1024]);
        let root = overlay.entry(1).unwrap().clone();
        let (token, _) = super::changes_since(&directory, "").unwrap();
        drop(overlay);
        let handles = Handles::open(&directory, false).unwrap();
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&handles.entries, ino.to_be_bytes(), legacy_record(&entry));
        tx.insert(&handles.entries, 1u64.to_be_bytes(), legacy_record(&root));
        tx.remove(&handles.meta, b"payload_format");
        let legacy_generation = journal::generation(&tx, &handles).unwrap();
        tx.insert(
            &handles.meta,
            b"generation",
            legacy_generation.to_be_bytes(),
        );
        tx.remove(&handles.meta, b"journal_generation");
        tx.remove(&handles.meta, b"journal_bounds");
        let old: Vec<_> = tx
            .iter(&handles.journal_v3)
            .map(|item| item.into_inner().unwrap())
            .collect();
        for (key, value) in old {
            tx.insert(&handles.journal, &*key, &*value);
            tx.remove(&handles.journal_v3, &*key);
        }
        let dirty: Vec<_> = tx
            .iter(&handles.dirty_paths)
            .map(|item| item.into_inner().unwrap().0)
            .collect();
        for key in dirty {
            tx.remove(&handles.dirty_paths, key);
        }

        tx.commit().unwrap();
        drop(handles);
        fail_next("after_install_sync");
        assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
        let handles = Handles::open(&directory, false).unwrap();
        let tx = handles.db.read_tx();
        // The old decoder rejects this sentinel before it could serve or commit.
        assert_eq!(
            tx.get(&handles.entries, 1u64.to_be_bytes())
                .unwrap()
                .unwrap()[0],
            2
        );
        assert_eq!(
            tx.get(&handles.entries, ino.to_be_bytes())
                .unwrap()
                .unwrap()[0],
            1
        );
        drop(tx);
        drop(handles);
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        assert!(matches!(
            overlay.entry(ino).unwrap().payload,
            Payload::File { generation: 0, .. }
        ));
        assert_eq!(overlay.read(ino, &mut repo).unwrap(), vec![4; 128 * 1024]);
        assert_eq!(
            super::changes_since(&directory, &token).unwrap(),
            (token, vec![])
        );
        drop(overlay);
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        assert_eq!(overlay.stats().payload_copy_bytes, 0);
        assert_eq!(overlay.read_range(ino, 0, 1, &mut repo).unwrap(), [4]);
    }

    #[test]
    fn online_collection_respects_lookup_open_snapshot_and_async_pins() {
        use crate::overlay::PinKind;
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let inode = overlay
            .create(
                1,
                OsStr::new("temp"),
                Kind::File,
                0o600,
                Some(vec![5; 128 * 1024]),
            )
            .unwrap();
        overlay.record_lookup(inode).unwrap();
        let open = overlay.pin(vec![inode], PinKind::Open).unwrap();
        let snapshot_pin = overlay.pin(vec![1, inode], PinKind::Snapshot).unwrap();
        let asynchronous = overlay.pin(vec![inode], PinKind::Async).unwrap();
        overlay.unlink(1, OsStr::new("temp"), false).unwrap();
        let (token, _) = super::changes_since(&directory, "").unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.read_range(inode, 0, 1, &mut repo).unwrap(), [5]);
        overlay.forget(inode, 1);
        drop(open);
        overlay.reclaim_unreferenced().unwrap();
        assert!(overlay.entry(inode).is_ok());
        drop(snapshot_pin);
        overlay.reclaim_unreferenced().unwrap();
        assert!(overlay.entry(inode).is_ok());
        drop(asynchronous);
        overlay.reclaim_unreferenced().unwrap();
        overlay.drain_maintenance().unwrap();
        assert!(overlay.entry(inode).is_err());
        assert_eq!(overlay.stats().online_reclaimed_payload_bytes, 128 * 1024);
        assert_eq!(overlay.stats().reclaimed_entries, 1);
        assert_eq!(overlay.stats().retirement_pending_items, 0);
        assert_eq!(overlay.stats().retirement_pending_bytes, 0);
        assert_eq!(
            super::changes_since(&directory, &token).unwrap(),
            (token, vec![])
        );
        let base_dir = overlay.lookup(1, OsStr::new("base-dir")).unwrap();
        // Compatibility boundary: origin cannot depend on an optional tree OID.
        let mut base_entry = overlay.entry(base_dir).unwrap().clone();
        base_entry.base_oid = None;
        overlay.commit(vec![base_entry], vec![]).unwrap();
        assert!(overlay.entry(base_dir).unwrap().base_oid.is_none());
        overlay
            .unlink(base_dir, OsStr::new("child"), false)
            .unwrap();
        overlay.unlink(1, OsStr::new("base-dir"), true).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert!(
            overlay.entry(base_dir).is_ok(),
            "base directory origin comes from snapshot, not base_oid"
        );
    }

    #[test]
    fn retained_children_and_quarantined_lookups_keep_dead_metadata_safe() {
        use crate::overlay::PinKind;
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let parent = overlay
            .create(1, OsStr::new("parent"), Kind::Directory, 0o700, None)
            .unwrap();
        let child = overlay
            .create(
                parent,
                OsStr::new("child"),
                Kind::File,
                0o600,
                Some(vec![3; 16]),
            )
            .unwrap();
        let pin = overlay.pin(vec![child], PinKind::Open).unwrap();
        overlay.unlink(parent, OsStr::new("child"), false).unwrap();
        overlay.unlink(1, OsStr::new("parent"), true).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.entry(child).unwrap().parent, parent);
        assert!(overlay.entry(parent).is_ok());
        assert_eq!(overlay.read_range(child, 0, 1, &mut repo).unwrap(), [3]);
        drop(pin);
        overlay.reclaim_unreferenced().unwrap();
        overlay.drain_maintenance().unwrap();
        assert!(overlay.entry(child).is_err());
        assert!(overlay.entry(parent).is_err());
        let quarantined = overlay
            .create(
                1,
                OsStr::new("quarantined"),
                Kind::File,
                0o600,
                Some(vec![7; 8]),
            )
            .unwrap();
        overlay.record_lookup(quarantined).unwrap();
        overlay.forget(quarantined, 2);
        overlay.forget(u64::MAX, 99); // Unknown forget is harmless; excess known count quarantines.
        overlay.unlink(1, OsStr::new("quarantined"), false).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.stats().lookup_accounting_errors, 1);
        assert_eq!(
            overlay.read_range(quarantined, 0, 1, &mut repo).unwrap(),
            [7]
        );
        drop(overlay);
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        assert!(overlay.entry(quarantined).is_err());
        let next = overlay
            .create(1, OsStr::new("next"), Kind::File, 0o600, None)
            .unwrap();
        assert!(
            next > quarantined && next > child,
            "persistent highwater survives collecting highest inode"
        );
    }

    #[test]
    fn retirement_capacity_falls_back_synchronously_and_failed_ledger_recovers() {
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let pause = overlay.database.retirements.as_ref().unwrap().pause();
        for n in 0..256 {
            let name = OsString::from(format!("temp-{n}"));
            overlay
                .create(1, &name, Kind::File, 0o600, Some(vec![1; 8]))
                .unwrap();
            overlay.unlink(1, &name, false).unwrap();
            overlay.reclaim_unreferenced().unwrap();
        }
        assert_eq!(overlay.stats().retirement_pending_items, 256);
        assert_eq!(overlay.stats().retirement_pending_bytes, 256 * 8);
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(pause);
        });
        let inode = overlay
            .create(
                1,
                OsStr::new("overflow"),
                Kind::File,
                0o600,
                Some(vec![1; 8]),
            )
            .unwrap();
        overlay.unlink(1, OsStr::new("overflow"), false).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        release.join().unwrap();
        overlay.drain_maintenance().unwrap();
        assert!(overlay.entry(inode).is_err());
        assert_eq!(overlay.stats().retirement_pending_items, 0);
        assert_eq!(overlay.stats().retirement_sync_fallbacks, 1);
        // Byte capacity counts queued and in-flight records, not just messages.
        let pause = overlay.database.retirements.as_ref().unwrap().pause();
        for name in ["bytes-a", "bytes-b"] {
            overlay
                .create(
                    1,
                    OsStr::new(name),
                    Kind::File,
                    0o600,
                    Some(vec![2; 16 * 1024 * 1024]),
                )
                .unwrap();
            overlay.unlink(1, OsStr::new(name), false).unwrap();
            overlay.reclaim_unreferenced().unwrap();
        }
        assert_eq!(overlay.stats().retirement_pending_items, 2);
        assert_eq!(overlay.stats().retirement_pending_bytes, 32 * 1024 * 1024);
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(pause);
        });
        overlay
            .create(
                1,
                OsStr::new("byte-overflow"),
                Kind::File,
                0o600,
                Some(vec![1; 8]),
            )
            .unwrap();
        overlay
            .unlink(1, OsStr::new("byte-overflow"), false)
            .unwrap();
        overlay.reclaim_unreferenced().unwrap();
        release.join().unwrap();
        overlay.drain_maintenance().unwrap();
        assert_eq!(overlay.stats().retirement_sync_fallbacks, 2);
        // One oversize payload is handled synchronously and alone, never queued.
        overlay
            .create(
                1,
                OsStr::new("oversize"),
                Kind::File,
                0o600,
                Some(vec![2; 33 * 1024 * 1024]),
            )
            .unwrap();
        overlay.unlink(1, OsStr::new("oversize"), false).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.stats().retirement_sync_fallbacks, 3);
        assert_eq!(overlay.stats().retirement_pending_items, 0);
        assert_eq!(overlay.stats().retirement_pending_bytes, 0);
        let survivor = overlay
            .create(
                1,
                OsStr::new("survivor"),
                Kind::File,
                0o600,
                Some(b"preserved".to_vec()),
            )
            .unwrap();
        let doomed = overlay
            .create(1, OsStr::new("doomed"), Kind::File, 0o600, Some(vec![4; 8]))
            .unwrap();
        overlay.unlink(1, OsStr::new("doomed"), false).unwrap();
        let path = directory
            .join("payloads")
            .join(format!("{doomed:016x}.data"));
        let backup = directory.join("saved-payload");
        std::fs::rename(&path, &backup).unwrap();
        std::fs::create_dir(&path).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert!(overlay.drain_maintenance().is_err());
        assert_eq!(overlay.stats().retirement_failures, 1);
        assert_eq!(overlay.stats().retirement_pending_items, 1);
        assert_eq!(overlay.stats().retirement_pending_bytes, 8);
        assert!(
            overlay
                .create(1, OsStr::new("blocked"), Kind::File, 0o600, None)
                .is_err()
        );
        assert_eq!(std::fs::read(&backup).unwrap(), [4; 8]);
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&backup, &path).unwrap();
        let unknown = directory.join("payloads/unknown-orphan");
        std::fs::write(&unknown, b"do not guess").unwrap();
        drop(overlay);
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        assert!(!path.exists());
        assert!(overlay.entry(doomed).is_err());
        assert_eq!(overlay.read(survivor, &mut repo).unwrap(), b"preserved");
        assert_eq!(std::fs::read(unknown).unwrap(), b"do not guess");
    }

    #[test]
    fn retirement_commit_uncertainty_recovers_without_journal_mutation() {
        for point in ["before_retirement_commit", "after_retirement_commit"] {
            let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let inode = overlay
                .create(1, OsStr::new("temp"), Kind::File, 0o600, Some(vec![9; 64]))
                .unwrap();
            overlay.unlink(1, OsStr::new("temp"), false).unwrap();
            let (token, _) = super::changes_since(&directory, "").unwrap();
            fail_next(point);
            assert!(overlay.reclaim_unreferenced().is_err());
            assert!(overlay.read(inode, &mut repo).is_err());
            assert_eq!(overlay.stats().retirement_failures, 1);
            drop(overlay);
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            assert!(overlay.entry(inode).is_err());
            assert_eq!(
                super::changes_since(&directory, &token).unwrap(),
                (token, vec![])
            );
            assert!(
                overlay
                    .create(1, OsStr::new("next"), Kind::File, 0o600, None)
                    .unwrap()
                    > inode
            );
        }
    }

    #[test]
    fn collection_visits_only_retired_candidates_in_a_large_base_tree() {
        let (_temp, repo, snapshot, directory, gitdir) = fixture();
        let oid = snapshot
            .nodes()
            .iter()
            .find(|n| n.name == OsStr::new("base"))
            .unwrap()
            .oid
            .as_deref()
            .unwrap();
        // Build a real large Git tree instead of mutating the immutable snapshot.
        let mut tree = Command::new("git")
            .arg("-C")
            .arg(&gitdir)
            .arg("mktree")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        {
            let mut input = tree.stdin.take().unwrap();
            writeln!(input, "100644 blob {oid}\tbase").unwrap();
            for n in 0..100_000 {
                writeln!(input, "100644 blob {oid}\tpadding-{n}").unwrap();
            }
        }
        let tree = tree.wait_with_output().unwrap();
        assert!(tree.status.success());
        let tree = String::from_utf8(tree.stdout).unwrap();
        let commit = Command::new("git")
            .arg("-C")
            .arg(&gitdir)
            .args([
                "commit-tree",
                tree.trim(),
                "-p",
                &snapshot.commit,
                "-m",
                "Large tree fixture",
            ])
            .env("GIT_AUTHOR_NAME", "Fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.test")
            .env("GIT_COMMITTER_NAME", "Fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.test")
            .output()
            .unwrap();
        assert!(commit.status.success());
        let commit = String::from_utf8(commit.stdout).unwrap();
        let snapshot = Snapshot::load(&repo, commit.trim()).unwrap();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let before = overlay.stats().gc_visited_entries;
        let inode = overlay
            .create(1, OsStr::new("temp"), Kind::File, 0o600, None)
            .unwrap();
        overlay.unlink(1, OsStr::new("temp"), false).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert!(overlay.entry(inode).is_err());
        assert_eq!(overlay.stats().gc_visited_entries - before, 1);
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(overlay.stats().gc_visited_entries - before, 1);
    }

    #[test]
    fn unpublished_first_copyup_failure_preserves_authoritative_bytes() {
        for point in [
            "after_copyup_old",
            "after_copyup_data",
            "before_copyup_directory_sync",
            "after_copyup_sync",
            "before_copyup_commit",
        ] {
            for from_base in [false, true] {
                let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
                let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
                let ino = if from_base {
                    overlay.lookup(1, OsStr::new("base")).unwrap()
                } else {
                    overlay
                        .create(1, OsStr::new("empty"), Kind::File, 0o600, None)
                        .unwrap()
                };
                let old = overlay.read(ino, &mut repo).unwrap();
                let (token, _) = super::changes_since(&directory, "").unwrap();
                fail_next(point);
                assert!(
                    overlay.write(ino, 3, &[7; 4], &mut repo).is_err(),
                    "{point}"
                );
                assert_eq!(overlay.read(ino, &mut repo).unwrap(), old, "{point}");
                assert_eq!(
                    super::changes_since(&directory, &token).unwrap(),
                    (token, vec![])
                );
                overlay.write(ino, 3, &[7; 4], &mut repo).unwrap();
                let mut expected = old;
                expected.resize(expected.len().max(7), 0);
                expected[3..7].fill(7);
                assert_eq!(overlay.read(ino, &mut repo).unwrap(), expected);
                assert_eq!(overlay.stats().recovered_operations, 0);
                drop(overlay);
                let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
                assert_eq!(overlay.read(ino, &mut repo).unwrap(), expected);
            }
        }
        for (point, committed) in [
            ("before_namespace_commit", false),
            ("after_namespace_commit", true),
        ] {
            let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let ino = overlay
                .create(1, OsStr::new("empty"), Kind::File, 0o600, None)
                .unwrap();
            let (token, _) = super::changes_since(&directory, "").unwrap();
            fail_next(point);
            assert!(overlay.write(ino, 0, b"new", &mut repo).is_err());
            assert!(overlay.read(ino, &mut repo).is_err());
            drop(overlay);
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            assert_eq!(
                overlay.read(ino, &mut repo).unwrap(),
                if committed { b"new".as_slice() } else { b"" }
            );
            assert_eq!(
                !super::changes_since(&directory, &token)
                    .unwrap()
                    .1
                    .is_empty(),
                committed
            );
        }
    }

    #[test]
    fn durable_payload_failures_poison_then_recover_once() {
        for point in [
            "after_intent",
            "before_payload_write",
            "after_first_write",
            "before_payload_resize",
            "before_payload_sync",
            "after_payload_sync",
            "before_final_commit",
            "after_final_commit",
            "before_redo_retire",
        ] {
            let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let ino = overlay
                .create(
                    1,
                    OsStr::new("edited"),
                    Kind::File,
                    0o600,
                    Some(vec![1; 192 * 1024]),
                )
                .unwrap();
            let (before_token, _) = super::changes_since(&directory, "").unwrap();
            let bytes = vec![2; 128 * 1024];
            fail_next(point);
            assert!(
                overlay.write(ino, 32 * 1024, &bytes, &mut repo).is_err(),
                "{point}"
            );
            assert!(overlay.read_range(ino, 0, 1, &mut repo).is_err(), "{point}");
            assert!(overlay.digest(ino).is_err(), "{point}");
            assert!(overlay.sync().is_err(), "{point}");
            assert!(
                overlay
                    .create(1, OsStr::new("forbidden"), Kind::File, 0o600, None)
                    .is_err(),
                "{point}"
            );
            drop(overlay);
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let mut expected = vec![1; 192 * 1024];
            expected[32 * 1024..160 * 1024].copy_from_slice(&bytes);
            assert_eq!(overlay.read(ino, &mut repo).unwrap(), expected, "{point}");
            let (after_token, paths) = super::changes_since(&directory, &before_token).unwrap();
            assert_eq!(paths, [b"edited".to_vec()], "{point}");
            let before_generation: u64 = before_token.rsplit(':').next().unwrap().parse().unwrap();
            let after_generation: u64 = after_token.rsplit(':').next().unwrap().parse().unwrap();
            assert_eq!(after_generation, before_generation + 1, "{point}");
            assert_eq!(
                std::fs::metadata(directory.join("payloads/redo.data"))
                    .unwrap()
                    .len(),
                0
            );
            drop(overlay);
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            assert_eq!(overlay.read(ino, &mut repo).unwrap(), expected);
            assert_eq!(
                super::changes_since(&directory, &after_token).unwrap(),
                (after_token, vec![])
            );
        }
    }

    #[test]
    fn replay_can_fail_again_and_corrupt_redo_fails_closed() {
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let ino = overlay
            .create(
                1,
                OsStr::new("edited"),
                Kind::File,
                0o600,
                Some(vec![1; 192 * 1024]),
            )
            .unwrap();
        let (token, _) = super::changes_since(&directory, "").unwrap();
        let bytes = vec![9; 128 * 1024];
        fail_next("after_intent");
        assert!(overlay.write(ino, 0, &bytes, &mut repo).is_err());
        drop(overlay);
        let redo = directory.join("payloads/redo.data");
        std::fs::write(&redo, vec![8; bytes.len()]).unwrap();
        assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
        assert_eq!(
            super::changes_since(&directory, &token).unwrap(),
            (token.clone(), vec![])
        );
        // Postmetadata with a still-pending intent is impossible: publication
        // removes it in the same transaction. Reject without altering evidence.
        let payload_path = directory.join("payloads").join(format!("{ino:016x}.data"));
        let payload_before = std::fs::read(&payload_path).unwrap();
        let redo_before = std::fs::read(&redo).unwrap();
        let handles = Handles::open(&directory, false).unwrap();
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        let raw_intent = tx
            .get(&handles.meta, b"payload_intent")
            .unwrap()
            .unwrap()
            .to_vec();
        let intent: Intent = serde_json::from_slice(&raw_intent).unwrap();
        let journal_before: Vec<_> = tx
            .iter(&handles.journal)
            .map(|item| {
                let (k, v) = item.into_inner().unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        tx.insert(
            &handles.entries,
            ino.to_be_bytes(),
            intent.after_entry.as_slice(),
        );
        tx.insert(
            &handles.meta,
            b"journal_generation",
            (intent.before_generation + 1).to_be_bytes(),
        );
        tx.commit().unwrap();
        drop(handles);
        assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
        assert_eq!(std::fs::read(&payload_path).unwrap(), payload_before);
        assert_eq!(std::fs::read(&redo).unwrap(), redo_before);
        let handles = Handles::open(&directory, false).unwrap();
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        assert_eq!(
            &*tx.get(&handles.meta, b"payload_intent").unwrap().unwrap(),
            raw_intent
        );
        assert_eq!(
            &*tx.get(&handles.entries, ino.to_be_bytes())
                .unwrap()
                .unwrap(),
            intent.after_entry
        );
        assert_eq!(
            number(
                &tx.get(&handles.meta, b"journal_generation")
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            intent.before_generation + 1
        );
        let journal_after: Vec<_> = tx
            .iter(&handles.journal)
            .map(|item| {
                let (k, v) = item.into_inner().unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        assert_eq!(journal_after, journal_before);
        // Repair only the explicitly injected metadata to continue the existing
        // valid replay / repeated failure checks using the original intent.
        tx.insert(
            &handles.entries,
            ino.to_be_bytes(),
            intent.before_entry.as_slice(),
        );
        tx.insert(
            &handles.meta,
            b"journal_generation",
            intent.before_generation.to_be_bytes(),
        );
        tx.commit().unwrap();
        drop(handles);
        std::fs::write(&redo, &bytes).unwrap();
        fail_next("after_first_write");
        assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
        assert_eq!(
            super::changes_since(&directory, &token).unwrap(),
            (token.clone(), vec![])
        );
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        assert_eq!(overlay.stats().recovered_operations, 1);
        assert_eq!(
            overlay.read_range(ino, 0, bytes.len(), &mut repo).unwrap(),
            bytes
        );
        assert_eq!(
            super::changes_since(&directory, &token).unwrap().1,
            [b"edited".to_vec()]
        );
    }

    #[test]
    fn unexpectedly_truncated_prepared_payload_is_not_zero_filled() {
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let ino = overlay
            .create(
                1,
                OsStr::new("edited"),
                Kind::File,
                0o600,
                Some(vec![1; 128 * 1024]),
            )
            .unwrap();
        let (token, _) = super::changes_since(&directory, "").unwrap();
        fail_next("after_intent");
        assert!(overlay.write(ino, 0, &[2; 16], &mut repo).is_err());
        drop(overlay);
        let path = directory.join("payloads").join(format!("{ino:016x}.data"));
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(1024)
            .unwrap();
        assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
        assert_eq!(std::fs::metadata(path).unwrap().len(), 1024);
        assert_eq!(
            super::changes_since(&directory, &token).unwrap(),
            (token, vec![])
        );
    }

    #[test]
    fn truncate_failure_recovery_preserves_the_single_journal_generation() {
        for size in [1024u64, 256 * 1024] {
            let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let ino = overlay
                .create(
                    1,
                    OsStr::new("edited"),
                    Kind::File,
                    0o600,
                    Some(vec![1; 128 * 1024]),
                )
                .unwrap();
            let (token, _) = super::changes_since(&directory, "").unwrap();
            fail_next("after_payload_sync");
            assert!(
                overlay
                    .setattr(ino, None, Some(size), None, &mut repo)
                    .is_err()
            );
            assert!(overlay.read(ino, &mut repo).is_err());
            drop(overlay);
            fail_next("before_final_commit");
            assert!(Overlay::open(&directory, &snapshot, &gitdir).is_err());
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let actual = overlay.read(ino, &mut repo).unwrap();
            let mut expected = vec![1; 128 * 1024];
            expected.resize(size as usize, 0);
            assert_eq!(actual, expected);
            let (new, paths) = super::changes_since(&directory, &token).unwrap();
            assert_eq!(paths, [b"edited".to_vec()]);
            let generation = |s: &str| s.rsplit(':').next().unwrap().parse::<u64>().unwrap();
            assert_eq!(generation(&new), generation(&token) + 1);
        }
    }

    #[test]
    fn killed_prepared_mutations_and_replay_recover_acknowledged_state() {
        const TEST: &str = "overlay::fjall::tests::killed_prepared_mutations_and_replay_recover_acknowledged_state";
        if let Some(root) = std::env::var_os("GIT_VFS_TEST_CRASH_ROOT") {
            let root = PathBuf::from(root);
            let gitdir = root.join("bare.git");
            let mut repo = GitRepo::open(&gitdir, "git", "origin", SizeFallback::Error).unwrap();
            let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
            let mut overlay = Overlay::open(&root.join("overlay"), &snapshot, &gitdir).unwrap();
            if std::env::var("GIT_VFS_TEST_ACTION").as_deref() == Ok("mutate") {
                let ino = overlay.lookup(1, OsStr::new("edited")).unwrap();
                overlay
                    .write(ino, 32 * 1024, &vec![2; 128 * 1024], &mut repo)
                    .unwrap();
            }
            panic!("child never reached requested durable pause boundary");
        }
        for point in [
            "after_intent",
            "after_first_write",
            "after_payload_sync",
            "after_final_commit",
        ] {
            let (temp, repo, snapshot, directory, gitdir) = fixture();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let ino = overlay
                .create(
                    1,
                    OsStr::new("edited"),
                    Kind::File,
                    0o600,
                    Some(vec![1; 192 * 1024]),
                )
                .unwrap();
            let (token, _) = super::changes_since(&directory, "").unwrap();
            drop(overlay);
            drop(repo); // The child owns the private Git size-cache database too.
            let kill_at = |pause: &str, action: &str| {
                let marker = temp.path().join("crash-ready");
                let _ = std::fs::remove_file(&marker);
                let mut child = Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env("GIT_VFS_TEST_CRASH_ROOT", temp.path())
                    .env("GIT_VFS_TEST_PAUSE_AT", pause)
                    .env("GIT_VFS_TEST_ACTION", action)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !marker.exists() {
                    if let Some(status) = child.try_wait().unwrap() {
                        panic!("crash child exited before {pause}: {status}");
                    }
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("crash child stalled before {pause}");
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                child.kill().unwrap();
                assert!(!child.wait().unwrap().success());
            };
            kill_at(point, "mutate");
            if point == "after_first_write" {
                // Kill a second process in the middle of replaying the same intent.
                kill_at("after_first_write", "recover");
            }
            let mut repo = GitRepo::open(&gitdir, "git", "origin", SizeFallback::Error).unwrap();
            let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
            let mut expected = vec![1; 192 * 1024];
            expected[32 * 1024..160 * 1024].fill(2);
            assert_eq!(overlay.read(ino, &mut repo).unwrap(), expected, "{point}");
            let (new, paths) = super::changes_since(&directory, &token).unwrap();
            assert_eq!(paths, [b"edited".to_vec()], "{point}");
            let generation = |s: &str| s.rsplit(':').next().unwrap().parse::<u64>().unwrap();
            assert_eq!(generation(&new), generation(&token) + 1, "{point}");
        }
    }

    #[test]
    fn range_writes_and_sparse_truncate_do_not_copy_whole_files() {
        let (_temp, mut repo, snapshot, directory, gitdir) = fixture();
        let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
        let ino = overlay
            .create(
                1,
                OsStr::new("edited"),
                Kind::File,
                0o600,
                Some(vec![1; 2 * 1024 * 1024]),
            )
            .unwrap();
        let before = overlay.stats();
        assert_eq!(
            overlay.read_range(ino, 1024, 16, &mut repo).unwrap(),
            [1; 16]
        );
        overlay.write(ino, 1024, &[2; 8], &mut repo).unwrap();
        overlay
            .setattr(ino, None, Some(3 * 1024 * 1024), None, &mut repo)
            .unwrap();
        let after = overlay.stats();
        assert_eq!(after.payload_read_bytes - before.payload_read_bytes, 16);
        assert_eq!(after.payload_write_bytes - before.payload_write_bytes, 8);
        assert_eq!(after.payload_copy_bytes, before.payload_copy_bytes);
        assert_eq!(
            overlay.read_range(ino, 1020, 16, &mut repo).unwrap(),
            [1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1]
        );
        assert_eq!(
            overlay
                .read_range(ino, 2 * 1024 * 1024, 8, &mut repo)
                .unwrap(),
            [0; 8]
        );
        overlay
            .write(ino, 3 * 1024 * 1024 + 16, &[5; 4], &mut repo)
            .unwrap();
        assert_eq!(
            overlay
                .read_range(ino, 3 * 1024 * 1024, 20, &mut repo)
                .unwrap(),
            [vec![0; 16], vec![5; 4]].concat()
        );
        let before = overlay.stats();
        overlay.digest(ino).unwrap();
        overlay.blake3_digest(ino).unwrap();
        assert_eq!(
            overlay.stats().payload_hash_bytes - before.payload_hash_bytes,
            3 * 1024 * 1024 + 20
        );
        fail_next("before_redo_stage");
        assert!(overlay.write(ino, 0, &[3], &mut repo).is_err());
        assert_eq!(overlay.read_range(ino, 0, 1, &mut repo).unwrap(), [1]);
        overlay
            .setattr(ino, None, Some(0), None, &mut repo)
            .unwrap();
        assert!(overlay.read(ino, &mut repo).unwrap().is_empty());
    }
}
