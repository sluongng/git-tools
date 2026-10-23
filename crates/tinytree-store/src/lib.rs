//! Authoritative native objects and references, not an LRU cache.
//!
//! One process owns a directory. Within that process the publication gate orders
//! object installation, reference changes, reader leases and GC. Upload reading,
//! hashing and temporary-file sync happen outside the gate. Files precede durable
//! metadata; GC durably removes metadata before unlinking files. Interrupted
//! operations can therefore leave harmless unindexed files, never an index-only
//! successful read. Authentication belongs to the service, before calling this
//! repository-scoped API; an object ID or staging lease is not authorization.

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use tempfile::NamedTempFile;
use tinytree_model::{CommitId, Descriptor, Kind, Limits, ObjectId, ObjectKey};

mod cloud;
pub use cloud::{
    CloudConfig, CloudObjectDescriptor, CloudOwner, CloudReadLease, CloudWriteReceipt,
};
mod client_gc;
mod closure;
pub use client_gc::{ClientGcEpoch, ClientGcLimits, ClientGcReport, ClientGcRequest};
mod pack;
mod retention;
pub use pack::{PackOptions, PackReport};

pub use retention::{
    RetentionLimits, RetentionOwner, parse_retention_reference, retention_reference,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Database(fjall::Error),
    Model(tinytree_model::Error),
    Invalid(&'static str),
    Corrupt(&'static str),
    UnknownRepository,
    MissingObject(ObjectKey),
    StaleReference(Reference),
    IdempotencyConflict,
    Limit(&'static str),
    Poisoned,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "native store I/O: {e}"),
            Self::Database(e) => write!(f, "native store database: {e}"),
            Self::Model(e) => write!(f, "native object: {e}"),
            Self::Invalid(why) => write!(f, "invalid native store request: {why}"),
            Self::Corrupt(why) => write!(f, "corrupt native store: {why}"),
            Self::UnknownRepository => f.write_str("unknown native repository"),
            Self::MissingObject(_) => f.write_str("native object is unavailable"),
            Self::StaleReference(_) => f.write_str("native reference generation changed"),
            Self::IdempotencyConflict => f.write_str("idempotency key has a different request"),
            Self::Limit(why) => write!(f, "native store limit: {why}"),
            Self::Poisoned => f.write_str("native store publication gate is poisoned"),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<fjall::Error> for Error {
    fn from(e: fjall::Error) -> Self {
        Self::Database(e)
    }
}
impl From<tinytree_model::Error> for Error {
    fn from(e: tinytree_model::Error) -> Self {
        Self::Model(e)
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub model_limits: Limits,
    pub cloud: CloudConfig,
    pub max_file_bytes: u64,
    pub max_repository_bytes: u64,
    pub max_objects_per_repository: u64,
    pub max_closure_objects: usize,
    pub max_closure_edges: usize,
    pub max_path_contexts: usize,
    pub max_path_context_bytes: usize,
    pub max_staging_bytes: u64,
    pub max_staging_uploads: usize,
    pub staging_ttl_seconds: i64,
    pub max_retention_validation_records: usize,
    pub max_retention_validation_bytes: u64,
    /// Independent upper bound for simultaneous packed-object reconstruction.
    pub max_packed_read_memory_bytes: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            model_limits: Limits::default(),
            cloud: CloudConfig::default(),
            max_file_bytes: 256 * 1024 * 1024,
            max_repository_bytes: 16 * 1024 * 1024 * 1024,
            max_objects_per_repository: 1_000_000,
            max_closure_objects: 100_000,
            max_closure_edges: 1_000_000,
            max_path_contexts: 100_000,
            max_path_context_bytes: 32 * 1024 * 1024,
            max_staging_bytes: 1024 * 1024 * 1024,
            max_staging_uploads: 64,
            staging_ttl_seconds: 3600,
            max_retention_validation_records: 2_000_000,
            max_retention_validation_bytes: 1024 * 1024 * 1024,
            max_packed_read_memory_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
    pub name: String,
    pub generation: u64,
    pub commit: Option<Descriptor>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub idempotency_key: String,
    pub reference: Reference,
}
#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub idempotency_key: String,
    pub reference_name: String,
    pub expected_generation: u64,
    pub new_commit: Option<CommitId>,
    pub staging_leases: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteReceipt {
    pub descriptor: Descriptor,
    pub staging_lease: String,
    pub expires_at: i64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub expired_leases: u64,
    pub deleted_cloud_objects: u64,
    pub deleted_cloud_bytes: u64,
    pub expired_cloud_leases: u64,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}
struct Inner {
    root: PathBuf,
    config: Config,
    db: SingleWriterTxDatabase,
    spaces: Spaces,
    gate: Mutex<Runtime>,
    uncertain_commit: AtomicBool,
    packed_read_bytes: Arc<AtomicU64>,
    _owner: File,
}
struct Spaces {
    meta: SingleWriterTxKeyspace,
    repos: SingleWriterTxKeyspace,
    objects: SingleWriterTxKeyspace,
    refs: SingleWriterTxKeyspace,
    receipts: SingleWriterTxKeyspace,
    staging: SingleWriterTxKeyspace,
    client_ages: SingleWriterTxKeyspace,
    client_revisions: SingleWriterTxKeyspace,
    client_deletions: SingleWriterTxKeyspace,
    locations: SingleWriterTxKeyspace,
    packs: SingleWriterTxKeyspace,
    cloud: cloud::Spaces,
}
#[derive(Default)]
struct Runtime {
    readers: HashMap<(String, ObjectKey), usize>,
    cloud_readers: HashMap<(String, [u8; 32], tinytree_cloud::ObjectId), usize>,
    cloud_reservations: HashMap<(String, [u8; 32]), (u64, u64, u64)>,
    staging_bytes: u64,
    staging_uploads: usize,
    repository_reservations: HashMap<String, (u64, u64)>,
}

// Metadata records deliberately use a separate representation from canonical
// native envelopes. Parsing metadata never defines a native object's identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDescriptor {
    kind: u8,
    id: [u8; 32],
    size: u64,
    version: u16,
}
impl From<Descriptor> for StoredDescriptor {
    fn from(d: Descriptor) -> Self {
        Self {
            kind: d.key.kind as u8,
            id: d.key.id.0,
            size: d.size_bytes,
            version: d.format_version,
        }
    }
}
impl StoredDescriptor {
    fn descriptor(&self) -> Result<Descriptor> {
        if self.version != tinytree_model::FORMAT_VERSION {
            return Err(Error::Corrupt("object format"));
        }
        Ok(Descriptor {
            key: ObjectKey {
                kind: Kind::try_from(self.kind)?,
                id: ObjectId(self.id),
            },
            size_bytes: self.size,
            format_version: self.version,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReference {
    generation: u64,
    commit: Option<StoredDescriptor>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepositoryRecord {
    bytes: u64,
    objects: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRecord {
    fingerprint: [u8; 32],
    reference: StoredReference,
    reference_name: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StageRecord {
    object: StoredDescriptor,
    expires_at: i64,
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::Corrupt("metadata encoding"))
}
fn decode<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > 16 * 1024 {
        return Err(Error::Corrupt("metadata record size"));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt("metadata record encoding"))
}
/// Validate a portable repository ID before preparing persistent storage.
pub fn validate_repository_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(Error::Invalid(
            "name must be lowercase ASCII, digits, '-' or '_'",
        ));
    }
    let device = matches!(value, "con" | "prn" | "aux" | "nul")
        || ((value.starts_with("com") || value.starts_with("lpt"))
            && value.len() == 4
            && matches!(value.as_bytes()[3], b'1'..=b'9'));
    if device {
        return Err(Error::Invalid("reserved repository name"));
    }
    Ok(())
}
fn name(value: &str) -> Result<()> {
    validate_repository_id(value)
}
fn reference_name(value: &str) -> Result<()> {
    // References are database values, never filesystem paths. Native bookmarks
    // need not adopt the Git side's naming rules or lowercase path restrictions.
    if value.is_empty() || value.len() > 512 || value.contains('\0') {
        return Err(Error::Invalid("reference name"));
    }
    Ok(())
}
fn request_token(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::Invalid("request token"));
    }
    Ok(())
}
fn key(repo: &str, suffix: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(repo.len() + 1 + suffix.len());
    result.extend_from_slice(repo.as_bytes());
    result.push(0);
    result.extend_from_slice(suffix);
    result
}
fn object_suffix(k: ObjectKey) -> [u8; 33] {
    let mut value = [0; 33];
    value[0] = k.kind as u8;
    value[1..].copy_from_slice(&k.id.0);
    value
}
fn object_key(repo: &str, k: ObjectKey) -> Vec<u8> {
    key(repo, &object_suffix(k))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn lease_id(k: ObjectKey) -> String {
    format!("{}-{}", k.kind as u8, hex(&k.id.0))
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn store_feature(value: Option<&[u8]>) -> Result<u8> {
    match value {
        Some(b"tinytree-store-1") => Ok(1),
        Some(b"tinytree-store-2") => Ok(2),
        Some(b"tinytree-store-3") => Ok(3),
        _ => Err(Error::Corrupt("store format")),
    }
}
fn advance_feature(
    tx: &mut fjall::SingleWriterWriteTx<'_>,
    meta: &SingleWriterTxKeyspace,
    minimum: u8,
) -> Result<()> {
    let current = store_feature(tx.get(meta, b"format")?.as_deref())?;
    if current < minimum {
        let value = match minimum {
            2 => b"tinytree-store-2".as_slice(),
            3 => b"tinytree-store-3".as_slice(),
            _ => return Err(Error::Invalid("store feature")),
        };
        tx.insert(meta, b"format", value);
    }
    Ok(())
}
const STORE_MARKER: &str = "tinytree.format";
const STORE_MARKER_BYTES: &[u8] = b"tinytree-authoritative-store-1\n";
struct RootPlan {
    path: PathBuf,
    fresh: bool,
}

/// Inspect provenance before opening an engine or creating/changing anything.
/// The final component is never followed when it is a symlink.
fn preflight_root(path: &Path) -> Result<RootPlan> {
    use std::path::Component;
    if path.as_os_str().is_empty() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(Error::Invalid("store root path"));
    }
    // Rebuild lexically, before asking the kernel to inspect the final entry.
    // A trailing slash or '/.' otherwise makes lstat follow a final symlink.
    // ParentDir was rejected above and is never collapsed across a link.
    let lexical: PathBuf = path
        .components()
        .filter(|part| !matches!(part, Component::CurDir))
        .collect();
    let absolute = if lexical.is_absolute() {
        lexical
    } else {
        std::env::current_dir()?.join(lexical)
    };
    match fs::symlink_metadata(&absolute) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(Error::Invalid(
                    "store root must be a directory, not a symlink",
                ));
            }
            let path = absolute.canonicalize()?;
            if fs::read_dir(&path)?.next().is_none() {
                return Ok(RootPlan { path, fresh: true });
            }
            require_file_bytes(&path.join(STORE_MARKER), STORE_MARKER_BYTES)?;
            require_regular_file(&path.join("owner.lock"))?;
            for directory in ["repos", "staging", "metadata"] {
                let m = fs::symlink_metadata(path.join(directory))
                    .map_err(|_| Error::Corrupt("store directory missing"))?;
                if !m.is_dir() {
                    return Err(Error::Corrupt("store directory type"));
                }
            }
            // Fjall otherwise treats a missing version file as a request to
            // initialize a fresh database, which is not permitted on reopen.
            require_file_bytes(&path.join("metadata/version"), b"FJL\x03")?;
            Ok(RootPlan { path, fresh: false })
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut ancestor = absolute.as_path();
            let mut components = Vec::new();
            loop {
                match fs::metadata(ancestor) {
                    Ok(m) => {
                        if !m.is_dir() {
                            return Err(Error::Invalid("store ancestor is not a directory"));
                        }
                        let mut path = ancestor.canonicalize()?;
                        for part in components.iter().rev() {
                            path.push(part);
                        }
                        return Ok(RootPlan { path, fresh: true });
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        components.push(
                            ancestor
                                .file_name()
                                .ok_or(Error::Invalid("store root path"))?
                                .to_owned(),
                        );
                        ancestor = ancestor.parent().ok_or(Error::Invalid("store root path"))?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}
fn require_regular_file(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| Error::Corrupt("store marker or owner missing"))?;
    if !metadata.is_file() {
        return Err(Error::Corrupt("store marker or owner type"));
    }
    Ok(())
}
fn require_file_bytes(path: &Path, expected: &[u8]) -> Result<()> {
    require_regular_file(path)?;
    if fs::symlink_metadata(path)?.len() != expected.len() as u64 || fs::read(path)? != expected {
        return Err(Error::Corrupt("store format marker"));
    }
    Ok(())
}
/// Publish every newly created directory entry durably in its parent before
/// descending. Syncing only the final directory does not retain its own name.
fn create_durable_directories(path: &Path) -> Result<()> {
    let mut cursor = path;
    let mut missing = Vec::new();
    while !cursor.try_exists()? {
        missing.push(cursor.to_path_buf());
        cursor = cursor
            .parent()
            .ok_or(Error::Invalid("directory ancestor"))?;
    }
    for directory in missing.iter().rev() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(directory)?;
        sync_dir(
            directory
                .parent()
                .ok_or(Error::Invalid("directory ancestor"))?,
        )?;
        sync_dir(directory)?;
    }
    // Also retain an externally supplied empty root whose creator may not have
    // synced its parent yet. No mode or directory entry is changed here.
    if missing.is_empty() {
        sync_dir(path)?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

impl Store {
    pub fn open(root: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::open_inner(root.as_ref(), config, false)
    }

    /// Reopen recognized authoritative storage without initializing a new root.
    /// Missing, empty and unrecognized paths fail before filesystem mutation.
    pub fn open_existing(root: impl AsRef<Path>, config: Config) -> Result<Self> {
        Self::open_inner(root.as_ref(), config, true)
    }

    fn open_inner(root: &Path, config: Config, existing_only: bool) -> Result<Self> {
        if config.staging_ttl_seconds <= 0
            || config.max_closure_objects == 0
            || config.max_closure_edges == 0
            || config.max_path_contexts == 0
            || config.max_path_context_bytes == 0
            || config.max_staging_bytes == 0
            || config.max_staging_uploads == 0
            || !(1..=10_000_000).contains(&config.max_retention_validation_records)
            || config.max_retention_validation_bytes == 0
            || config.max_packed_read_memory_bytes < pack::PACK_READ_CHARGE
            || config.max_repository_bytes < tinytree_model::empty_tree().descriptor.size_bytes
            || config.max_objects_per_repository == 0
        {
            return Err(Error::Invalid("store configuration"));
        }
        config.cloud.validate()?;
        let planned = preflight_root(root)?;
        if existing_only && planned.fresh {
            return Err(Error::Invalid("existing store required"));
        }
        let root = planned.path;
        if planned.fresh {
            create_durable_directories(&root)?;
        }
        let mut owner_options = OpenOptions::new();
        owner_options.read(true).write(true);
        if planned.fresh {
            owner_options.create_new(true);
        }
        let owner = owner_options.open(root.join("owner.lock"))?;
        owner.try_lock_exclusive()?;
        if planned.fresh {
            // The owner file was created without replacing anything. Recheck
            // before taking ownership of a previously empty directory.
            for item in fs::read_dir(&root)? {
                if item?.file_name() != "owner.lock" {
                    return Err(Error::Invalid("root changed during initialization"));
                }
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
            }
            for p in [root.join("repos"), root.join("staging")] {
                create_durable_directories(&p)?;
            }
            sync_dir(&root)?;
        }
        let db = SingleWriterTxDatabase::builder(root.join("metadata"))
            .manual_journal_persist(true)
            .open()?;
        if !planned.fresh {
            for space in ["meta", "repos", "objects", "refs", "receipts", "staging"] {
                if !db.keyspace_exists(space) {
                    return Err(Error::Corrupt("required keyspace missing"));
                }
            }
        }
        let feature = if planned.fresh {
            1
        } else {
            let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
            store_feature(db.read_tx().get(&meta, b"format")?.as_deref())?
        };
        if !planned.fresh
            && feature >= 2
            && (!db.keyspace_exists("pack-locations-v1") || !db.keyspace_exists("packs-v1"))
        {
            return Err(Error::Corrupt("packed store keyspace missing"));
        }
        let cloud_spaces = cloud::Spaces::open(&db, feature >= 3)?;
        if feature >= 3 {
            let metadata = fs::symlink_metadata(root.join("cloud"))
                .map_err(|_| Error::Corrupt("cloud payload namespace missing"))?;
            if !metadata.is_dir() {
                return Err(Error::Corrupt("cloud payload namespace type"));
            }
        }
        let spaces = Spaces {
            meta: db.keyspace("meta", KeyspaceCreateOptions::default)?,
            repos: db.keyspace("repos", KeyspaceCreateOptions::default)?,
            objects: db.keyspace("objects", KeyspaceCreateOptions::default)?,
            refs: db.keyspace("refs", KeyspaceCreateOptions::default)?,
            receipts: db.keyspace("receipts", KeyspaceCreateOptions::default)?,
            staging: db.keyspace("staging", KeyspaceCreateOptions::default)?,
            client_ages: db.keyspace("client-ages", KeyspaceCreateOptions::default)?,
            client_revisions: db.keyspace("client-revisions", KeyspaceCreateOptions::default)?,
            client_deletions: db.keyspace("client-deletions", KeyspaceCreateOptions::default)?,
            locations: db.keyspace("pack-locations-v1", KeyspaceCreateOptions::default)?,
            packs: db.keyspace("packs-v1", KeyspaceCreateOptions::default)?,
            cloud: cloud_spaces,
        };
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        match tx.get(&spaces.meta, b"format")? {
            Some(value)
                if matches!(
                    &*value,
                    b"tinytree-store-1" | b"tinytree-store-2" | b"tinytree-store-3"
                ) => {}
            Some(_) => return Err(Error::Corrupt("store format")),
            None if planned.fresh => {
                tx.insert(&spaces.meta, b"format", b"tinytree-store-1");
            }
            None => return Err(Error::Corrupt("missing store format")),
        }
        tx.commit()?;
        sync_dir(&root)?;
        if planned.fresh {
            // Last initialization milestone. An interrupted setup without this
            // receipt is refused rather than guessed/reinitialized on reopen.
            let mut marker = NamedTempFile::new_in(&root)?;
            marker.write_all(STORE_MARKER_BYTES)?;
            marker.as_file().sync_all()?;
            marker
                .persist_noclobber(root.join(STORE_MARKER))
                .map_err(|e| Error::Io(e.error))?;
            sync_dir(&root)?;
        }
        let store = Self {
            inner: Arc::new(Inner {
                root,
                config,
                db,
                spaces,
                gate: Mutex::new(Runtime::default()),
                uncertain_commit: AtomicBool::new(false),
                packed_read_bytes: Arc::new(AtomicU64::new(0)),
                _owner: owner,
            }),
        };
        if !planned.fresh {
            store.inner.validate_retention_on_reopen()?;
            store.inner.validate_packs_on_reopen()?;
            store.inner.validate_cloud_on_reopen()?;
        }
        Ok(store)
    }

    pub fn create_repository(&self, repo: &str) -> Result<()> {
        name(repo)?;
        let _guard = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let empty = tinytree_model::empty_tree();
        if tx.get(&self.inner.spaces.repos, repo)?.is_some() {
            if self.inner.descriptor(repo, empty.descriptor.key)? != Some(empty.descriptor) {
                return Err(Error::Corrupt("empty tree missing"));
            }
            return Ok(());
        }
        let directory = self.inner.root.join("repos").join(repo);
        self.inner.reclaim_empty_packs(repo)?;
        for kind in [
            Kind::File,
            Kind::Tree,
            Kind::Commit,
            Kind::Symlink,
            Kind::CopyHistory,
        ] {
            let p = directory.join((kind as u8).to_string());
            fs::create_dir_all(&p)?;
            sync_dir(&p)?;
        }
        sync_dir(&directory)?;
        sync_dir(&self.inner.root.join("repos"))?;
        // Every repository has the same real, persisted empty tree. It is also a
        // permanent GC root; publish its index and repo marker in one transaction.
        // Only this tiny intrinsic object is written under the publication gate.
        let target = self.inner.path(repo, empty.descriptor.key);
        let mut temporary = NamedTempFile::new_in(self.inner.root.join("staging"))?;
        temporary.write_all(&empty.bytes)?;
        temporary.as_file().sync_all()?;
        match temporary.persist_noclobber(&target) {
            Ok(_) => {}
            Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                let mut existing = File::open(&target)?;
                verify_stream(
                    &mut existing,
                    empty.descriptor,
                    self.inner.config.model_limits,
                )?;
                existing.sync_all()?;
            }
            Err(e) => return Err(e.error.into()),
        }
        sync_dir(target.parent().ok_or(Error::Corrupt("empty tree path"))?)?;
        tx.insert(
            &self.inner.spaces.objects,
            object_key(repo, empty.descriptor.key),
            encode(&StoredDescriptor::from(empty.descriptor))?,
        );
        tx.insert(
            &self.inner.spaces.repos,
            repo,
            encode(&RepositoryRecord {
                bytes: empty.descriptor.size_bytes,
                objects: 1,
            })?,
        );
        self.inner.commit(tx)?;
        Ok(())
    }

    pub fn references(&self, repo: &str, names: &[String]) -> Result<Vec<Reference>> {
        let _guard = self.inner.lock()?;
        self.inner.repository(repo)?;
        if names.len() > 1024 {
            return Err(Error::Limit("reference batch"));
        }
        let tx = self.inner.db.read_tx();
        names
            .iter()
            .map(|n| {
                reference_name(n)?;
                let stored = tx.get(&self.inner.spaces.refs, key(repo, n.as_bytes()))?;
                let value = reference(n.clone(), stored.as_deref())?;
                if let Some(expected) = value.commit {
                    self.inner.require_descriptor(repo, expected)?;
                }
                Ok(value)
            })
            .collect()
    }

    pub fn descriptor(&self, repo: &str, object: ObjectKey) -> Result<Option<Descriptor>> {
        let _guard = self.inner.lock()?;
        self.inner.repository(repo)?;
        self.inner.descriptor(repo, object)
    }

    /// A bounded, kind/ID-ordered metadata page. `after` is exclusive. The page
    /// is a consistent read, not a retention lease; callers need protected roots
    /// when following its objects across subsequent calls or GC cycles.
    pub fn list_objects(
        &self,
        repo: &str,
        kind: Kind,
        after: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<Descriptor>> {
        use std::ops::Bound;
        let _guard = self.inner.lock()?;
        self.inner.repository(repo)?;
        if !(1..=1024).contains(&limit) {
            return Err(Error::Invalid("object page limit"));
        }
        let lower = match after {
            Some(id) => Bound::Excluded(object_key(repo, ObjectKey { kind, id })),
            None => Bound::Included(key(repo, &[kind as u8])),
        };
        let upper = Bound::Excluded(key(repo, &[kind as u8 + 1]));
        let view = self.inner.db.read_tx();
        let mut result = Vec::with_capacity(limit);
        for item in view
            .range(&self.inner.spaces.objects, (lower, upper))
            .take(limit)
        {
            let (physical, bytes) = item.into_inner()?;
            let descriptor = decode::<StoredDescriptor>(&bytes)?.descriptor()?;
            if descriptor.key.kind != kind
                || &*physical != object_key(repo, descriptor.key).as_slice()
            {
                return Err(Error::Corrupt("object page key"));
            }
            self.inner.require_descriptor(repo, descriptor)?;
            result.push(descriptor);
        }
        Ok(result)
    }

    /// Protect a stored object and its present reachable graph from GC without
    /// opening or reconstructing its payload. This is a metadata claim, not
    /// byte verification or proof that every dependency is resident. Callers
    /// still verify the objects they read. The guard is process-local only.
    pub fn pin_root(&self, repo: &str, object: ObjectKey) -> Result<RootReadGuard> {
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let descriptor = self
            .inner
            .descriptor(repo, object)?
            .ok_or(Error::MissingObject(object))?;
        let readers = gate.readers.entry((repo.to_owned(), object)).or_default();
        *readers = readers.checked_add(1).ok_or(Error::Limit("reader roots"))?;
        Ok(RootReadGuard {
            descriptor,
            repo: repo.to_owned(),
            inner: Arc::clone(&self.inner),
        })
    }

    pub fn read(&self, repo: &str, object: ObjectKey) -> Result<ReadLease> {
        // Acquire the lease under the same gate as GC before opening the file.
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let descriptor = self
            .inner
            .descriptor(repo, object)?
            .ok_or(Error::MissingObject(object))?;
        let file = self.inner.open_backing(repo, descriptor)?;
        *gate.readers.entry((repo.to_owned(), object)).or_default() += 1;
        drop(gate);
        let mut lease = ReadLease {
            file,
            descriptor,
            repo: repo.to_owned(),
            inner: Arc::clone(&self.inner),
        };
        // Verify outside the gate; the lease prevents deletion while hashing.
        verify_stream(&mut lease.file, descriptor, self.inner.config.model_limits)?;
        lease.file.seek(SeekFrom::Start(0))?;
        Ok(lease)
    }

    pub fn read_metadata(&self, repo: &str, object: ObjectKey) -> Result<(Descriptor, Vec<u8>)> {
        if object.kind == Kind::File {
            return Err(Error::Invalid("file requires streaming read"));
        }
        let mut lease = self.read(repo, object)?;
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes)?;
        Ok((lease.descriptor, bytes))
    }

    pub fn put(
        &self,
        repo: &str,
        descriptor: Descriptor,
        bytes: &[u8],
        now: i64,
    ) -> Result<WriteReceipt> {
        self.put_stream(repo, descriptor, &mut io::Cursor::new(bytes), now)
    }

    pub fn put_stream(
        &self,
        repo: &str,
        descriptor: Descriptor,
        input: &mut impl Read,
        now: i64,
    ) -> Result<WriteReceipt> {
        let requested_expiry = now
            .checked_add(self.inner.config.staging_ttl_seconds)
            .ok_or(Error::Invalid("lease expiry"))?;
        let reservation = self.inner.reserve(repo, descriptor)?;
        let mut temporary = NamedTempFile::new_in(self.inner.root.join("staging"))?;
        let mut hash = blake3::Hasher::new();
        let mut size = 0u64;
        let mut buffer = [0; 64 * 1024];
        loop {
            let remaining = descriptor
                .size_bytes
                .saturating_sub(size)
                .saturating_add(1)
                .min(buffer.len() as u64) as usize;
            let n = input.read(&mut buffer[..remaining])?;
            if n == 0 {
                break;
            }
            size = size
                .checked_add(n as u64)
                .ok_or(Error::Limit("object size"))?;
            if size > descriptor.size_bytes {
                return Err(Error::Invalid("object exceeds declared size"));
            }
            hash.update(&buffer[..n]);
            temporary.write_all(&buffer[..n])?;
        }
        if size != descriptor.size_bytes {
            return Err(Error::Model(tinytree_model::Error::SizeMismatch));
        }
        if hash.finalize().as_bytes() != &descriptor.key.id.0 {
            return Err(Error::Model(tinytree_model::Error::HashMismatch));
        }
        temporary.as_file_mut().seek(SeekFrom::Start(0))?;
        verify_stream(
            temporary.as_file_mut(),
            descriptor,
            self.inner.config.model_limits,
        )?;
        temporary.as_file().sync_all()?;
        let target = self.inner.path(repo, descriptor.key);
        // A concurrent duplicate can win installation. Verify that immutable
        // candidate outside the gate while holding a lease; never replace it.
        let mut candidate = self.existing_candidate(repo, descriptor)?;
        let _guard = loop {
            let guard = self.inner.lock()?;
            self.inner.repository(repo)?;
            // An upload reserved as a duplicate can become a new object if GC
            // removed it while its input was being read. Recheck current quota
            // BEFORE installing any payload under the publication gate.
            self.inner.check_install_quota(repo, descriptor)?;
            if candidate.is_some() {
                break guard;
            }
            match temporary.persist_noclobber(&target) {
                Ok(file) => {
                    drop(file);
                    break guard;
                }
                Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                    temporary = e.file;
                    drop(guard);
                    candidate = self.existing_candidate(repo, descriptor)?;
                }
                Err(e) => return Err(e.error.into()),
            }
        };
        sync_dir(target.parent().ok_or(Error::Corrupt("object path"))?)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let mut repository: RepositoryRecord = decode(
            &tx.get(&self.inner.spaces.repos, repo)?
                .ok_or(Error::UnknownRepository)?,
        )?;
        let object_index = object_key(repo, descriptor.key);
        match tx.get(&self.inner.spaces.objects, &object_index)? {
            Some(value) => {
                if decode::<StoredDescriptor>(&value)?.descriptor()? != descriptor {
                    return Err(Error::Corrupt("descriptor collision"));
                }
            }
            None => {
                repository.bytes = repository
                    .bytes
                    .checked_add(descriptor.size_bytes)
                    .ok_or(Error::Limit("repository size"))?;
                repository.objects = repository
                    .objects
                    .checked_add(1)
                    .ok_or(Error::Limit("repository object count"))?;
                let cloud = self.inner.cloud_usage(repo)?;
                if repository
                    .bytes
                    .checked_add(cloud.0)
                    .is_none_or(|n| n > self.inner.config.max_repository_bytes)
                    || repository
                        .objects
                        .checked_add(cloud.1)
                        .is_none_or(|n| n > self.inner.config.max_objects_per_repository)
                {
                    return Err(Error::Limit("repository quota"));
                }
                tx.insert(
                    &self.inner.spaces.objects,
                    &object_index,
                    encode(&StoredDescriptor::from(descriptor))?,
                );
                tx.insert(&self.inner.spaces.repos, repo, encode(&repository)?);
            }
        }
        self.inner
            .touch_client_object(&mut tx, repo, descriptor.key, now)?;
        let staging_lease = lease_id(descriptor.key);
        let stage_key = key(repo, staging_lease.as_bytes());
        let expires_at = match tx.get(&self.inner.spaces.staging, &stage_key)? {
            Some(bytes) => {
                let previous: StageRecord = decode(&bytes)?;
                if previous.object.descriptor()? != descriptor {
                    return Err(Error::Corrupt("staging descriptor changed"));
                }
                requested_expiry.max(previous.expires_at)
            }
            None => requested_expiry,
        };
        tx.insert(
            &self.inner.spaces.staging,
            stage_key,
            encode(&StageRecord {
                object: descriptor.into(),
                expires_at,
            })?,
        );
        self.inner.commit(tx)?;
        drop(_guard);
        drop(candidate);
        drop(reservation);
        Ok(WriteReceipt {
            descriptor,
            staging_lease,
            expires_at,
        })
    }

    fn existing_candidate(&self, repo: &str, descriptor: Descriptor) -> Result<Option<ReadLease>> {
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let path = self.inner.path(repo, descriptor.key);
        if self.inner.is_packed(repo, descriptor.key)? {
            let file = self.inner.open_backing(repo, descriptor)?;
            *gate
                .readers
                .entry((repo.to_owned(), descriptor.key))
                .or_default() += 1;
            drop(gate);
            let mut lease = ReadLease {
                file,
                descriptor,
                repo: repo.to_owned(),
                inner: self.inner.clone(),
            };
            verify_stream(&mut lease.file, descriptor, self.inner.config.model_limits)?;
            return Ok(Some(lease));
        }
        let metadata = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if !metadata.is_file() || metadata.len() != descriptor.size_bytes {
            return Err(Error::Corrupt("existing object payload"));
        }
        let file = pack::Backing::Loose(File::open(path)?);
        *gate
            .readers
            .entry((repo.to_owned(), descriptor.key))
            .or_default() += 1;
        drop(gate);
        let mut lease = ReadLease {
            file,
            descriptor,
            repo: repo.to_owned(),
            inner: Arc::clone(&self.inner),
        };
        verify_stream(&mut lease.file, descriptor, self.inner.config.model_limits)?;
        lease.file.sync_all()?;
        Ok(Some(lease))
    }

    pub fn publication(&self, repo: &str, idempotency_key: &str) -> Result<Option<Publication>> {
        request_token(idempotency_key)?;
        let _guard = self.inner.lock()?;
        self.inner.repository(repo)?;
        self.inner
            .db
            .read_tx()
            .get(
                &self.inner.spaces.receipts,
                key(repo, idempotency_key.as_bytes()),
            )?
            .map(|bytes| {
                decode::<ReceiptRecord>(&bytes)
                    .and_then(|record| publication(idempotency_key, record))
            })
            .transpose()
    }

    pub fn publish(&self, repo: &str, request: &PublishRequest, now: i64) -> Result<Publication> {
        if request.reference_name == "_tinytree" || request.reference_name.starts_with("_tinytree/")
        {
            return Err(Error::Invalid("reserved native reference namespace"));
        }
        self.publish_inner(repo, request, now, None)
    }

    /// Publish an owner-scoped durable hold or release it. Holds never expire.
    /// Authentication and stable principal-to-owner mapping belong to the caller.
    pub fn publish_retention(
        &self,
        repo: &str,
        owner: RetentionOwner,
        request: &PublishRequest,
        limits: &RetentionLimits,
        now: i64,
    ) -> Result<Publication> {
        limits.validate()?;
        if !request.staging_leases.is_empty() {
            return Err(Error::Invalid("retention does not use staging leases"));
        }
        let (actual_owner, named_commit) = parse_retention_reference(&request.reference_name)
            .ok_or(Error::Invalid("retention reference name"))?;
        if actual_owner != owner || request.new_commit.is_some_and(|id| id != named_commit) {
            return Err(Error::Invalid("retention owner or immutable target"));
        }
        self.publish_inner(repo, request, now, Some((owner, limits)))
    }

    fn publish_inner(
        &self,
        repo: &str,
        request: &PublishRequest,
        now: i64,
        retention: Option<(RetentionOwner, &RetentionLimits)>,
    ) -> Result<Publication> {
        request_token(&request.idempotency_key)?;
        reference_name(&request.reference_name)?;
        if request.staging_leases.len() > self.inner.config.max_closure_objects
            || request.staging_leases.iter().any(|s| s.len() > 128)
        {
            return Err(Error::Limit("publication leases"));
        }
        let fingerprint = fingerprint(request);
        let receipt_key = key(repo, request.idempotency_key.as_bytes());
        {
            let _guard = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(receipt) = self.inner.replay(repo, request, fingerprint)? {
                return Ok(receipt);
            }
            let view = self.inner.db.read_tx();
            let current = reference(
                request.reference_name.clone(),
                view.get(
                    &self.inner.spaces.refs,
                    key(repo, request.reference_name.as_bytes()),
                )?
                .as_deref(),
            )?;
            if current.generation != request.expected_generation {
                return Err(Error::StaleReference(current));
            }
        }
        // Root lease acquisition is ordered with GC. Its graph protects present
        // children while canonical/hash/length validation runs outside the gate.
        let root_lease = if let Some(id) = request.new_commit {
            if id == CommitId::ROOT {
                return Err(Error::Invalid("synthetic root cannot be published"));
            }
            let k = ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(id.0),
            };
            let lease = self.read(repo, k)?;
            self.inner.closure(repo, [k], true, true)?;
            Some(lease)
        } else {
            None
        };
        let commit = root_lease.as_ref().map(|lease| lease.descriptor);
        let _guard = self.inner.lock()?;
        self.inner.repository(repo)?;
        // Another caller can have published while validation ran. Recheck both
        // idempotency and generation at the actual publication linearization.
        if let Some(receipt) = self.inner.replay(repo, request, fingerprint)? {
            return Ok(receipt);
        }
        let view = self.inner.db.read_tx();
        let ref_key = key(repo, request.reference_name.as_bytes());
        let current = reference(
            request.reference_name.clone(),
            view.get(&self.inner.spaces.refs, &ref_key)?.as_deref(),
        )?;
        if current.generation != request.expected_generation {
            return Err(Error::StaleReference(current));
        }
        for lease in &request.staging_leases {
            let bytes = view
                .get(&self.inner.spaces.staging, key(repo, lease.as_bytes()))?
                .ok_or(Error::Invalid("unknown staging lease"))?;
            let stage: StageRecord = decode(&bytes)?;
            if stage.expires_at <= now {
                return Err(Error::Invalid("expired staging lease"));
            }
        }
        let retention_update = retention
            .map(|(owner, limits)| {
                self.inner
                    .retention_update(repo, owner, request, &current, limits)
            })
            .transpose()?;
        let stored = StoredReference {
            generation: current
                .generation
                .checked_add(1)
                .ok_or(Error::Limit("reference generation"))?,
            commit: commit.map(Into::into),
        };
        let record = ReceiptRecord {
            fingerprint,
            reference: stored.clone(),
            reference_name: request.reference_name.clone(),
        };
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&self.inner.spaces.refs, ref_key, encode(&stored)?);
        tx.insert(&self.inner.spaces.receipts, receipt_key, encode(&record)?);
        if let Some(update) = retention_update {
            update.write(&mut tx, &self.inner.spaces.meta)?;
        }
        self.inner.commit(tx)?;
        publication(&request.idempotency_key, record)
    }

    /// Fail-closed mark/sweep. Root corruption or an incomplete published closure
    /// aborts before any deletion. Staging roots can reference not-yet-uploaded
    /// children; present descendants are protected without inventing missing ones.
    pub fn gc(&self, repo: &str, now: i64) -> Result<GcReport> {
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let cloud = self.inner.cloud_gc_plan(repo, now, &gate)?;
        let prefix = key(repo, b"");
        let view = self.inner.db.read_tx();
        let mut roots = HashSet::from([tinytree_model::empty_tree().descriptor.key]);
        for item in view.prefix(&self.inner.spaces.refs, &prefix) {
            let (_, bytes) = item.into_inner()?;
            let r: StoredReference = decode(&bytes)?;
            if let Some(d) = r.commit {
                let expected = d.descriptor()?;
                if expected.key.kind != Kind::Commit {
                    return Err(Error::Corrupt("reference target type"));
                }
                self.inner.require_descriptor(repo, expected)?;
                roots.insert(expected.key);
                if roots.len() > self.inner.config.max_closure_objects {
                    return Err(Error::Limit("object closure"));
                }
            }
        }
        let mut retained = self.inner.closure(repo, roots, true, false)?;
        let mut expired = Vec::new();
        let mut stage_roots = HashSet::new();
        for item in view.prefix(&self.inner.spaces.staging, &prefix) {
            let (k, bytes) = item.into_inner()?;
            let stage: StageRecord = decode(&bytes)?;
            if stage.expires_at > now {
                let expected = stage.object.descriptor()?;
                self.inner.require_descriptor(repo, expected)?;
                stage_roots.insert(expected.key);
                if stage_roots.len() > self.inner.config.max_closure_objects {
                    return Err(Error::Limit("object closure"));
                }
            } else {
                expired.push(k);
            }
        }
        for ((r, key), _) in &gate.readers {
            if r == repo {
                stage_roots.insert(*key);
                if stage_roots.len() > self.inner.config.max_closure_objects {
                    return Err(Error::Limit("object closure"));
                }
            }
        }
        stage_roots.extend(cloud.transient.iter().copied());
        retained.extend(self.inner.closure(repo, stage_roots, false, false)?);
        let mut victims = Vec::new();
        for item in view.prefix(&self.inner.spaces.objects, &prefix) {
            let (k, bytes) = item.into_inner()?;
            let d = decode::<StoredDescriptor>(&bytes)?.descriptor()?;
            if &*k != object_key(repo, d.key).as_slice() {
                return Err(Error::Corrupt("object index key"));
            }
            if !retained.contains(&d.key) {
                victims.push((k, d));
            }
        }
        let mut repository = self.inner.repository(repo)?;
        let mut report = GcReport {
            expired_leases: expired.len() as u64,
            deleted_cloud_objects: cloud.objects,
            deleted_cloud_bytes: cloud.bytes,
            expired_cloud_leases: cloud.expired,
            ..Default::default()
        };
        for (_, d) in &victims {
            repository.bytes = repository
                .bytes
                .checked_sub(d.size_bytes)
                .ok_or(Error::Corrupt("repository byte count"))?;
            repository.objects = repository
                .objects
                .checked_sub(1)
                .ok_or(Error::Corrupt("repository object count"))?;
            report.deleted_objects += 1;
            report.deleted_bytes += d.size_bytes;
        }
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        for (k, d) in &victims {
            self.inner.remove_location(&mut tx, repo, d.key)?;
            tx.remove(&self.inner.spaces.objects, k.as_ref());
            tx.remove(&self.inner.spaces.client_ages, k.as_ref());
        }
        cloud.apply(&self.inner, repo, &mut tx)?;
        self.inner.advance_client_revision(&mut tx, repo)?;
        for k in expired {
            tx.remove(&self.inner.spaces.staging, k);
        }
        tx.insert(&self.inner.spaces.repos, repo, encode(&repository)?);
        self.inner.commit(tx)?;
        // Index removal is durable first. Unlink failure leaves an unindexed
        // orphan and is surfaced, never an indexed success with missing bytes.
        for (_, d) in victims {
            match fs::remove_file(self.inner.path(repo, d.key)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        cloud.unlink()?;
        self.inner.reclaim_empty_packs(repo)?;
        for kind in [
            Kind::File,
            Kind::Tree,
            Kind::Commit,
            Kind::Symlink,
            Kind::CopyHistory,
        ] {
            sync_dir(
                &self
                    .inner
                    .root
                    .join("repos")
                    .join(repo)
                    .join((kind as u8).to_string()),
            )?;
        }
        Ok(report)
    }
}

impl Inner {
    fn lock(&self) -> Result<MutexGuard<'_, Runtime>> {
        let guard = self.gate.lock().map_err(|_| Error::Poisoned)?;
        if self.uncertain_commit.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        Ok(guard)
    }
    fn commit(&self, tx: fjall::SingleWriterWriteTx<'_>) -> Result<()> {
        // A failed journal sync may already be visible in memory. Never turn
        // that uncertain write into a successful replay ACK in this process.
        // Reopening lets Fjall recover the durable atomic journal outcome.
        tx.commit().map_err(|error| {
            self.uncertain_commit.store(true, Ordering::Release);
            Error::Database(error)
        })
    }
    fn repository(&self, repo: &str) -> Result<RepositoryRecord> {
        name(repo)?;
        self.db
            .read_tx()
            .get(&self.spaces.repos, repo)?
            .map(|v| decode(&v))
            .transpose()?
            .ok_or(Error::UnknownRepository)
    }
    fn path(&self, repo: &str, object: ObjectKey) -> PathBuf {
        self.root
            .join("repos")
            .join(repo)
            .join((object.kind as u8).to_string())
            .join(hex(&object.id.0))
    }
    fn descriptor(&self, repo: &str, object: ObjectKey) -> Result<Option<Descriptor>> {
        let Some(bytes) = self
            .db
            .read_tx()
            .get(&self.spaces.objects, object_key(repo, object))?
        else {
            return Ok(None);
        };
        let d = decode::<StoredDescriptor>(&bytes)?.descriptor()?;
        if d.key != object {
            return Err(Error::Corrupt("object descriptor key"));
        }
        self.check_backing(repo, d)?;
        Ok(Some(d))
    }
    fn require_descriptor(&self, repo: &str, expected: Descriptor) -> Result<()> {
        if self.descriptor(repo, expected.key)? != Some(expected) {
            return Err(Error::Corrupt("root descriptor missing or changed"));
        }
        Ok(())
    }
    fn check_install_quota(&self, repo: &str, descriptor: Descriptor) -> Result<()> {
        let view = self.db.read_tx();
        if let Some(bytes) = view.get(&self.spaces.objects, object_key(repo, descriptor.key))? {
            if decode::<StoredDescriptor>(&bytes)?.descriptor()? != descriptor {
                return Err(Error::Corrupt("descriptor collision"));
            }
            return Ok(());
        }
        let record = self.repository(repo)?;
        let cloud = self.cloud_usage(repo)?;
        if record
            .bytes
            .checked_add(cloud.0)
            .and_then(|n| n.checked_add(descriptor.size_bytes))
            .is_none_or(|n| n > self.config.max_repository_bytes)
            || record
                .objects
                .checked_add(cloud.1)
                .and_then(|n| n.checked_add(1))
                .is_none_or(|n| n > self.config.max_objects_per_repository)
        {
            return Err(Error::Limit("repository quota"));
        }
        Ok(())
    }
    fn replay(
        &self,
        repo: &str,
        request: &PublishRequest,
        fingerprint: [u8; 32],
    ) -> Result<Option<Publication>> {
        let Some(bytes) = self.db.read_tx().get(
            &self.spaces.receipts,
            key(repo, request.idempotency_key.as_bytes()),
        )?
        else {
            return Ok(None);
        };
        let record: ReceiptRecord = decode(&bytes)?;
        if record.fingerprint != fingerprint {
            return Err(Error::IdempotencyConflict);
        }
        publication(&request.idempotency_key, record).map(Some)
    }
    fn reserve(self: &Arc<Self>, repo: &str, descriptor: Descriptor) -> Result<Reservation> {
        let mut gate = self.lock()?;
        let repository = self.repository(repo)?;
        let cloud = self.cloud_usage(repo)?;
        if descriptor.format_version != tinytree_model::FORMAT_VERSION {
            return Err(Error::Invalid("object format"));
        }
        let max = if descriptor.key.kind == Kind::File {
            self.config.max_file_bytes
        } else {
            self.config.model_limits.max_object_bytes as u64
        };
        if descriptor.size_bytes > max {
            return Err(Error::Limit("object size"));
        }
        let n = gate
            .staging_bytes
            .checked_add(descriptor.size_bytes)
            .ok_or(Error::Limit("staging byte budget"))?;
        if n > self.config.max_staging_bytes {
            return Err(Error::Limit("staging byte budget"));
        }
        if gate.staging_uploads >= self.config.max_staging_uploads {
            return Err(Error::Limit("staging upload budget"));
        }
        let existing = self
            .db
            .read_tx()
            .get(&self.spaces.objects, object_key(repo, descriptor.key))?;
        let (new_bytes, new_objects) = match existing {
            Some(bytes) => {
                if decode::<StoredDescriptor>(&bytes)?.descriptor()? != descriptor {
                    return Err(Error::Corrupt("descriptor collision"));
                }
                (0, 0)
            }
            None => (descriptor.size_bytes, 1),
        };
        let reserved = gate
            .repository_reservations
            .entry(repo.to_owned())
            .or_default();
        let bytes = reserved
            .0
            .checked_add(new_bytes)
            .ok_or(Error::Limit("repository staging bytes"))?;
        let objects = reserved
            .1
            .checked_add(new_objects)
            .ok_or(Error::Limit("repository staging objects"))?;
        if repository
            .bytes
            .checked_add(cloud.0)
            .and_then(|n| n.checked_add(bytes))
            .is_none_or(|v| v > self.config.max_repository_bytes)
            || repository
                .objects
                .checked_add(cloud.1)
                .and_then(|n| n.checked_add(objects))
                .is_none_or(|v| v > self.config.max_objects_per_repository)
        {
            return Err(Error::Limit("repository quota"));
        }
        *reserved = (bytes, objects);
        gate.staging_bytes = n;
        gate.staging_uploads += 1;
        Ok(Reservation {
            bytes: descriptor.size_bytes,
            repo: repo.to_owned(),
            new_bytes,
            new_objects,
            inner: Arc::downgrade(self),
        })
    }
}

fn verify_stream(
    file: &mut (impl Read + Seek),
    descriptor: Descriptor,
    limits: Limits,
) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut hash = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buffer = [0; 64 * 1024];
    let mut metadata = Vec::new();
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        size = size
            .checked_add(n as u64)
            .ok_or(Error::Limit("object length"))?;
        if size > descriptor.size_bytes {
            return Err(Error::Model(tinytree_model::Error::SizeMismatch));
        }
        hash.update(&buffer[..n]);
        if descriptor.key.kind != Kind::File {
            if size > limits.max_object_bytes as u64 {
                return Err(Error::Limit("metadata object size"));
            }
            metadata.extend_from_slice(&buffer[..n]);
        }
    }
    if size != descriptor.size_bytes {
        return Err(Error::Model(tinytree_model::Error::SizeMismatch));
    }
    if hash.finalize().as_bytes() != &descriptor.key.id.0 {
        return Err(Error::Model(tinytree_model::Error::HashMismatch));
    }
    if descriptor.key.kind != Kind::File {
        tinytree_model::verify(&descriptor, &metadata, limits)?;
    }
    Ok(())
}

fn reference(name: String, bytes: Option<&[u8]>) -> Result<Reference> {
    let stored: StoredReference = match bytes {
        Some(b) => decode(b)?,
        None => StoredReference {
            generation: 0,
            commit: None,
        },
    };
    let commit = stored.commit.map(|d| d.descriptor()).transpose()?;
    if bytes.is_some() && stored.generation == 0 {
        return Err(Error::Corrupt("stored reference generation"));
    }
    if commit.is_some_and(|d| d.key.kind != Kind::Commit) {
        return Err(Error::Corrupt("reference target type"));
    }
    Ok(Reference {
        name,
        generation: stored.generation,
        commit,
    })
}
fn publication(id: &str, record: ReceiptRecord) -> Result<Publication> {
    let bytes = encode(&record.reference)?;
    Ok(Publication {
        idempotency_key: id.to_owned(),
        reference: reference(record.reference_name, Some(&bytes))?,
    })
}
fn fingerprint(request: &PublishRequest) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-native-publication-1\0");
    for field in [&request.reference_name, &request.idempotency_key] {
        h.update(&(field.len() as u64).to_be_bytes());
        h.update(field.as_bytes());
    }
    h.update(&request.expected_generation.to_be_bytes());
    match request.new_commit {
        Some(id) => {
            h.update(&[1]);
            h.update(&id.0);
        }
        None => {
            h.update(&[0]);
        }
    }
    h.update(&(request.staging_leases.len() as u64).to_be_bytes());
    for lease in &request.staging_leases {
        h.update(&(lease.len() as u64).to_be_bytes());
        h.update(lease.as_bytes());
    }
    *h.finalize().as_bytes()
}

/// A lightweight process-local GC root. Unlike `ReadLease`, it owns no payload
/// reader or packed reconstruction buffer and does not verify object bytes.
pub struct RootReadGuard {
    descriptor: Descriptor,
    repo: String,
    inner: Arc<Inner>,
}
impl RootReadGuard {
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
}
impl Drop for RootReadGuard {
    fn drop(&mut self) {
        release_read_root(&self.inner, &self.repo, self.descriptor.key);
    }
}

pub struct ReadLease {
    file: pack::Backing,
    descriptor: Descriptor,
    repo: String,
    inner: Arc<Inner>,
}
impl ReadLease {
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
}
impl Read for ReadLease {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        release_read_root(&self.inner, &self.repo, self.descriptor.key);
    }
}
fn release_read_root(inner: &Inner, repo: &str, key: ObjectKey) {
    if let Ok(mut gate) = inner.gate.lock() {
        let k = (repo.to_owned(), key);
        if let Some(n) = gate.readers.get_mut(&k) {
            *n -= 1;
            if *n == 0 {
                gate.readers.remove(&k);
            }
        }
    }
}
struct Reservation {
    bytes: u64,
    repo: String,
    new_bytes: u64,
    new_objects: u64,
    inner: Weak<Inner>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            if let Ok(mut gate) = inner.gate.lock() {
                gate.staging_bytes = gate.staging_bytes.saturating_sub(self.bytes);
                gate.staging_uploads = gate.staging_uploads.saturating_sub(1);
                if let Some(reserved) = gate.repository_reservations.get_mut(&self.repo) {
                    reserved.0 = reserved.0.saturating_sub(self.new_bytes);
                    reserved.1 = reserved.1.saturating_sub(self.new_objects);
                    if *reserved == (0, 0) {
                        gate.repository_reservations.remove(&self.repo);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
