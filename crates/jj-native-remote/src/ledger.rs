//! Durable admission/retention bookkeeping, never authoritative object storage.
use crate::{Config, hex, unhex};
mod cloud;
mod cloud_query;
mod query;
use anyhow::{Context, Result, ensure};
pub(crate) use cloud::CloudVersionAuthority;
pub(crate) use cloud_query::CloudQueryAuthority;
use fjall::Readable;
pub(crate) use query::{Authority, QueryAuthority};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Mutex,
};
use tinytree_model::{CommitId, Descriptor, Kind, ObjectId, ObjectKey};
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Binding {
    pub base: String,
    pub credential: String,
    pub scope: String,
    #[serde(default)]
    pub trust: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StoredDescriptor {
    pub kind: u8,
    pub id: String,
    pub size: u64,
}
impl StoredDescriptor {
    pub fn new(d: Descriptor) -> Self {
        Self {
            kind: d.key.kind as u8,
            id: hex(&d.key.id.0),
            size: d.size_bytes,
        }
    }
    pub fn descriptor(&self) -> Result<Descriptor> {
        Ok(Descriptor {
            key: ObjectKey {
                kind: Kind::try_from(self.kind)?,
                id: ObjectId(unhex(&self.id)?),
            },
            size_bytes: self.size,
            format_version: tinytree_model::FORMAT_VERSION,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Request {
    pub id: String,
    pub name: String,
    pub generation: u64,
    pub commit: Option<String>,
    pub leases: Vec<String>,
}
impl Request {
    pub fn wire(&self, repo: &str) -> Result<tinytree_client::protocol::PublishReferenceRequest> {
        Ok(tinytree_client::protocol::PublishReferenceRequest {
            repository_id: repo.into(),
            idempotency_key: self.id.clone(),
            reference_name: self.name.clone(),
            expected_generation: self.generation,
            new_commit: self
                .commit
                .as_ref()
                .map(|id| {
                    Ok::<_, anyhow::Error>(tinytree_client::protocol::ObjectKey {
                        kind: Kind::Commit as i32,
                        native_id: unhex::<32>(id)?.to_vec(),
                    })
                })
                .transpose()?,
            staging_leases: self.leases.clone(),
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Root {
    pub head: String,
    pub hold_name: String,
    pub request: Option<Request>,
    pub receipt: Option<Request>,
    pub receipt_binding: Option<String>,
    pub generation: Option<u64>,
    pub binding: String,
    pub complete: bool,
    /// Remote retention is independent of complete local metadata residency.
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub ready: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct FileClaim {
    pub size: u64,
    pub root: String,
    pub tree: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Receipt {
    pub request: Request,
    pub generation: u64,
    pub binding: String,
}
impl Root {
    pub fn retained(&self) -> bool {
        (self.pinned || self.complete)
            && self.generation.is_some_and(|generation| generation > 0)
            && self.request.is_none()
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct KnownReference {
    pub parent: Option<StoredDescriptor>,
}
#[derive(Default, Serialize, Deserialize)]
struct Progress {
    objects: usize,
    bytes: u64,
    edges: usize,
    known: usize,
}
pub(crate) struct Ledger {
    db: fjall::SingleWriterTxDatabase,
    meta: fjall::SingleWriterTxKeyspace,
    roots: fjall::SingleWriterTxKeyspace,
    pending: fjall::SingleWriterTxKeyspace,
    members: fjall::SingleWriterTxKeyspace,
    references: fjall::SingleWriterTxKeyspace,
    progress: fjall::SingleWriterTxKeyspace,
    files: fjall::SingleWriterTxKeyspace,
    receipts: fjall::SingleWriterTxKeyspace,
    query_authorities: fjall::SingleWriterTxKeyspace,
    query_grants: fjall::SingleWriterTxKeyspace,
    cloud_authorities: fjall::SingleWriterTxKeyspace,
    cloud_queries: fjall::SingleWriterTxKeyspace,
    poisoned: std::sync::atomic::AtomicBool,
    gate: Mutex<()>,
    config: Config,
    _owner: File,
    _directory: private_fs::PrivateDir,
    pub client: [u8; 16],
    pub binding: Binding,
}
const MARKER: &str = "admission.format";
const LEGACY_FORMAT: &[u8] = b"tinytree-native-admission-1\n";
const SPARSE_FORMAT: &[u8] = b"tinytree-native-admission-2\n";
const QUERY_FORMAT: &[u8] = b"tinytree-native-admission-3\n";
const FORMAT: &[u8] = b"tinytree-native-admission-4\n";
const CLOUD_QUERY_FORMAT: &[u8] = b"tinytree-native-admission-5\n";
pub(crate) fn preflight(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "native admission ledger root must be a real directory"
            );
            if std::fs::read_dir(path)?.next().is_none() {
                return Ok(true);
            }
            require_file(&path.join(MARKER))?;
            ensure!(
                std::fs::metadata(path.join(MARKER))?.len() <= FORMAT.len() as u64,
                "native admission marker exceeds its bound"
            );
            let marker = std::fs::read(path.join(MARKER))?;
            ensure!(
                marker == FORMAT
                    || marker == CLOUD_QUERY_FORMAT
                    || marker == QUERY_FORMAT
                    || marker == SPARSE_FORMAT
                    || marker == LEGACY_FORMAT,
                "unrecognized native admission ledger format"
            );
            require_file(&path.join("owner"))?;
            ensure!(
                std::fs::symlink_metadata(path.join("data"))?.is_dir(),
                "native admission ledger database missing"
            );
            require_bytes(&path.join("data/version"), b"FJL\x03")?;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                ensure!(
                    matches!(
                        entry.file_name().to_str(),
                        Some("admission.format" | "owner" | "data")
                    ),
                    "unrecognized native admission ledger contents"
                );
            }
            Ok(false)
        }
    }
}
fn require_file(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| anyhow::anyhow!("recognized native admission ledger files are missing"))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "native admission ledger file has invalid type"
    );
    Ok(())
}
fn require_bytes(path: &Path, expected: &[u8]) -> Result<()> {
    require_file(path)?;
    ensure!(
        std::fs::metadata(path)?.len() == expected.len() as u64 && std::fs::read(path)? == expected,
        "unrecognized native admission ledger format"
    );
    Ok(())
}
impl Ledger {
    fn check(&self) -> Result<()> {
        ensure!(
            !self.poisoned.load(std::sync::atomic::Ordering::Acquire),
            "native ledger requires reopening"
        );
        Ok(())
    }
    fn enable_cloud_queries(&self) -> Result<()> {
        self.check()?;
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        let format = self
            .db
            .read_tx()
            .get(&self.meta, b"format")?
            .context("native feature gate missing")?;
        if format.as_ref() == CLOUD_QUERY_FORMAT {
            return Ok(());
        }
        ensure!(
            format.as_ref() == FORMAT,
            "native cloud query feature gate unsupported"
        );
        ensure!(
            self.db.read_tx().iter(&self.cloud_queries).next().is_none(),
            "ungated native cloud query records"
        );
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        tx.insert(&self.meta, b"format", CLOUD_QUERY_FORMAT);
        if let Err(error) = tx.commit() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = self._directory.replace(MARKER, CLOUD_QUERY_FORMAT) {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }
    pub fn open(path: &Path, config: &Config, trust: &str) -> Result<Self> {
        let fresh = preflight(path)?;
        if fresh {
            if let Some(name) = &config.remote.token_env {
                ensure!(
                    std::env::var(name).is_ok(),
                    "cannot initialize native remote ledger without configured credentials"
                );
            }
        }
        let directory = crate::private_directory(path)?;
        let path = path.canonicalize()?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("owner"))?;
        fs2::FileExt::try_lock_exclusive(&owner)
            .context("native admission ledger is already owned")?;
        let db = fjall::SingleWriterTxDatabase::builder(path.join("data")).open()?;
        if !fresh {
            for name in [
                "meta",
                "roots",
                "pending",
                "admissions",
                "derived_files",
                "receipts",
            ] {
                ensure!(
                    db.keyspace_exists(name),
                    "recognized native admission ledger keyspace missing"
                );
            }
        }
        let meta = db.keyspace("meta", fjall::KeyspaceCreateOptions::default)?;
        if !fresh
            && matches!(db.read_tx().get(&meta, b"format")?.as_deref(), Some(value) if value == CLOUD_QUERY_FORMAT || value == FORMAT || value == QUERY_FORMAT || value == SPARSE_FORMAT)
        {
            ensure!(
                db.keyspace_exists("known-references-v2")
                    && db.keyspace_exists("verified-progress-v2"),
                "recognized sparse admission keyspace missing"
            );
        }
        if !fresh
            && matches!(db.read_tx().get(&meta,b"format")?.as_deref(),Some(value) if value==CLOUD_QUERY_FORMAT||value==FORMAT||value==QUERY_FORMAT)
        {
            ensure!(
                db.keyspace_exists("query-authorities-v3") && db.keyspace_exists("query-grants-v3"),
                "recognized query admission keyspace missing"
            );
        }
        if !fresh
            && matches!(db.read_tx().get(&meta, b"format")?.as_deref(), Some(value) if value == FORMAT || value == CLOUD_QUERY_FORMAT)
        {
            ensure!(
                db.keyspace_exists("cloud-authorities-v4"),
                "recognized cloud admission keyspace missing"
            );
        }
        let has_cloud_queries =
            db.read_tx().get(&meta, b"format")?.as_deref() == Some(CLOUD_QUERY_FORMAT);
        ensure!(
            fresh || !has_cloud_queries || db.keyspace_exists("cloud-query-authorities-v5"),
            "recognized cloud query keyspace missing"
        );
        let cloud_queries = db.keyspace(
            "cloud-query-authorities-v5",
            fjall::KeyspaceCreateOptions::default,
        )?;
        ensure!(
            has_cloud_queries || db.read_tx().iter(&cloud_queries).next().is_none(),
            "cloud query feature gate is missing"
        );
        let cloud_authorities = db.keyspace(
            "cloud-authorities-v4",
            fjall::KeyspaceCreateOptions::default,
        )?;
        let query_authorities = db.keyspace(
            "query-authorities-v3",
            fjall::KeyspaceCreateOptions::default,
        )?;
        let query_grants = db.keyspace("query-grants-v3", fjall::KeyspaceCreateOptions::default)?;
        let roots = db.keyspace("roots", fjall::KeyspaceCreateOptions::default)?;
        let pending = db.keyspace("pending", fjall::KeyspaceCreateOptions::default)?;
        let members = db.keyspace("admissions", fjall::KeyspaceCreateOptions::default)?;
        let files = db.keyspace("derived_files", fjall::KeyspaceCreateOptions::default)?;
        let receipts = db.keyspace("receipts", fjall::KeyspaceCreateOptions::default)?;
        let references =
            db.keyspace("known-references-v2", fjall::KeyspaceCreateOptions::default)?;
        let progress = db.keyspace(
            "verified-progress-v2",
            fjall::KeyspaceCreateOptions::default,
        )?;
        if !fresh {
            let stored_format = db.read_tx().get(&meta, b"format")?;
            ensure!(
                matches!(stored_format.as_deref(), Some(value) if value == CLOUD_QUERY_FORMAT || value == FORMAT || value == QUERY_FORMAT || value == SPARSE_FORMAT || value == LEGACY_FORMAT),
                "native admission ledger format identity missing"
            );
            let counter = db
                .read_tx()
                .get(&meta, b"bytes")?
                .context("native admission ledger byte counter missing")?;
            ensure!(
                counter.len() <= 20,
                "native admission ledger byte counter corrupt"
            );
            let counter: u64 = serde_json::from_slice(&counter)?;
            ensure!(
                counter <= config.max_ledger_bytes as u64,
                "native admission ledger exceeds configured byte budget"
            );
        }
        let existing: Option<Binding> = db
            .read_tx()
            .get(&meta, b"binding")?
            .map(|v| serde_json::from_slice(&v))
            .transpose()?;
        ensure!(
            fresh || existing.is_some(),
            "native admission ledger authorization identity missing"
        );
        let endpoint = config.remote.endpoint.trim_end_matches('/');
        let base = blake3::hash(
            serde_json::to_vec(&(
                endpoint,
                &config.remote.repository_id,
                &config.authorization_profile,
            ))?
            .as_slice(),
        )
        .to_hex()
        .to_string();
        if let Some(existing) = &existing {
            ensure!(
                existing.base == base,
                "native ledger belongs to another repository or authorization profile"
            );
        }
        let credential = match &config.remote.token_env {
            None => Some("anonymous".to_owned()),
            Some(name) => std::env::var(name)
                .ok()
                .map(|value| blake3::hash(value.as_bytes()).to_hex().to_string()),
        };
        let binding = match credential {
            Some(credential) => {
                let scope =
                    blake3::hash(serde_json::to_vec(&(&base, &credential, trust))?.as_slice())
                        .to_hex()
                        .to_string();
                Binding {
                    base,
                    credential,
                    scope,
                    trust: trust.into(),
                }
            }
            None => existing
                .context("cannot initialize native remote ledger without configured credentials")?,
        };
        let client = if let Some(value) = db.read_tx().get(&meta, b"client")? {
            value
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid durable native client identity"))?
        } else {
            ensure!(fresh, "native admission ledger client identity missing");
            *uuid::Uuid::new_v4().as_bytes()
        };
        ensure!(
            client != [0; 16],
            "invalid native admission ledger client identity"
        );
        let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
        // The durable database feature gate precedes the marker replacement.
        // Old readers reject the database even after a crash between these writes.
        let format = if has_cloud_queries {
            CLOUD_QUERY_FORMAT
        } else {
            FORMAT
        };
        tx.insert(&meta, b"format", format);
        if fresh {
            tx.insert(&meta, b"bytes", serde_json::to_vec(&0u64)?);
        }
        tx.insert(&meta, b"binding", serde_json::to_vec(&binding)?);
        tx.insert(&meta, b"client", client);
        tx.commit()?;
        directory.replace(MARKER, format)?;
        Ok(Self {
            db,
            meta,
            roots,
            pending,
            members,
            references,
            progress,
            files,
            receipts,
            query_authorities,
            query_grants,
            cloud_authorities,
            cloud_queries,
            poisoned: std::sync::atomic::AtomicBool::new(false),
            gate: Mutex::new(()),
            config: config.clone(),
            _owner: owner,
            _directory: directory,
            client,
            binding,
        })
    }
    pub fn root(&self, head: CommitId) -> Result<Option<Root>> {
        self.get(&self.roots, hex(&head.0).as_bytes())
    }
    pub fn roots(&self) -> Result<Vec<Root>> {
        self.list(
            &self.roots,
            self.config.max_admitted_roots + self.config.max_pending_operations,
        )
    }
    pub fn pending_request(&self, id: &str) -> Result<Option<Request>> {
        self.get(&self.pending, id.as_bytes())
    }
    pub fn pending(&self) -> Result<Vec<Request>> {
        self.list(&self.pending, self.config.max_pending_operations)
    }
    pub fn claim(&self, id: [u8; 32]) -> Result<Option<FileClaim>> {
        self.get(&self.files, hex(&id).as_bytes())
    }
    pub fn member(&self, root: &str, key: ObjectKey) -> Result<bool> {
        let Some(value) = self
            .db
            .read_tx()
            .get(&self.members, member_key(root, key))?
        else {
            return Ok(false);
        };
        let descriptor: StoredDescriptor = serde_json::from_slice(&value)?;
        ensure!(
            descriptor.descriptor()?.key == key,
            "native metadata admission membership is corrupt"
        );
        Ok(true)
    }
    pub fn pin(&self, mut root: Root) -> Result<()> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        ensure!(
            root.generation.is_some_and(|generation| generation > 0) && root.request.is_none(),
            "native pin lacks a current retention proof"
        );
        if !self
            .root(CommitId(unhex(&root.head)?))?
            .is_some_and(|value| value.retained())
        {
            ensure!(
                self.roots()?
                    .iter()
                    .filter(|value| value.retained())
                    .count()
                    < self.config.max_admitted_roots,
                "native admitted root budget exceeded"
            );
        }
        let key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(unhex(&root.head)?),
        };
        if root.pinned && !root.complete && self.reference(&root.head, key)?.is_some() {
            ensure!(
                self.get::<Progress>(&self.progress, root.head.as_bytes())?
                    .is_some(),
                "native sparse admission progress missing"
            );
        }
        root.pinned = true;
        let key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(unhex(&root.head)?),
        };
        let mut changes = vec![(
            &self.roots,
            root.head.as_bytes().to_vec(),
            Some(serde_json::to_vec(&root)?),
        )];
        if self
            .get::<KnownReference>(&self.references, member_key(&root.head, key))?
            .is_none()
        {
            let mut progress: Progress = self
                .get(&self.progress, root.head.as_bytes())?
                .unwrap_or_default();
            progress.known = progress
                .known
                .checked_add(1)
                .context("native known-reference count overflow")?;
            ensure!(
                progress.known <= self.config.closure.max_edges,
                "native known-reference budget exceeded"
            );
            changes.push((
                &self.references,
                member_key(&root.head, key),
                Some(serde_json::to_vec(&KnownReference { parent: None })?),
            ));
            changes.push((
                &self.progress,
                root.head.as_bytes().to_vec(),
                Some(serde_json::to_vec(&progress)?),
            ));
        }
        self.write_locked(changes)
    }
    /// A known reference is not residency: its canonical parent is rechecked by the provider.
    pub fn reference(&self, root: &str, key: ObjectKey) -> Result<Option<KnownReference>> {
        self.get(&self.references, member_key(root, key))
    }
    pub fn metadata_root(&self, key: ObjectKey) -> Result<Option<Root>> {
        for root in self.roots()? {
            if root.retained()
                && (self.reference(&root.head, key)?.is_some()
                    || (root.complete && self.member(&root.head, key)?))
            {
                return Ok(Some(root));
            }
        }
        Ok(None)
    }
    pub fn verified_members(&self, root: &str) -> Result<Vec<Descriptor>> {
        let mut result = Vec::new();
        for item in self
            .db
            .read_tx()
            .prefix(&self.members, format!("{root}/").as_bytes())
        {
            ensure!(
                result.len() < self.config.closure.max_objects,
                "native verified metadata count exceeded"
            );
            let (key, value) = item.into_inner()?;
            let descriptor = serde_json::from_slice::<StoredDescriptor>(&value)?.descriptor()?;
            ensure!(
                key.as_ref() == member_key(root, descriptor.key).as_slice(),
                "native verified metadata index is corrupt"
            );
            result.push(descriptor);
        }
        Ok(result)
    }
    pub fn check_verified_batch(
        &self,
        head: CommitId,
        nodes: &[(Descriptor, tinytree_model::Object)],
    ) -> Result<()> {
        let root = self
            .root(head)?
            .context("native metadata root disappeared")?;
        ensure!(root.retained(), "native metadata root is not pinned");
        let progress: Option<Progress> = self.get(&self.progress, root.head.as_bytes())?;
        ensure!(
            root.complete || progress.is_some(),
            "native sparse admission progress missing"
        );
        let mut progress = progress.unwrap_or_default();
        let mut known = std::collections::HashSet::new();
        for (descriptor, object) in nodes {
            if self.member(&root.head, descriptor.key)? {
                continue;
            }
            progress.objects = progress
                .objects
                .checked_add(1)
                .context("native metadata count overflow")?;
            progress.bytes = progress
                .bytes
                .checked_add(descriptor.size_bytes)
                .context("native metadata bytes overflow")?;
            let refs = object.references();
            progress.edges = progress
                .edges
                .checked_add(refs.len())
                .context("native metadata edges overflow")?;
            for key in refs.into_iter().filter(|key| key.kind != Kind::File) {
                if known.insert(key) && self.reference(&root.head, key)?.is_none() {
                    progress.known = progress
                        .known
                        .checked_add(1)
                        .context("native known-reference count overflow")?;
                }
            }
        }
        ensure!(
            progress.objects <= self.config.closure.max_objects
                && progress.bytes <= self.config.closure.max_metadata_bytes as u64
                && progress.edges <= self.config.closure.max_edges
                && progress.known <= self.config.closure.max_edges,
            "native verified metadata budget exceeded"
        );
        Ok(())
    }
    /// Publish verified bytes, their outgoing references and exact FILE claims atomically.
    pub fn verified(
        &self,
        head: CommitId,
        descriptor: Descriptor,
        object: &tinytree_model::Object,
    ) -> Result<()> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        let root = self
            .root(head)?
            .context("native metadata root disappeared")?;
        ensure!(root.retained(), "native metadata root is not pinned");
        if let Some(previous) =
            self.get::<StoredDescriptor>(&self.members, member_key(&root.head, descriptor.key))?
        {
            ensure!(
                previous.descriptor()? == descriptor,
                "native verified metadata descriptor changed"
            );
            return Ok(());
        }
        ensure!(
            self.reference(&root.head, descriptor.key)?.is_some(),
            "native metadata has no known reference"
        );
        let progress: Option<Progress> = self.get(&self.progress, root.head.as_bytes())?;
        ensure!(
            root.complete || progress.is_some(),
            "native sparse admission progress missing"
        );
        let mut progress = progress.unwrap_or_default();
        progress.objects = progress
            .objects
            .checked_add(1)
            .context("native metadata count overflow")?;
        progress.bytes = progress
            .bytes
            .checked_add(descriptor.size_bytes)
            .context("native metadata bytes overflow")?;
        let refs = object.references();
        progress.edges = progress
            .edges
            .checked_add(refs.len())
            .context("native metadata edges overflow")?;
        ensure!(
            progress.objects <= self.config.closure.max_objects
                && progress.bytes <= self.config.closure.max_metadata_bytes as u64
                && progress.edges <= self.config.closure.max_edges,
            "native verified metadata budget exceeded"
        );
        let parent = StoredDescriptor::new(descriptor);
        let mut changes = vec![(
            &self.members,
            member_key(&root.head, descriptor.key),
            Some(serde_json::to_vec(&parent)?),
        )];
        let mut outgoing = std::collections::HashSet::new();
        for key in refs.into_iter().filter(|key| key.kind != Kind::File) {
            if !outgoing.insert(key) {
                continue;
            }
            if self.reference(&root.head, key)?.is_none() {
                progress.known = progress
                    .known
                    .checked_add(1)
                    .context("native known-reference count overflow")?;
                ensure!(
                    progress.known <= self.config.closure.max_edges,
                    "native known-reference budget exceeded"
                );
                changes.push((
                    &self.references,
                    member_key(&root.head, key),
                    Some(serde_json::to_vec(&KnownReference {
                        parent: Some(parent.clone()),
                    })?),
                ));
            }
        }
        if let tinytree_model::Object::Tree(tree) = object {
            let mut count = 0usize;
            for item in self.db.read_tx().iter(&self.files) {
                item.into_inner()?;
                count += 1;
            }
            let mut files = std::collections::HashMap::new();
            for entry in &tree.entries {
                if let tinytree_model::TreeValue::File { id, size_bytes, .. } = entry.value {
                    if let Some(previous) = files.insert(id, size_bytes) {
                        ensure!(
                            previous == size_bytes,
                            "conflicting admitted native file lengths"
                        );
                        continue;
                    }
                    if let Some(existing) = self.claim(id.0)? {
                        ensure!(
                            existing.size == size_bytes,
                            "conflicting admitted native file lengths"
                        );
                    } else {
                        count += 1;
                    }
                    ensure!(
                        count <= self.config.max_file_index_entries,
                        "native derived file index count exceeded"
                    );
                    changes.push((
                        &self.files,
                        hex(&id.0).into_bytes(),
                        Some(serde_json::to_vec(&FileClaim {
                            size: size_bytes,
                            root: root.head.clone(),
                            tree: hex(&descriptor.key.id.0),
                        })?),
                    ));
                }
            }
        }
        changes.push((
            &self.progress,
            root.head.as_bytes().to_vec(),
            Some(serde_json::to_vec(&progress)?),
        ));
        self.write_locked(changes)
    }
    pub fn discard_pending(&self, id: &str) -> Result<()> {
        self.write(vec![(&self.pending, id.as_bytes().to_vec(), None)])
    }
    pub fn save_root(&self, root: &Root) -> Result<()> {
        if self.root(CommitId(unhex(&root.head)?))?.is_none() {
            let roots = self.roots()?;
            ensure!(
                roots.iter().filter(|root| !root.retained()).count()
                    < self.config.max_pending_operations,
                "native pending hold budget exceeded"
            );
        }

        self.write(vec![(
            &self.roots,
            root.head.as_bytes().to_vec(),
            Some(serde_json::to_vec(root)?),
        )])
    }
    pub fn save_pending(&self, request: &Request) -> Result<()> {
        if let Some(receipt) = self.receipt(&request.id)? {
            ensure!(
                serde_json::to_vec(&receipt.request)? == serde_json::to_vec(request)?,
                "native completed publication key has another request"
            );
        }
        if self
            .get::<Request>(&self.pending, request.id.as_bytes())?
            .is_none()
        {
            ensure!(
                self.pending()?.len() < self.config.max_pending_operations,
                "native pending publication budget exceeded"
            );
        }

        if let Some(existing) = self.get::<Request>(&self.pending, request.id.as_bytes())? {
            ensure!(
                serde_json::to_vec(&existing)? == serde_json::to_vec(request)?,
                "native pending publication key has another request"
            );
        }
        self.write(vec![(
            &self.pending,
            request.id.as_bytes().to_vec(),
            Some(serde_json::to_vec(request)?),
        )])
    }
    pub fn completed(
        &self,
        mut root: Root,
        members: &[Descriptor],
        files: &[(ObjectId, u64, ObjectId)],
    ) -> Result<()> {
        if !root.retained() {
            ensure!(
                self.roots()?.iter().filter(|root| root.retained()).count()
                    < self.config.max_admitted_roots,
                "native admitted root budget exceeded"
            );
        }
        root.complete = true;
        root.pinned = true;
        let mut changes = Vec::new();
        for descriptor in members {
            changes.push((
                &self.members,
                member_key(&root.head, descriptor.key),
                Some(serde_json::to_vec(&StoredDescriptor::new(*descriptor))?),
            ));
        }
        let mut index_ids = std::collections::HashSet::new();
        for item in self.db.read_tx().iter(&self.files) {
            let (key, _) = item.into_inner()?;
            index_ids.insert(key.to_vec());
            ensure!(
                index_ids.len() <= self.config.max_file_index_entries,
                "native derived file index count exceeded"
            );
        }
        for (id, size, tree) in files {
            index_ids.insert(hex(&id.0).into_bytes());
            ensure!(
                index_ids.len() <= self.config.max_file_index_entries,
                "native derived file index count exceeded"
            );
            if let Some(existing) = self.claim(id.0)? {
                ensure!(
                    existing.size == *size,
                    "conflicting admitted native file lengths"
                );
            }
            changes.push((
                &self.files,
                hex(&id.0).into_bytes(),
                Some(serde_json::to_vec(&FileClaim {
                    size: *size,
                    root: root.head.clone(),
                    tree: hex(&tree.0),
                })?),
            ));
        }
        changes.push((
            &self.roots,
            root.head.as_bytes().to_vec(),
            Some(serde_json::to_vec(&root)?),
        ));
        self.write(changes)
    }
    pub fn replace_file_index(&self, claims: Vec<(ObjectId, FileClaim)>) -> Result<()> {
        ensure!(
            claims.len() <= self.config.max_file_index_entries,
            "native derived file index count exceeded"
        );
        let mut changes = Vec::new();
        for (count, item) in self.db.read_tx().iter(&self.files).enumerate() {
            ensure!(
                count < self.config.max_file_index_entries,
                "native derived file index count exceeded"
            );
            let (key, _) = item.into_inner()?;
            changes.push((&self.files, key.to_vec(), None));
        }
        for (id, claim) in claims {
            changes.push((
                &self.files,
                hex(&id.0).into_bytes(),
                Some(serde_json::to_vec(&claim)?),
            ));
        }
        self.write(changes)
    }
    pub fn finish_publication(&self, request: &Request, generation: u64) -> Result<()> {
        self.write(vec![
            (&self.pending, request.id.as_bytes().to_vec(), None),
            (
                &self.receipts,
                request.id.as_bytes().to_vec(),
                Some(serde_json::to_vec(&Receipt {
                    request: request.clone(),
                    generation,
                    binding: self.binding.scope.clone(),
                })?),
            ),
        ])
    }
    pub fn receipt(&self, id: &str) -> Result<Option<Receipt>> {
        self.get(&self.receipts, id.as_bytes())
    }
    fn get<T: serde::de::DeserializeOwned>(
        &self,
        space: &fjall::SingleWriterTxKeyspace,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.check()?;
        self.db
            .read_tx()
            .get(space, key)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }
    fn list<T: serde::de::DeserializeOwned>(
        &self,
        space: &fjall::SingleWriterTxKeyspace,
        max: usize,
    ) -> Result<Vec<T>> {
        self.check()?;
        let mut values = Vec::new();
        for item in self.db.read_tx().iter(space) {
            ensure!(
                values.len() < max,
                "native ledger record count exceeds budget"
            );
            let (_, bytes) = item.into_inner()?;
            values.push(serde_json::from_slice(&bytes)?);
        }
        Ok(values)
    }
    fn write(
        &self,
        changes: Vec<(&fjall::SingleWriterTxKeyspace, Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<()> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.write_locked(changes)
    }
    fn write_locked(
        &self,
        changes: Vec<(&fjall::SingleWriterTxKeyspace, Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<()> {
        self.check()?;
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut bytes: u64 = tx
            .get(&self.meta, b"bytes")?
            .map(|v| serde_json::from_slice(&v))
            .transpose()?
            .unwrap_or(0);
        for (space, key, value) in changes {
            if let Some(old) = tx.get(space, &key)? {
                bytes = bytes
                    .checked_sub((key.len() + old.len()) as u64)
                    .context("native ledger byte counter corrupt")?;
            }
            if let Some(value) = value {
                bytes = bytes
                    .checked_add((key.len() + value.len()) as u64)
                    .context("native ledger exceeds byte budget")?;
                ensure!(
                    bytes <= self.config.max_ledger_bytes as u64,
                    "native admission ledger byte budget exceeded"
                );
                tx.insert(space, key, value);
            } else {
                tx.remove(space, key);
            }
        }
        tx.insert(&self.meta, b"bytes", serde_json::to_vec(&bytes)?);
        if let Err(error) = tx.commit() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }
}
fn member_key(root: &str, key: ObjectKey) -> Vec<u8> {
    format!("{root}/{}/{:}", key.kind as u8, hex(&key.id.0)).into_bytes()
}
