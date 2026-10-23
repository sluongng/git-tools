//! Durable admission/retention bookkeeping, never authoritative object storage.
use crate::{Config, hex, unhex};
use anyhow::{Context, Result, ensure};
use fjall::Readable;
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
pub(crate) struct Ledger {
    db: fjall::SingleWriterTxDatabase,
    meta: fjall::SingleWriterTxKeyspace,
    roots: fjall::SingleWriterTxKeyspace,
    pending: fjall::SingleWriterTxKeyspace,
    members: fjall::SingleWriterTxKeyspace,
    files: fjall::SingleWriterTxKeyspace,
    receipts: fjall::SingleWriterTxKeyspace,
    gate: Mutex<()>,
    config: Config,
    _owner: File,
    _directory: private_fs::PrivateDir,
    pub client: [u8; 16],
    pub binding: Binding,
}
const MARKER: &str = "admission.format";
const FORMAT: &[u8] = b"tinytree-native-admission-1\n";
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
            require_bytes(&path.join(MARKER), FORMAT)?;
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
        let roots = db.keyspace("roots", fjall::KeyspaceCreateOptions::default)?;
        let pending = db.keyspace("pending", fjall::KeyspaceCreateOptions::default)?;
        let members = db.keyspace("admissions", fjall::KeyspaceCreateOptions::default)?;
        let files = db.keyspace("derived_files", fjall::KeyspaceCreateOptions::default)?;
        let receipts = db.keyspace("receipts", fjall::KeyspaceCreateOptions::default)?;
        if !fresh {
            ensure!(
                db.read_tx().get(&meta, b"format")?.as_deref() == Some(FORMAT),
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
        if fresh {
            tx.insert(&meta, b"format", FORMAT);
            tx.insert(&meta, b"bytes", serde_json::to_vec(&0u64)?);
        }
        tx.insert(&meta, b"binding", serde_json::to_vec(&binding)?);
        tx.insert(&meta, b"client", client);
        tx.commit()?;
        if fresh {
            directory.replace(MARKER, FORMAT)?;
        }
        Ok(Self {
            db,
            meta,
            roots,
            pending,
            members,
            files,
            receipts,
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
    pub fn discard_pending(&self, id: &str) -> Result<()> {
        self.write(vec![(&self.pending, id.as_bytes().to_vec(), None)])
    }
    pub fn save_root(&self, root: &Root) -> Result<()> {
        if self.root(CommitId(unhex(&root.head)?))?.is_none() {
            let roots = self.roots()?;
            ensure!(
                roots.iter().filter(|root| !root.complete).count()
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
        if !root.complete {
            ensure!(
                self.roots()?.iter().filter(|root| root.complete).count()
                    < self.config.max_admitted_roots,
                "native admitted root budget exceeded"
            );
        }
        root.complete = true;
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
        tx.commit()?;
        Ok(())
    }
}
fn member_key(root: &str, key: ObjectKey) -> Vec<u8> {
    format!("{root}/{}/{:}", key.kind as u8, hex(&key.id.0)).into_bytes()
}
