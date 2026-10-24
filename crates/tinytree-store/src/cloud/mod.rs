//! Private authoritative operation/view catalogs. Upload is not a retained version.
use super::*;
use tinytree_cloud::{BlobDigest, Dependencies, ObjectId as CloudId, VerifiedObject};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CloudOwner {
    pub principal: [u8; 32],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudObjectDescriptor {
    pub id: CloudId,
    pub selected: BlobDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudWriteReceipt {
    pub descriptor: CloudObjectDescriptor,
    pub staging_lease: String,
    pub expires_at: i64,
}
#[derive(Clone, Debug)]
pub struct CloudConfig {
    pub max_object_bytes: usize,
    pub index_artifacts: tinytree_cloud::IndexArtifactLimits,
    pub manifest_limits: tinytree_cloud::ManifestLimits,
    pub max_version_decode_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_fields: usize,
    pub max_field_bytes: usize,
    pub max_validation_payload_bytes: u64,
    pub max_objects_per_owner: u64,
    pub max_blob_bytes_per_owner: u64,
    pub max_metadata_bytes_per_owner: u64,
    pub max_staging_records_per_owner: u64,
    pub max_staging_bytes_per_owner: u64,
    pub max_workspaces_per_owner: u64,
    pub max_versions_per_workspace: u64,
    pub max_receipts_per_owner: u64,
}
impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            max_object_bytes: 4 * 1024 * 1024,
            index_artifacts: Default::default(),
            manifest_limits: tinytree_cloud::ManifestLimits::default(),
            max_version_decode_bytes: 512 * 1024 * 1024,
            max_decode_bytes: 64 * 1024 * 1024,
            max_fields: 100_000,
            max_field_bytes: 1024 * 1024,
            max_validation_payload_bytes: 16 * 1024 * 1024 * 1024,
            max_objects_per_owner: 100_000,
            max_blob_bytes_per_owner: 8 * 1024 * 1024 * 1024,
            max_metadata_bytes_per_owner: 128 * 1024 * 1024,
            max_staging_records_per_owner: 100_000,
            max_staging_bytes_per_owner: 128 * 1024 * 1024,
            max_workspaces_per_owner: 1024,
            max_versions_per_workspace: 1024,
            max_receipts_per_owner: 100_000,
        }
    }
}
impl CloudConfig {
    pub(super) fn validate(&self) -> Result<()> {
        self.index_artifacts
            .validate()
            .map_err(|_| Error::Invalid("cloud index artifact limits"))?;
        self.manifest_limits
            .validate()
            .map_err(|_| Error::Invalid("cloud manifest limits"))?;
        if !(1024..=64 * 1024 * 1024).contains(&self.max_object_bytes)
            || self.max_decode_bytes < self.max_object_bytes
            || self.max_decode_bytes > 1024 * 1024 * 1024
            || !(1..=1_000_000).contains(&self.max_fields)
            || self.max_field_bytes == 0
            || self.max_field_bytes > self.max_object_bytes
            || !(65536..=1024 * 1024 * 1024).contains(&self.max_version_decode_bytes)
            || self.max_validation_payload_bytes == 0
            || !(1..=1_000_000).contains(&self.max_objects_per_owner)
            || self.max_blob_bytes_per_owner == 0
            || self.max_metadata_bytes_per_owner == 0
            || !(1..=1_000_000).contains(&self.max_staging_records_per_owner)
            || self.max_staging_bytes_per_owner == 0
            || !(1..=1_000_000).contains(&self.max_workspaces_per_owner)
            || !(1..=1_000_000).contains(&self.max_versions_per_workspace)
            || !(1..=10_000_000).contains(&self.max_receipts_per_owner)
        {
            return Err(Error::Invalid("cloud storage bounds"));
        }
        Ok(())
    }
}
// Cloud dependency summaries are larger than ordinary native catalog rows.
// Keep read and precommit bounds identical; a durable ACK must remain readable.
const CLOUD_JSON_ROW_BYTES: usize = 1024 * 1024;
const CLOUD_BINARY_ROW_BYTES: usize = 64 * 1024 * 1024 + 4096;
mod native_read;
pub use native_read::{CloudNativeReadLease, CloudNativeReadLimits, CloudVersionSelector};
mod resources;
pub use resources::CloudResourceProfile;
mod artifacts;
pub use artifacts::{CloudIndexReadLease, CloudIndexWriteReceipt};
mod versions;
pub use versions::{
    CloudPublication, CloudPublicationLimits, CloudSnapshotCommit, CloudSnapshotReadGuard,
    CloudVersion, CloudVersionReadGuard, CloudWorkspace, PublishCloudRequest,
};
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > CLOUD_JSON_ROW_BYTES {
        return Err(Error::Corrupt("cloud metadata row size"));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt("cloud metadata encoding"))
}
const TABLES: [&str; 11] = [
    "cloud-objects-v1",
    "cloud-blobs-v1",
    "cloud-staging-v1",
    "cloud-manifests-v1",
    "cloud-workspaces-v1",
    "cloud-versions-v1",
    "cloud-native-roots-v1",
    "cloud-receipts-v1",
    "cloud-accounting-v1",
    "cloud-index-artifacts-v1",
    "cloud-index-staging-v1",
];
pub(super) struct Spaces {
    objects: SingleWriterTxKeyspace,
    blobs: SingleWriterTxKeyspace,
    staging: SingleWriterTxKeyspace,
    manifests: SingleWriterTxKeyspace,
    workspaces: SingleWriterTxKeyspace,
    versions: SingleWriterTxKeyspace,
    roots: SingleWriterTxKeyspace,
    receipts: SingleWriterTxKeyspace,
    accounting: SingleWriterTxKeyspace,
    index_artifacts: SingleWriterTxKeyspace,
    index_staging: SingleWriterTxKeyspace,
}
impl Spaces {
    pub(super) fn open(db: &SingleWriterTxDatabase, feature: u8) -> Result<Self> {
        if feature >= 3 && TABLES[..9].iter().any(|name| !db.keyspace_exists(name)) {
            return Err(Error::Corrupt("cloud keyspace missing"));
        }
        if feature >= 4 && TABLES[9..].iter().any(|name| !db.keyspace_exists(name)) {
            return Err(Error::Corrupt("cloud index keyspace missing"));
        }
        Ok(Self {
            objects: db.keyspace(TABLES[0], KeyspaceCreateOptions::default)?,
            blobs: db.keyspace(TABLES[1], KeyspaceCreateOptions::default)?,
            staging: db.keyspace(TABLES[2], KeyspaceCreateOptions::default)?,
            manifests: db.keyspace(TABLES[3], KeyspaceCreateOptions::default)?,
            workspaces: db.keyspace(TABLES[4], KeyspaceCreateOptions::default)?,
            versions: db.keyspace(TABLES[5], KeyspaceCreateOptions::default)?,
            roots: db.keyspace(TABLES[6], KeyspaceCreateOptions::default)?,
            receipts: db.keyspace(TABLES[7], KeyspaceCreateOptions::default)?,
            accounting: db.keyspace(TABLES[8], KeyspaceCreateOptions::default)?,
            index_artifacts: db.keyspace(TABLES[9], KeyspaceCreateOptions::default)?,
            index_staging: db.keyspace(TABLES[10], KeyspaceCreateOptions::default)?,
        })
    }
    fn all(&self) -> [&SingleWriterTxKeyspace; 11] {
        [
            &self.objects,
            &self.blobs,
            &self.staging,
            &self.manifests,
            &self.workspaces,
            &self.versions,
            &self.roots,
            &self.receipts,
            &self.accounting,
            &self.index_artifacts,
            &self.index_staging,
        ]
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selected {
    kind: u8,
    id: Vec<u8>,
    digest: [u8; 32],
    size: u64,
}
impl Selected {
    fn new(d: CloudObjectDescriptor) -> Self {
        Self {
            kind: match d.id {
                CloudId::Operation(_) => 1,
                CloudId::View(_) => 2,
            },
            id: d.id.bytes().to_vec(),
            digest: d.selected.blake3,
            size: d.selected.size_bytes,
        }
    }
    fn descriptor(&self) -> Result<CloudObjectDescriptor> {
        let id: [u8; 64] = self
            .id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Corrupt("cloud semantic identity width"))?;
        let id = match self.kind {
            1 => CloudId::Operation(id),
            2 => CloudId::View(id),
            _ => return Err(Error::Corrupt("cloud semantic kind")),
        };
        if id.is_intrinsic() {
            return Err(Error::Corrupt("stored intrinsic cloud object"));
        }
        Ok(CloudObjectDescriptor {
            id,
            selected: BlobDigest {
                blake3: self.digest,
                size_bytes: self.size,
            },
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDependencies {
    operations: Vec<Vec<u8>>,
    views: Vec<Vec<u8>>,
    native_commits: Vec<[u8; 32]>,
}
impl StoredDependencies {
    fn new(d: &Dependencies) -> Self {
        Self {
            operations: d.operations.iter().map(|id| id.to_vec()).collect(),
            views: d.views.iter().map(|id| id.to_vec()).collect(),
            native_commits: d.native_commits.clone(),
        }
    }
    fn decoded(&self) -> Result<Dependencies> {
        let ids = |values: &[Vec<u8>]| {
            values
                .iter()
                .map(|id| {
                    id.as_slice()
                        .try_into()
                        .map_err(|_| Error::Corrupt("cloud dependency identity"))
                })
                .collect::<Result<Vec<_>>>()
        };
        Ok(Dependencies {
            operations: ids(&self.operations)?,
            views: ids(&self.views)?,
            native_commits: self.native_commits.clone(),
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRecord {
    version: u8,
    selected: Selected,
    dependencies: StoredDependencies,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobRecord {
    size: u64,
    catalog_refs: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StageRecord {
    selected: Selected,
    expires: i64,
}
#[derive(Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Accounting {
    objects: u64,
    blob_bytes: u64,
    selected_bytes: u64,
    #[serde(default)]
    version_payload_bytes: u64,
    #[serde(default)]
    version_metadata_bytes: u64,
    #[serde(default)]
    version_records: u64,
    metadata_bytes: u64,
    records: u64,
    namespaces: u64,
    stages: u64,
    workspaces: u64,
    versions: u64,
    receipts: u64,
}
fn repo_prefix(repo: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(repo.len() + 2);
    key.extend_from_slice(&(repo.len() as u16).to_be_bytes());
    key.extend_from_slice(repo.as_bytes());
    key
}
fn scope(repo: &str, owner: CloudOwner) -> Vec<u8> {
    let mut key = repo_prefix(repo);
    key.extend_from_slice(&owner.principal);
    key
}
fn object_key(repo: &str, owner: CloudOwner, id: CloudId) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.push(match id {
        CloudId::Operation(_) => 1,
        CloudId::View(_) => 2,
    });
    key.extend_from_slice(id.bytes());
    key
}
fn blob_key(repo: &str, owner: CloudOwner, digest: [u8; 32]) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.extend_from_slice(&digest);
    key
}
fn stage_id(d: CloudObjectDescriptor) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"native-cloud-upload-v1\0");
    h.update(&[match d.id {
        CloudId::Operation(_) => 1,
        CloudId::View(_) => 2,
    }]);
    h.update(d.id.bytes());
    h.update(&d.selected.blake3);
    hex(h.finalize().as_bytes())
}
fn parse_scope(key: &[u8]) -> Result<(&str, CloudOwner, &[u8])> {
    let size = u16::from_be_bytes(
        key.get(..2)
            .ok_or(Error::Corrupt("cloud key prefix"))?
            .try_into()
            .unwrap(),
    ) as usize;
    let repo = std::str::from_utf8(
        key.get(2..2 + size)
            .ok_or(Error::Corrupt("cloud repository key"))?,
    )
    .map_err(|_| Error::Corrupt("cloud repository key"))?;
    name(repo)?;
    let owner = CloudOwner {
        principal: key
            .get(2 + size..2 + size + 32)
            .ok_or(Error::Corrupt("cloud owner key"))?
            .try_into()
            .unwrap(),
    };
    Ok((repo, owner, &key[2 + size + 32..]))
}

impl Inner {
    fn cloud_object(
        &self,
        repo: &str,
        owner: CloudOwner,
        id: CloudId,
    ) -> Result<Option<ObjectRecord>> {
        self.db
            .read_tx()
            .get(&self.spaces.cloud.objects, object_key(repo, owner, id))?
            .map(|bytes| {
                let record: ObjectRecord = decode(&bytes)?;
                if record.version != 1 || record.selected.descriptor()?.id != id {
                    return Err(Error::Corrupt("cloud catalog identity"));
                }
                Ok(record)
            })
            .transpose()
    }
    fn cloud_account(&self, key: &[u8]) -> Result<Accounting> {
        self.db
            .read_tx()
            .get(&self.spaces.cloud.accounting, key)?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(|v| v.unwrap_or_default())
    }
    pub(super) fn cloud_usage(&self, repo: &str) -> Result<(u64, u64)> {
        let a = self.cloud_account(&repo_prefix(repo))?;
        Ok((a.blob_bytes, a.objects))
    }
    // Creating/reusing upload namespaces requires the publication gate. Retry
    // repeats durability barriers even if an earlier failed sync left a directory.
    fn cloud_namespace(&self, repo: &str, owner: CloudOwner, create: bool) -> Result<()> {
        let root = self.root.join("cloud");
        let path = root.join(hex(blake3::hash(&scope(repo, owner)).as_bytes()));
        for directory in [root, path] {
            match fs::symlink_metadata(&directory) {
                Ok(m) if m.is_dir() => {}
                Ok(_) => return Err(Error::Corrupt("cloud namespace type")),
                Err(e) if create && e.kind() == io::ErrorKind::NotFound => {
                    create_durable_directories(&directory)?
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(Error::Corrupt("cloud namespace missing"));
                }
                Err(e) => return Err(e.into()),
            }
            if create {
                sync_dir(
                    directory
                        .parent()
                        .ok_or(Error::Corrupt("cloud namespace parent"))?,
                )?;
                sync_dir(&directory)?;
            }
        }
        Ok(())
    }
    fn cloud_path(&self, repo: &str, owner: CloudOwner, digest: [u8; 32]) -> PathBuf {
        self.root
            .join("cloud")
            .join(hex(blake3::hash(&scope(repo, owner)).as_bytes()))
            .join(hex(&digest))
    }
    fn cloud_verification_limits(&self) -> tinytree_cloud::Limits {
        tinytree_cloud::Limits {
            max_payload_bytes: self.config.cloud.max_object_bytes,
            max_decode_bytes: self.config.cloud.max_decode_bytes,
            max_fields: self.config.cloud.max_fields,
            max_field_bytes: self.config.cloud.max_field_bytes,
            commit_id_bytes: 32,
        }
    }
    fn cloud_codec_limits(
        &self,
        mut limits: tinytree_cloud::Limits,
    ) -> Result<tinytree_cloud::Limits> {
        let policy = self.cloud_verification_limits();
        limits.max_payload_bytes = limits.max_payload_bytes.min(policy.max_payload_bytes);
        limits.max_decode_bytes = limits.max_decode_bytes.min(policy.max_decode_bytes);
        limits.max_fields = limits.max_fields.min(policy.max_fields);
        limits.max_field_bytes = limits.max_field_bytes.min(policy.max_field_bytes);
        limits
            .validate()
            .map_err(|_| Error::Invalid("cloud codec limits"))?;
        if limits.commit_id_bytes != 32 {
            return Err(Error::Invalid("cloud native identity width"));
        }
        Ok(limits)
    }
}
pub struct CloudReadLease {
    file: File,
    descriptor: CloudObjectDescriptor,
    repo: String,
    owner: CloudOwner,
    inner: Arc<Inner>,
}
impl CloudReadLease {
    pub fn descriptor(&self) -> CloudObjectDescriptor {
        self.descriptor
    }
}
impl Read for CloudReadLease {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.file.read(bytes)
    }
}
impl Drop for CloudReadLease {
    fn drop(&mut self) {
        if let Ok(mut gate) = self.inner.gate.lock() {
            let k = (self.repo.clone(), self.owner.principal, self.descriptor.id);
            if let Some(count) = gate.cloud_readers.get_mut(&k) {
                *count -= 1;
                if *count == 0 {
                    gate.cloud_readers.remove(&k);
                }
            }
        }
    }
}
fn verify_record(
    record: &ObjectRecord,
    bytes: &[u8],
    limits: tinytree_cloud::Limits,
) -> Result<VerifiedObject> {
    let descriptor = record.selected.descriptor()?;
    let verified = VerifiedObject::verify(descriptor.id, descriptor.selected, bytes, limits)
        .map_err(|_| Error::Corrupt("cloud backing verification"))?;
    if verified.dependencies() != &record.dependencies.decoded()? {
        return Err(Error::Corrupt("cloud dependency summary"));
    }
    Ok(verified)
}
impl Store {
    pub fn lookup_cloud_objects(
        &self,
        repo: &str,
        owner: CloudOwner,
        ids: &[CloudId],
    ) -> Result<Vec<Option<CloudObjectDescriptor>>> {
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        if ids.len() > 1024 {
            return Err(Error::Limit("cloud lookup batch"));
        }
        ids.iter()
            .map(|id| {
                self.inner
                    .cloud_object(repo, owner, *id)?
                    .map(|r| r.selected.descriptor())
                    .transpose()
            })
            .collect()
    }
    pub fn read_cloud_object(
        &self,
        repo: &str,
        owner: CloudOwner,
        id: CloudId,
        limits: tinytree_cloud::Limits,
    ) -> Result<CloudReadLease> {
        let limits = self.inner.cloud_codec_limits(limits)?;
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let record = self
            .inner
            .cloud_object(repo, owner, id)?
            .ok_or(Error::Invalid("cloud object unavailable"))?;
        let descriptor = record.selected.descriptor()?;
        if descriptor.selected.size_bytes > limits.max_payload_bytes as u64 {
            return Err(Error::Limit("cloud payload bytes"));
        }
        let path = self
            .inner
            .cloud_path(repo, owner, descriptor.selected.blake3);
        let metadata =
            fs::symlink_metadata(&path).map_err(|_| Error::Corrupt("cloud backing missing"))?;
        if !metadata.is_file() || metadata.len() != descriptor.selected.size_bytes {
            return Err(Error::Corrupt("cloud backing type or size"));
        }
        let file = File::open(path)?;
        *gate
            .cloud_readers
            .entry((repo.to_owned(), owner.principal, id))
            .or_default() += 1;
        drop(gate);
        let mut lease = CloudReadLease {
            file,
            descriptor,
            repo: repo.into(),
            owner,
            inner: self.inner.clone(),
        };
        let mut bytes = Vec::with_capacity(descriptor.selected.size_bytes as usize);
        lease.read_to_end(&mut bytes)?;
        verify_record(&record, &bytes, limits)?;
        lease.file.seek(SeekFrom::Start(0))?;
        Ok(lease)
    }
}
struct CloudReservation {
    base: Reservation,
    owner: CloudOwner,
}
impl Drop for CloudReservation {
    fn drop(&mut self) {
        if let Some(inner) = self.base.inner.upgrade() {
            if let Ok(mut gate) = inner.gate.lock() {
                if let Some(value) = gate
                    .cloud_reservations
                    .get_mut(&(self.base.repo.clone(), self.owner.principal))
                {
                    value.0 = value.0.saturating_sub(self.base.new_bytes);
                    value.1 = value.1.saturating_sub(self.base.new_objects);
                    value.2 = value.2.saturating_sub(self.base.bytes);
                    if *value == (0, 0, 0) {
                        gate.cloud_reservations
                            .remove(&(self.base.repo.clone(), self.owner.principal));
                    }
                }
            }
        }
    }
}
impl Inner {
    fn reserve_cloud(
        self: &Arc<Self>,
        repo: &str,
        owner: CloudOwner,
        id: CloudId,
        digest: BlobDigest,
    ) -> Result<CloudReservation> {
        self.reserve_cloud_catalog(
            repo,
            owner,
            &self.spaces.cloud.objects,
            object_key(repo, owner, id),
            digest,
        )
    }
    fn reserve_cloud_catalog(
        self: &Arc<Self>,
        repo: &str,
        owner: CloudOwner,
        space: &SingleWriterTxKeyspace,
        catalog_key: Vec<u8>,
        digest: BlobDigest,
    ) -> Result<CloudReservation> {
        let mut gate = self.lock()?;
        let native = self.repository(repo)?;
        let repository = self.cloud_account(&repo_prefix(repo))?;
        let principal = self.cloud_account(&scope(repo, owner))?;
        let existing = self.db.read_tx().get(space, catalog_key)?;
        let blob = self.db.read_tx().get(
            &self.spaces.cloud.blobs,
            blob_key(repo, owner, digest.blake3),
        )?;
        let new_objects = u64::from(existing.is_none());
        let new_bytes = if existing.is_some() || blob.is_some() {
            0
        } else {
            digest.size_bytes
        };
        let current = gate
            .repository_reservations
            .get(repo)
            .copied()
            .unwrap_or_default();
        let own = gate
            .cloud_reservations
            .get(&(repo.to_owned(), owner.principal))
            .copied()
            .unwrap_or_default();
        let bytes = current
            .0
            .checked_add(new_bytes)
            .ok_or(Error::Limit("cloud staging bytes"))?;
        let objects = current
            .1
            .checked_add(new_objects)
            .ok_or(Error::Limit("cloud staging objects"))?;
        if native
            .bytes
            .checked_add(repository.blob_bytes)
            .and_then(|n| n.checked_add(bytes))
            .is_none_or(|n| n > self.config.max_repository_bytes)
            || native
                .objects
                .checked_add(repository.objects)
                .and_then(|n| n.checked_add(objects))
                .is_none_or(|n| n > self.config.max_objects_per_repository)
            || principal
                .blob_bytes
                .checked_add(own.0)
                .and_then(|n| n.checked_add(new_bytes))
                .is_none_or(|n| n > self.config.cloud.max_blob_bytes_per_owner)
            || principal
                .objects
                .checked_add(own.1)
                .and_then(|n| n.checked_add(new_objects))
                .is_none_or(|n| n > self.config.cloud.max_objects_per_owner)
            || own
                .2
                .checked_add(digest.size_bytes)
                .is_none_or(|n| n > self.config.cloud.max_staging_bytes_per_owner)
            || gate
                .staging_bytes
                .checked_add(digest.size_bytes)
                .is_none_or(|n| n > self.config.max_staging_bytes)
            || gate.staging_uploads >= self.config.max_staging_uploads
        {
            return Err(Error::Limit("cloud upload quota"));
        }
        gate.repository_reservations
            .insert(repo.into(), (bytes, objects));
        gate.cloud_reservations.insert(
            (repo.into(), owner.principal),
            (
                own.0 + new_bytes,
                own.1 + new_objects,
                own.2 + digest.size_bytes,
            ),
        );
        gate.staging_bytes += digest.size_bytes;
        gate.staging_uploads += 1;
        Ok(CloudReservation {
            base: Reservation {
                bytes: digest.size_bytes,
                repo: repo.into(),
                new_bytes,
                new_objects,
                inner: Arc::downgrade(self),
            },
            owner,
        })
    }
}
#[derive(Default)]
struct Delta {
    objects: i128,
    blob_bytes: i128,
    selected_bytes: i128,
    version_payload_bytes: i128,
    version_metadata_bytes: i128,
    version_records: i128,
    stages: i128,
    workspaces: i128,
    versions: i128,
    receipts: i128,
}
fn adjust(value: &mut u64, delta: i128) -> Result<()> {
    *value = u64::try_from(
        (*value as i128)
            .checked_add(delta)
            .ok_or(Error::Corrupt("cloud accounting overflow"))?,
    )
    .map_err(|_| Error::Corrupt("cloud accounting underflow"))?;
    Ok(())
}
type Change<'a> = (&'a SingleWriterTxKeyspace, Vec<u8>, Option<Vec<u8>>);
impl Inner {
    fn cloud_changes(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        repo: &str,
        owner: CloudOwner,
        changes: Vec<Change<'_>>,
        delta: Delta,
    ) -> Result<()> {
        let repository_key = repo_prefix(repo);
        let owner_key = scope(repo, owner);
        let read_account = |key: &[u8]| {
            tx.get(&self.spaces.cloud.accounting, key)?
                .map(|v| decode::<Accounting>(&v))
                .transpose()
                .map(|a| a.unwrap_or_default())
        };
        let mut repository = read_account(&repository_key)?;
        let mut principal = read_account(&owner_key)?;
        let mut global = read_account(&[0, 0])?;
        let fresh_namespace = tx.get(&self.spaces.cloud.accounting, &owner_key)?.is_none();
        let mut metadata = 0i128;
        let mut records = 0i128;
        for (space, key, value) in changes {
            if let Some(old) = tx.get(space, &key)? {
                metadata -= (key.len() + old.len()) as i128;
                records -= 1;
            }
            match value {
                Some(value) => {
                    let binary = std::ptr::eq(space, &self.spaces.cloud.manifests)
                        || std::ptr::eq(space, &self.spaces.cloud.roots);
                    if value.len()
                        > if binary {
                            CLOUD_BINARY_ROW_BYTES
                        } else {
                            CLOUD_JSON_ROW_BYTES
                        }
                    {
                        return Err(Error::Limit("cloud metadata row bytes"));
                    }
                    metadata += (key.len() + value.len()) as i128;
                    records += 1;
                    tx.insert(space, key, value);
                }
                None => tx.remove(space, key),
            }
        }
        for account in [&mut repository, &mut principal, &mut global] {
            adjust(&mut account.objects, delta.objects)?;
            adjust(&mut account.blob_bytes, delta.blob_bytes)?;
            adjust(&mut account.metadata_bytes, metadata)?;
            adjust(&mut account.records, records)?;
            adjust(&mut account.selected_bytes, delta.selected_bytes)?;
            adjust(
                &mut account.version_payload_bytes,
                delta.version_payload_bytes,
            )?;
            adjust(
                &mut account.version_metadata_bytes,
                delta.version_metadata_bytes,
            )?;
            adjust(&mut account.version_records, delta.version_records)?;
            if fresh_namespace {
                adjust(&mut account.namespaces, 1)?;
            }
            adjust(&mut account.stages, delta.stages)?;
            adjust(&mut account.workspaces, delta.workspaces)?;
            adjust(&mut account.versions, delta.versions)?;
            adjust(&mut account.receipts, delta.receipts)?;
        }
        let growth = delta.objects > 0
            || delta.blob_bytes > 0
            || delta.stages > 0
            || delta.versions > 0
            || delta.workspaces > 0
            || delta.receipts > 0
            || metadata > 0;
        if growth
            && (principal.objects > self.config.cloud.max_objects_per_owner
                || principal.blob_bytes > self.config.cloud.max_blob_bytes_per_owner
                || principal.metadata_bytes > self.config.cloud.max_metadata_bytes_per_owner
                || principal.stages > self.config.cloud.max_staging_records_per_owner
                || principal.workspaces > self.config.cloud.max_workspaces_per_owner
                || principal.receipts > self.config.cloud.max_receipts_per_owner)
        {
            return Err(Error::Limit("cloud owner quota"));
        }
        if growth
            && (global
                .selected_bytes
                .checked_add(global.version_payload_bytes)
                .is_none_or(|bytes| bytes > self.config.cloud.max_validation_payload_bytes)
                || global
                    .records
                    .checked_add(global.version_records)
                    .and_then(|n| n.checked_add(global.namespaces.saturating_mul(2)))
                    .and_then(|n| n.checked_add(1))
                    .is_none_or(|n| n > self.config.max_retention_validation_records as u64)
                || global
                    .metadata_bytes
                    .checked_add(global.version_metadata_bytes)
                    .and_then(|n| n.checked_add(global.namespaces.saturating_mul(4096)))
                    .and_then(|n| n.checked_add(4096))
                    .is_none_or(|n| n > self.config.max_retention_validation_bytes))
        {
            return Err(Error::Limit("cloud validation catalog budget"));
        }
        let native = self.repository(repo)?;
        if growth
            && (native
                .bytes
                .checked_add(repository.blob_bytes)
                .is_none_or(|n| n > self.config.max_repository_bytes)
                || native
                    .objects
                    .checked_add(repository.objects)
                    .is_none_or(|n| n > self.config.max_objects_per_repository))
        {
            return Err(Error::Limit("repository quota"));
        }
        tx.insert(
            &self.spaces.cloud.accounting,
            repository_key,
            encode(&repository)?,
        );
        tx.insert(
            &self.spaces.cloud.accounting,
            owner_key,
            encode(&principal)?,
        );
        tx.insert(&self.spaces.cloud.accounting, [0, 0], encode(&global)?);
        Ok(())
    }
}
impl Store {
    /// The first verified raw representation is selected per owner and semantic
    /// identity. An upload is temporary staging, never a cloud-version promise.
    pub fn put_cloud_object(
        &self,
        repo: &str,
        owner: CloudOwner,
        id: CloudId,
        digest: BlobDigest,
        bytes: &[u8],
        now: i64,
        limits: tinytree_cloud::Limits,
    ) -> Result<CloudWriteReceipt> {
        let limits = self.inner.cloud_codec_limits(limits)?;
        if id.is_intrinsic() {
            return Err(Error::Invalid("intrinsic cloud object payload"));
        }
        if bytes.len() > limits.max_payload_bytes || digest.size_bytes != bytes.len() as u64 {
            return Err(Error::Limit("cloud payload bytes"));
        }
        let expires = now
            .checked_add(self.inner.config.staging_ttl_seconds)
            .ok_or(Error::Invalid("cloud staging expiry"))?;
        let reservation = self.inner.reserve_cloud(repo, owner, id, digest)?;
        let verified = VerifiedObject::verify(id, digest, bytes, limits)
            .map_err(|_| Error::Invalid("cloud object verification"))?;
        let proposed = CloudObjectDescriptor {
            id,
            selected: digest,
        };
        let mut temporary = NamedTempFile::new_in(self.inner.root.join("staging"))?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        {
            let _gate = self.inner.lock()?;
            self.inner.cloud_namespace(repo, owner, true)?;
        }
        let mut candidate: Option<CloudReadLease> = None;
        let mut raw_candidate: Option<File> = None;
        loop {
            let _gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(record) = self.inner.cloud_object(repo, owner, id)? {
                let selected = record.selected.descriptor()?;
                if candidate
                    .as_ref()
                    .is_none_or(|candidate| candidate.descriptor != selected)
                {
                    drop(_gate);
                    candidate = Some(self.read_cloud_object(
                        repo,
                        owner,
                        id,
                        self.inner.cloud_verification_limits(),
                    )?);
                    continue;
                }
                let lease = stage_id(selected);
                let mut stage = scope(repo, owner);
                stage.extend_from_slice(lease.as_bytes());
                let previous = self
                    .inner
                    .db
                    .read_tx()
                    .get(&self.inner.spaces.cloud.staging, &stage)?;
                let old = previous
                    .as_ref()
                    .map(|bytes| decode::<StageRecord>(bytes))
                    .transpose()?;
                if old
                    .as_ref()
                    .is_some_and(|old| old.selected.descriptor().ok() != Some(selected))
                {
                    return Err(Error::Corrupt("cloud staging descriptor changed"));
                }
                let expires = old.map_or(expires, |old| old.expires.max(expires));
                let mut tx = self
                    .inner
                    .db
                    .write_tx()
                    .durability(Some(PersistMode::SyncAll));
                self.inner.cloud_changes(
                    &mut tx,
                    repo,
                    owner,
                    vec![(
                        &self.inner.spaces.cloud.staging,
                        stage,
                        Some(encode(&StageRecord {
                            selected: record.selected,
                            expires,
                        })?),
                    )],
                    Delta {
                        stages: i128::from(previous.is_none()),
                        ..Default::default()
                    },
                )?;
                self.inner.commit(tx)?;
                drop(_gate);
                drop(candidate);
                drop(reservation);
                return Ok(CloudWriteReceipt {
                    descriptor: selected,
                    staging_lease: lease,
                    expires_at: expires,
                });
            }
            let view = self.inner.db.read_tx();
            let key = blob_key(repo, owner, digest.blake3);
            let old = view.get(&self.inner.spaces.cloud.blobs, &key)?;
            let mut blob = old
                .as_ref()
                .map(|bytes| decode::<BlobRecord>(bytes))
                .transpose()?
                .unwrap_or(BlobRecord {
                    size: digest.size_bytes,
                    catalog_refs: 0,
                });
            if blob.size != digest.size_bytes {
                return Err(Error::Corrupt("cloud blob size"));
            }
            blob.catalog_refs = blob
                .catalog_refs
                .checked_add(1)
                .ok_or(Error::Limit("cloud catalog references"))?;
            let record = ObjectRecord {
                version: 1,
                selected: Selected::new(proposed),
                dependencies: StoredDependencies::new(verified.dependencies()),
            };
            let lease = stage_id(proposed);
            let mut stage = scope(repo, owner);
            stage.extend_from_slice(lease.as_bytes());
            let mut tx = self
                .inner
                .db
                .write_tx()
                .durability(Some(PersistMode::SyncAll));
            advance_feature(&mut tx, &self.inner.spaces.meta, 3)?;
            self.inner.cloud_changes(
                &mut tx,
                repo,
                owner,
                vec![
                    (
                        &self.inner.spaces.cloud.objects,
                        object_key(repo, owner, id),
                        Some(encode(&record)?),
                    ),
                    (&self.inner.spaces.cloud.blobs, key, Some(encode(&blob)?)),
                    (
                        &self.inner.spaces.cloud.staging,
                        stage,
                        Some(encode(&StageRecord {
                            selected: Selected::new(proposed),
                            expires,
                        })?),
                    ),
                ],
                Delta {
                    objects: 1,
                    blob_bytes: if old.is_none() {
                        digest.size_bytes as i128
                    } else {
                        0
                    },
                    selected_bytes: digest.size_bytes as i128,
                    stages: 1,
                    ..Default::default()
                },
            )?;
            // Reject catalog/owner/validation quotas before installing a raw
            // payload. The transaction is still uncommitted: bytes and their
            // directory must become durable before any successful metadata ACK.
            let target = self.inner.cloud_path(repo, owner, digest.blake3);
            match temporary.persist_noclobber(&target) {
                Ok(file) => drop(file),
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    temporary = error.file;
                    let metadata = fs::symlink_metadata(&target)?;
                    if !metadata.is_file() || metadata.len() != digest.size_bytes {
                        return Err(Error::Corrupt("cloud raw blob type or size"));
                    }
                    if raw_candidate.is_none() {
                        let mut file = File::open(&target)?;
                        // Release the uncommitted writer transaction before
                        // verifying an existing raw file outside the GC gate.
                        drop(tx);
                        drop(_gate);
                        let mut actual = Vec::new();
                        (&mut file)
                            .take(digest.size_bytes + 1)
                            .read_to_end(&mut actual)?;
                        if BlobDigest::of(&actual) != digest {
                            return Err(Error::Corrupt("cloud raw blob collision"));
                        }
                        file.sync_all()?;
                        raw_candidate = Some(file);
                        continue;
                    }
                }
                Err(error) => return Err(error.error.into()),
            }
            sync_dir(
                target
                    .parent()
                    .ok_or(Error::Corrupt("cloud payload path"))?,
            )?;
            self.inner.commit(tx)?;
            drop(_gate);
            drop(reservation);
            return Ok(CloudWriteReceipt {
                descriptor: proposed,
                staging_lease: lease,
                expires_at: expires,
            });
        }
    }
}

fn add_account(target: &mut Accounting, source: &Accounting) -> Result<()> {
    adjust(&mut target.objects, source.objects as i128)?;
    adjust(&mut target.blob_bytes, source.blob_bytes as i128)?;
    adjust(&mut target.selected_bytes, source.selected_bytes as i128)?;
    adjust(
        &mut target.version_payload_bytes,
        source.version_payload_bytes as i128,
    )?;
    adjust(
        &mut target.version_metadata_bytes,
        source.version_metadata_bytes as i128,
    )?;
    adjust(&mut target.version_records, source.version_records as i128)?;
    adjust(&mut target.metadata_bytes, source.metadata_bytes as i128)?;
    adjust(&mut target.records, source.records as i128)?;
    adjust(&mut target.namespaces, source.namespaces as i128)?;
    adjust(&mut target.stages, source.stages as i128)?;
    adjust(&mut target.workspaces, source.workspaces as i128)?;
    adjust(&mut target.versions, source.versions as i128)?;
    adjust(&mut target.receipts, source.receipts as i128)
}
fn metadata_account(target: &mut Accounting, key: &[u8], value: &[u8]) -> Result<()> {
    adjust(&mut target.records, 1)?;
    adjust(
        &mut target.metadata_bytes,
        (key.len() + value.len()) as i128,
    )
}
struct Scan {
    records: usize,
    bytes: u64,
    payload: u64,
}
impl Scan {
    fn visit(&mut self, inner: &Inner, key: &[u8], value: &[u8]) -> Result<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or(Error::Limit("cloud catalog scan"))?;
        self.bytes = self
            .bytes
            .checked_add((key.len() + value.len()) as u64)
            .ok_or(Error::Limit("cloud catalog scan"))?;
        if self.records > inner.config.max_retention_validation_records
            || self.bytes > inner.config.max_retention_validation_bytes
        {
            return Err(Error::Limit("cloud catalog scan"));
        }
        Ok(())
    }
    fn payload(&mut self, inner: &Inner, size: u64) -> Result<()> {
        self.payload = self
            .payload
            .checked_add(size)
            .ok_or(Error::Limit("cloud payload validation"))?;
        if self.payload > inner.config.cloud.max_validation_payload_bytes {
            return Err(Error::Limit("cloud payload validation"));
        }
        Ok(())
    }
}
impl Inner {
    fn verify_cloud_backing(
        &self,
        repo: &str,
        owner: CloudOwner,
        record: &ObjectRecord,
        scan: &mut Scan,
    ) -> Result<VerifiedObject> {
        let descriptor = record.selected.descriptor()?;
        scan.payload(self, descriptor.selected.size_bytes)?;
        if descriptor.selected.size_bytes > self.config.cloud.max_object_bytes as u64 {
            return Err(Error::Corrupt("cloud stored payload size"));
        }
        self.cloud_namespace(repo, owner, false)?;
        let path = self.cloud_path(repo, owner, descriptor.selected.blake3);
        let metadata =
            fs::symlink_metadata(&path).map_err(|_| Error::Corrupt("cloud backing missing"))?;
        if !metadata.is_file() || metadata.len() != descriptor.selected.size_bytes {
            return Err(Error::Corrupt("cloud backing type or size"));
        }
        let bytes = fs::read(path)?;
        verify_record(record, &bytes, self.cloud_verification_limits())
    }
    pub(super) fn validate_cloud_on_reopen(&self) -> Result<()> {
        let feature = store_feature(
            self.db
                .read_tx()
                .get(&self.spaces.meta, b"format")?
                .as_deref(),
        )?;
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        if feature < 3 {
            for space in self.spaces.cloud.all() {
                if self.db.read_tx().iter(space).next().is_some() {
                    return Err(Error::Corrupt("cloud catalog feature marker missing"));
                }
            }
            return Ok(());
        }
        let mut expected = HashMap::<Vec<u8>, Accounting>::new();
        let mut blob_refs = HashMap::<Vec<u8>, (u64, u64)>::new();
        let view = self.db.read_tx();
        for item in view.iter(&self.spaces.cloud.objects) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let (repo, owner, _) = parse_scope(&key)?;
            self.repository(repo)?;
            let record: ObjectRecord = decode(&value)?;
            let descriptor = record.selected.descriptor()?;
            if record.version != 1
                || key.as_ref() != object_key(repo, owner, descriptor.id).as_slice()
            {
                return Err(Error::Corrupt("cloud object key identity"));
            }
            self.verify_cloud_backing(repo, owner, &record, &mut scan)?;
            let a = expected.entry(scope(repo, owner)).or_default();
            metadata_account(a, &key, &value)?;
            adjust(&mut a.objects, 1)?;
            adjust(
                &mut a.selected_bytes,
                descriptor.selected.size_bytes as i128,
            )?;
            let blob = blob_refs
                .entry(blob_key(repo, owner, descriptor.selected.blake3))
                .or_insert((descriptor.selected.size_bytes, 0));
            if blob.0 != descriptor.selected.size_bytes {
                return Err(Error::Corrupt("cloud raw blob descriptor collision"));
            }
            blob.1 = blob
                .1
                .checked_add(1)
                .ok_or(Error::Corrupt("cloud blob references overflow"))?;
        }
        self.validate_index_catalog(feature, &mut expected, &mut blob_refs, &mut scan)?;
        for item in view.iter(&self.spaces.cloud.blobs) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let (repo, owner, suffix) = parse_scope(&key)?;
            self.repository(repo)?;
            if suffix.len() != 32 {
                return Err(Error::Corrupt("cloud blob key"));
            }
            let blob: BlobRecord = decode(&value)?;
            if blob.catalog_refs == 0
                || blob_refs.remove(key.as_ref()) != Some((blob.size, blob.catalog_refs))
            {
                return Err(Error::Corrupt("cloud blob accounting"));
            }
            let a = expected.entry(scope(repo, owner)).or_default();
            metadata_account(a, &key, &value)?;
            adjust(&mut a.blob_bytes, blob.size as i128)?;
        }
        if !blob_refs.is_empty() {
            return Err(Error::Corrupt("cloud blob catalog missing"));
        }
        for item in view.iter(&self.spaces.cloud.staging) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let (repo, owner, suffix) = parse_scope(&key)?;
            self.repository(repo)?;
            let stage: StageRecord = decode(&value)?;
            let selected = stage.selected.descriptor()?;
            if suffix != stage_id(selected).as_bytes()
                || self
                    .cloud_object(repo, owner, selected.id)?
                    .is_none_or(|record| record.selected.descriptor().ok() != Some(selected))
            {
                return Err(Error::Corrupt("cloud staging identity"));
            }
            let a = expected.entry(scope(repo, owner)).or_default();
            metadata_account(a, &key, &value)?;
            adjust(&mut a.stages, 1)?;
        }
        self.validate_cloud_versions_on_reopen(&mut expected, &mut scan)?;
        let mut actual = HashMap::<Vec<u8>, Accounting>::new();
        let mut namespaces = Vec::new();
        for item in view.iter(&self.spaces.cloud.accounting) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let account: Accounting = decode(&value)?;
            if key.as_ref() != [0, 0] {
                let size = u16::from_be_bytes(
                    key.get(..2)
                        .ok_or(Error::Corrupt("cloud accounting key"))?
                        .try_into()
                        .unwrap(),
                ) as usize;
                if key.len() == 2 + size + 32 {
                    let (repo, owner, suffix) = parse_scope(&key)?;
                    self.repository(repo)?;
                    if !suffix.is_empty() {
                        return Err(Error::Corrupt("cloud accounting namespace"));
                    }
                    namespaces.push((repo.to_owned(), owner));
                } else if key.len() != 2 + size {
                    return Err(Error::Corrupt("cloud accounting key"));
                }
            }
            actual.insert(key.to_vec(), account);
        }
        let mut repos = HashMap::<Vec<u8>, Accounting>::new();
        let mut global = Accounting::default();
        for (repo, owner) in namespaces {
            let key = scope(&repo, owner);
            let mut account = expected.remove(&key).unwrap_or_default();
            account.namespaces = 1;
            if actual.remove(&key) != Some(account.clone()) {
                return Err(Error::Corrupt("cloud owner accounting totals"));
            }
            add_account(repos.entry(repo_prefix(&repo)).or_default(), &account)?;
            add_account(&mut global, &account)?;
        }
        if !expected.is_empty() {
            return Err(Error::Corrupt("cloud owner accounting missing"));
        }
        for (key, account) in repos {
            if actual.remove(&key) != Some(account) {
                return Err(Error::Corrupt("cloud repository accounting totals"));
            }
        }
        let stored = actual.remove([0u8, 0u8].as_slice());
        if stored != Some(global.clone()) && !(stored.is_none() && global == Accounting::default())
        {
            return Err(Error::Corrupt("cloud global accounting totals"));
        }
        if !actual.is_empty() {
            return Err(Error::Corrupt("unexpected cloud accounting"));
        }
        Ok(())
    }
    pub(super) fn has_cloud_state(&self, repo: &str) -> Result<bool> {
        Ok(self.cloud_account(&repo_prefix(repo))?.objects != 0
            || self
                .db
                .read_tx()
                .prefix(&self.spaces.cloud.versions, repo_prefix(repo))
                .next()
                .is_some())
    }
}
struct GcChange {
    space: usize,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
#[derive(Default)]
struct OwnerGc {
    changes: Vec<GcChange>,
    delta: Delta,
}
pub(super) struct CloudGcPlan {
    pub strict: HashSet<ObjectKey>,
    pub transient: HashSet<ObjectKey>,
    groups: HashMap<CloudOwner, OwnerGc>,
    paths: Vec<PathBuf>,
    pub objects: u64,
    pub bytes: u64,
    pub expired: u64,
}
impl CloudGcPlan {
    pub(super) fn apply(
        &self,
        inner: &Inner,
        repo: &str,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
    ) -> Result<()> {
        for (owner, group) in &self.groups {
            let spaces = inner.spaces.cloud.all();
            let changes = group
                .changes
                .iter()
                .map(|c| (spaces[c.space], c.key.clone(), c.value.clone()))
                .collect();
            inner.cloud_changes(
                tx,
                repo,
                *owner,
                changes,
                Delta {
                    objects: group.delta.objects,
                    blob_bytes: group.delta.blob_bytes,
                    selected_bytes: group.delta.selected_bytes,
                    stages: group.delta.stages,
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }
    pub(super) fn unlink(self) -> Result<()> {
        let mut parents = HashSet::new();
        for path in self.paths {
            if let Some(parent) = path.parent() {
                parents.insert(parent.to_owned());
            }
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        for parent in parents {
            sync_dir(&parent)?;
        }
        Ok(())
    }
}
impl Inner {
    pub(super) fn cloud_gc_plan(
        &self,
        repo: &str,
        now: i64,
        gate: &Runtime,
    ) -> Result<CloudGcPlan> {
        let view = self.db.read_tx();
        let prefix = repo_prefix(repo);
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let published = self.retained_cloud_plan(Some(repo), None, true, &mut scan)?;
        let mut objects = HashMap::<(CloudOwner, CloudId), (Vec<u8>, ObjectRecord)>::new();
        for item in view.prefix(&self.spaces.cloud.objects, &prefix) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let (actual, owner, _) = parse_scope(&key)?;
            let record: ObjectRecord = decode(&value)?;
            let selected = record.selected.descriptor()?;
            if actual != repo
                || record.version != 1
                || key.as_ref() != object_key(repo, owner, selected.id).as_slice()
            {
                return Err(Error::Corrupt("cloud object key identity"));
            }
            objects.insert((owner, selected.id), (key.to_vec(), record));
        }
        let mut plan = CloudGcPlan {
            strict: published.native,
            transient: HashSet::new(),
            groups: HashMap::new(),
            paths: Vec::new(),
            objects: 0,
            bytes: 0,
            expired: 0,
        };
        let mut pending = Vec::new();
        let published_index = published.index;
        let mut marked = published.cloud;
        for item in view.prefix(&self.spaces.cloud.staging, &prefix) {
            let (key, value) = item.into_inner()?;
            scan.visit(self, &key, &value)?;
            let (actual, owner, suffix) = parse_scope(&key)?;
            let stage: StageRecord = decode(&value)?;
            let selected = stage.selected.descriptor()?;
            if actual != repo
                || suffix != stage_id(selected).as_bytes()
                || objects
                    .get(&(owner, selected.id))
                    .is_none_or(|(_, r)| r.selected.descriptor().ok() != Some(selected))
            {
                return Err(Error::Corrupt("cloud staging identity"));
            }
            if stage.expires > now {
                if marked.insert((owner, selected.id)) {
                    pending.push((owner, selected.id));
                }
            } else {
                let group = plan.groups.entry(owner).or_default();
                group.changes.push(GcChange {
                    space: 2,
                    key: key.to_vec(),
                    value: None,
                });
                group.delta.stages -= 1;
                plan.expired += 1;
            }
        }
        for ((actual, owner, id), _) in &gate.cloud_readers {
            if actual == repo {
                let root = (CloudOwner { principal: *owner }, *id);
                if !objects.contains_key(&root) {
                    return Err(Error::Corrupt("cloud active reader catalog missing"));
                }
                if marked.insert(root) {
                    pending.push(root);
                }
            }
        }
        while let Some((owner, id)) = pending.pop() {
            let Some((_, record)) = objects.get(&(owner, id)) else {
                continue;
            };
            let verified = self.verify_cloud_backing(repo, owner, record, &mut scan)?;
            for commit in &verified.dependencies().native_commits {
                if *commit != [0; 32] {
                    plan.transient.insert(ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId(*commit),
                    });
                }
            }
            for child in verified
                .dependencies()
                .operations
                .iter()
                .copied()
                .map(CloudId::Operation)
                .chain(
                    verified
                        .dependencies()
                        .views
                        .iter()
                        .copied()
                        .map(CloudId::View),
                )
            {
                if !child.is_intrinsic()
                    && objects.contains_key(&(owner, child))
                    && marked.insert((owner, child))
                {
                    pending.push((owner, child));
                }
            }
            if marked.len() > self.config.max_retention_validation_records
                || plan.transient.len() > self.config.max_closure_objects
            {
                return Err(Error::Limit("cloud staging closure"));
            }
        }
        let mut victim_refs = HashMap::<Vec<u8>, (CloudOwner, u64)>::new();
        for ((owner, id), (key, record)) in objects {
            if marked.contains(&(owner, id)) {
                continue;
            }
            let descriptor = record.selected.descriptor()?;
            let group = plan.groups.entry(owner).or_default();
            group.changes.push(GcChange {
                space: 0,
                key,
                value: None,
            });
            group.delta.objects -= 1;
            group.delta.selected_bytes -= descriptor.selected.size_bytes as i128;
            plan.objects += 1;
            let references = victim_refs
                .entry(blob_key(repo, owner, descriptor.selected.blake3))
                .or_insert((owner, 0));
            references.1 += 1;
        }
        self.index_gc_into(
            repo,
            now,
            gate,
            &published_index,
            &mut scan,
            &mut plan,
            &mut victim_refs,
        )?;
        for (key, (owner, removed)) in victim_refs {
            let bytes = view
                .get(&self.spaces.cloud.blobs, &key)?
                .ok_or(Error::Corrupt("cloud blob catalog missing"))?;
            let mut blob: BlobRecord = decode(&bytes)?;
            blob.catalog_refs = blob
                .catalog_refs
                .checked_sub(removed)
                .ok_or(Error::Corrupt("cloud blob references"))?;
            let value = if blob.catalog_refs == 0 {
                let digest: [u8; 32] = key[key.len() - 32..].try_into().unwrap();
                plan.paths.push(self.cloud_path(repo, owner, digest));
                plan.bytes = plan
                    .bytes
                    .checked_add(blob.size)
                    .ok_or(Error::Limit("cloud GC byte count"))?;
                plan.groups.entry(owner).or_default().delta.blob_bytes -= blob.size as i128;
                None
            } else {
                Some(encode(&blob)?)
            };
            plan.groups
                .entry(owner)
                .or_default()
                .changes
                .push(GcChange {
                    space: 1,
                    key,
                    value,
                });
        }
        Ok(plan)
    }
}
