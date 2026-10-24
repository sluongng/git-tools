//! Atomic workspace versions. All published history is retained in this format.
use super::artifacts::{IndexStage, artifact_key};
use super::*;
use tinytree_cloud::{
    IndexArtifactDescriptor, IndexArtifactId, IndexRootId, Manifest, ManifestId, ManifestLimits,
    ManifestV2, ManifestV2Limits, VerifiedIndexArtifact, decode_manifest, decode_manifest_v2,
    verify_coverage, verify_coverage_v2, verify_index_artifact_graph,
};

enum RecoveryManifest {
    V1(Manifest),
    V2(ManifestV2),
}
fn v2_error(error: tinytree_cloud::Error, invalid: &'static str) -> Error {
    match error {
        tinytree_cloud::Error::Limit(_) => Error::Limit("cloud index validation budget"),
        _ => Error::Invalid(invalid),
    }
}
impl std::ops::Deref for RecoveryManifest {
    type Target = Manifest;
    fn deref(&self) -> &Manifest {
        match self {
            Self::V1(v) => v,
            Self::V2(v) => &v.recovery,
        }
    }
}
impl RecoveryManifest {
    fn format(&self) -> u8 {
        match self {
            Self::V1(_) => 1,
            Self::V2(_) => 2,
        }
    }
}
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
struct ExtraWork {
    records: u64,
    metadata: u64,
    payload: u64,
}
impl ExtraWork {
    fn row(&mut self, key: &[u8], bytes: &[u8]) -> Result<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or(Error::Limit("cloud index proof work"))?;
        self.metadata = self
            .metadata
            .checked_add((key.len() + bytes.len()) as u64)
            .ok_or(Error::Limit("cloud index proof work"))?;
        Ok(())
    }
    fn raw(&mut self, bytes: u64) -> Result<()> {
        self.payload = self
            .payload
            .checked_add(bytes)
            .ok_or(Error::Limit("cloud index proof work"))?;
        Ok(())
    }
    fn check(&self, inner: &Inner) -> Result<()> {
        if self.records > inner.config.max_retention_validation_records as u64
            || self.metadata > inner.config.max_retention_validation_bytes
            || self.payload > inner.config.cloud.max_validation_payload_bytes
        {
            return Err(Error::Limit("cloud index proof work"));
        }
        Ok(())
    }
    fn charge(&self, inner: &Inner, scan: &mut Scan) -> Result<()> {
        scan.records = scan
            .records
            .checked_add(
                usize::try_from(self.records)
                    .map_err(|_| Error::Limit("cloud index proof work"))?,
            )
            .ok_or(Error::Limit("cloud index proof work"))?;
        scan.bytes = scan
            .bytes
            .checked_add(self.metadata)
            .ok_or(Error::Limit("cloud index proof work"))?;
        scan.payload(inner, self.payload)?;
        if scan.records > inner.config.max_retention_validation_records
            || scan.bytes > inner.config.max_retention_validation_bytes
        {
            return Err(Error::Limit("cloud catalog scan"));
        }
        Ok(())
    }
}
struct ProofMemory {
    used: usize,
    maximum: usize,
}
impl ProofMemory {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|n| *n <= self.maximum)
            .ok_or(Error::Limit("cloud index native proof memory"))?;
        Ok(())
    }
    fn release(&mut self, bytes: usize) {
        self.used = self
            .used
            .checked_sub(bytes)
            .expect("native proof memory ownership");
    }
}
fn snapshot_memory(value: &tinytree_cloud::PortableIndexSnapshot) -> Result<usize> {
    let mut bytes = std::mem::size_of_val(value)
        .checked_add(4096)
        .and_then(|n| {
            n.checked_add(
                value
                    .roots
                    .capacity()
                    .checked_mul(std::mem::size_of::<CommitId>())?,
            )
        })
        .and_then(|n| {
            n.checked_add(
                value
                    .drafts
                    .capacity()
                    .checked_mul(std::mem::size_of::<tinytree_cloud::IndexDraft>())?,
            )
        })
        .ok_or(Error::Limit("cloud index snapshot memory"))?;
    for draft in &value.drafts {
        bytes = bytes
            .checked_add(
                draft
                    .parents
                    .capacity()
                    .checked_mul(std::mem::size_of::<CommitId>())
                    .ok_or(Error::Limit("cloud index snapshot memory"))?,
            )
            .ok_or(Error::Limit("cloud index snapshot memory"))?;
    }
    Ok(bytes)
}
#[derive(Clone)]
struct VersionRoots {
    native: Vec<ObjectKey>,
    artifacts: Vec<IndexArtifactDescriptor>,
    extra: ExtraWork,
}
impl Inner {
    fn v2_limits(&self, limits: ManifestLimits) -> Result<ManifestV2Limits> {
        Ok(ManifestV2Limits {
            recovery: self.manifest_limits(limits)?,
            artifacts: self.index_limits(self.config.cloud.index_artifacts)?,
            max_index_roots: self.config.cloud.index_artifacts.max_snapshots,
            max_operation_indexes: self
                .config
                .cloud
                .manifest_limits
                .max_members
                .checked_add(1)
                .ok_or(Error::Limit("cloud operation index count"))?,
        })
    }
    fn decode_recovery(
        &self,
        bytes: &[u8],
        id: ManifestId,
        limits: ManifestLimits,
    ) -> Result<RecoveryManifest> {
        match bytes.get(8..10) {
            Some([1, 0]) => decode_manifest(bytes, id, self.manifest_limits(limits)?)
                .map(RecoveryManifest::V1)
                .map_err(|_| Error::Invalid("cloud manifest encoding")),
            Some([2, 0]) => decode_manifest_v2(bytes, id, self.v2_limits(limits)?)
                .map(RecoveryManifest::V2)
                .map_err(|error| v2_error(error, "cloud v2 manifest encoding")),
            _ => Err(Error::Invalid("cloud manifest version")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudWorkspace {
    pub workspace: [u8; 32],
    pub generation: u64,
    pub manifest: Option<ManifestId>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudPublication {
    pub idempotency_key: String,
    pub workspace: CloudWorkspace,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudVersion {
    pub workspace: [u8; 32],
    pub generation: u64,
    pub manifest: ManifestId,
    pub previous: Option<ManifestId>,
}
#[derive(Clone)]
pub struct PublishCloudRequest {
    pub workspace: [u8; 32],
    pub expected_generation: u64,
    pub idempotency_key: String,
    pub manifest: ManifestId,
    pub manifest_bytes: Vec<u8>,
    pub staging_leases: Vec<String>,
}
#[derive(Clone, Debug, Default)]
pub struct CloudPublicationLimits {
    pub manifest: ManifestLimits,
    pub retention: RetentionLimits,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceRecord {
    generation: u64,
    manifest: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionRecord {
    format: u8,
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    previous: Option<[u8; 32]>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptRecord {
    format: u8,
    fingerprint: [u8; 32],
    version: VersionRecord,
}
fn workspace_key(repo: &str, owner: CloudOwner, workspace: [u8; 32]) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.extend_from_slice(&workspace);
    key
}
fn version_key(repo: &str, owner: CloudOwner, workspace: [u8; 32], generation: u64) -> Vec<u8> {
    let mut key = workspace_key(repo, owner, workspace);
    key.extend_from_slice(&generation.to_be_bytes());
    key
}
fn manifest_key(repo: &str, owner: CloudOwner, id: ManifestId) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.extend_from_slice(&id.0);
    key
}
fn receipt_key(repo: &str, owner: CloudOwner, id: &str) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.extend_from_slice(id.as_bytes());
    key
}
fn publication(key: &str, record: &VersionRecord) -> CloudPublication {
    CloudPublication {
        idempotency_key: key.into(),
        workspace: CloudWorkspace {
            workspace: record.workspace,
            generation: record.generation,
            manifest: Some(ManifestId(record.manifest)),
        },
    }
}
fn version(record: &VersionRecord) -> CloudVersion {
    CloudVersion {
        workspace: record.workspace,
        generation: record.generation,
        manifest: ManifestId(record.manifest),
        previous: record.previous.map(ManifestId),
    }
}
fn fingerprint(request: &PublishCloudRequest) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-cloud-publication-v1\0");
    h.update(&request.workspace);
    h.update(&request.expected_generation.to_be_bytes());
    h.update(&request.manifest.0);
    h.update(&(request.manifest_bytes.len() as u64).to_be_bytes());
    h.update(&request.manifest_bytes);
    h.update(&(request.staging_leases.len() as u64).to_be_bytes());
    for lease in &request.staging_leases {
        h.update(&(lease.len() as u64).to_be_bytes());
        h.update(lease.as_bytes());
    }
    *h.finalize().as_bytes()
}
fn encode_roots(id: ManifestId, roots: &[ObjectKey]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"TTROOT01");
    bytes.extend_from_slice(&id.0);
    bytes.extend_from_slice(
        &u32::try_from(roots.len())
            .map_err(|_| Error::Limit("cloud native roots"))?
            .to_le_bytes(),
    );
    for root in roots {
        bytes.push(root.kind as u8);
        bytes.extend_from_slice(&root.id.0);
    }
    if bytes.len() > CLOUD_BINARY_ROW_BYTES {
        return Err(Error::Limit("cloud native root row"));
    }
    Ok(bytes)
}
fn decode_roots(bytes: &[u8], id: ManifestId, maximum: usize) -> Result<Vec<ObjectKey>> {
    if bytes.len() < 44
        || bytes.len() > CLOUD_BINARY_ROW_BYTES
        || &bytes[..8] != b"TTROOT01"
        || bytes[8..40] != id.0
    {
        return Err(Error::Corrupt("cloud native root record"));
    }
    let size = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    if size > maximum || size.checked_mul(33).and_then(|n| n.checked_add(44)) != Some(bytes.len()) {
        return Err(Error::Corrupt("cloud native root count"));
    }
    let mut roots = Vec::with_capacity(size);
    let mut previous = None;
    for value in bytes[44..].chunks_exact(33) {
        let root = ObjectKey {
            kind: Kind::try_from(value[0])?,
            id: ObjectId(value[1..].try_into().unwrap()),
        };
        let key = (root.kind as u8, root.id.0);
        if previous.is_some_and(|old| old >= key)
            || (root.kind == Kind::Commit && root.id.0 == [0; 32])
        {
            return Err(Error::Corrupt("cloud native root order"));
        }
        previous = Some(key);
        roots.push(root);
    }
    Ok(roots)
}
fn encode_version_roots(id: ManifestId, value: &VersionRoots, format: u8) -> Result<Vec<u8>> {
    let mut bytes = encode_roots(id, &value.native)?;
    if format == 1 {
        if !value.artifacts.is_empty() || value.extra != ExtraWork::default() {
            return Err(Error::Corrupt("v1 index roots"));
        }
        return Ok(bytes);
    }
    bytes[..8].copy_from_slice(b"TTROOT02");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    for n in [
        value.extra.records,
        value.extra.metadata,
        value.extra.payload,
    ] {
        bytes.extend_from_slice(&n.to_le_bytes());
    }
    bytes.extend_from_slice(
        &u32::try_from(value.artifacts.len())
            .map_err(|_| Error::Limit("cloud artifact closure"))?
            .to_le_bytes(),
    );
    for d in &value.artifacts {
        match d.id {
            IndexArtifactId::Root(id) => {
                bytes.push(1);
                bytes.extend_from_slice(&id.0);
            }
            IndexArtifactId::Node(id) => {
                bytes.push(2);
                bytes.extend_from_slice(&id.0);
            }
        }
        bytes.extend_from_slice(&d.selected.blake3);
        bytes.extend_from_slice(&d.selected.size_bytes.to_le_bytes());
    }
    if bytes.len() > CLOUD_BINARY_ROW_BYTES {
        return Err(Error::Limit("cloud root record"));
    }
    Ok(bytes)
}
fn decode_version_roots(
    bytes: &[u8],
    id: ManifestId,
    native_max: usize,
    artifact_max: usize,
    format: u8,
) -> Result<VersionRoots> {
    if format == 1 {
        return Ok(VersionRoots {
            native: decode_roots(bytes, id, native_max)?,
            artifacts: Vec::new(),
            extra: ExtraWork::default(),
        });
    }
    if bytes.len() < 74
        || bytes.len() > CLOUD_BINARY_ROW_BYTES
        || &bytes[..8] != b"TTROOT02"
        || bytes[8..40] != id.0
    {
        return Err(Error::Corrupt("cloud v2 root record"));
    }
    let count = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    let end = count
        .checked_mul(33)
        .and_then(|n| n.checked_add(44))
        .filter(|n| n.checked_add(30).is_some_and(|n| n <= bytes.len()))
        .ok_or(Error::Corrupt("cloud v2 root count"))?;
    let mut legacy = bytes[..end].to_vec();
    legacy[..8].copy_from_slice(b"TTROOT01");
    let native = decode_roots(&legacy, id, native_max)?;
    if bytes[end..end + 2] != 1u16.to_le_bytes() {
        return Err(Error::Corrupt("cloud v2 proof policy"));
    }
    let extra = ExtraWork {
        records: u64::from_le_bytes(bytes[end + 2..end + 10].try_into().unwrap()),
        metadata: u64::from_le_bytes(bytes[end + 10..end + 18].try_into().unwrap()),
        payload: u64::from_le_bytes(bytes[end + 18..end + 26].try_into().unwrap()),
    };
    let n = u32::from_le_bytes(bytes[end + 26..end + 30].try_into().unwrap()) as usize;
    if n > artifact_max
        || n.checked_mul(73).and_then(|n| n.checked_add(end + 30)) != Some(bytes.len())
    {
        return Err(Error::Corrupt("cloud artifact closure count"));
    }
    let mut artifacts = Vec::with_capacity(n);
    let mut previous = None;
    for raw in bytes[end + 30..].chunks_exact(73) {
        let digest: [u8; 32] = raw[1..33].try_into().unwrap();
        let key = match raw[0] {
            1 => IndexArtifactId::Root(IndexRootId(digest)),
            2 => IndexArtifactId::Node(tinytree_cloud::IndexNodeId(digest)),
            _ => return Err(Error::Corrupt("cloud artifact closure kind")),
        };
        if previous.is_some_and(|old| old >= key) {
            return Err(Error::Corrupt("cloud artifact closure order"));
        }
        previous = Some(key);
        artifacts.push(IndexArtifactDescriptor {
            id: key,
            selected: BlobDigest {
                blake3: raw[33..65].try_into().unwrap(),
                size_bytes: u64::from_le_bytes(raw[65..73].try_into().unwrap()),
            },
        });
    }
    Ok(VersionRoots {
        native,
        artifacts,
        extra,
    })
}
struct ValidationGuard {
    native: Vec<ObjectKey>,
    cloud: Vec<CloudId>,
    artifacts: Vec<IndexArtifactId>,
    version: Option<([u8; 32], u64)>,
    repo: String,
    owner: CloudOwner,
    inner: Arc<Inner>,
}
impl Drop for ValidationGuard {
    fn drop(&mut self) {
        if let Ok(mut gate) = self.inner.gate.lock() {
            if let Some((workspace, generation)) = self.version {
                let key = (
                    self.repo.clone(),
                    self.owner.principal,
                    workspace,
                    generation,
                );
                if let Some(count) = gate.cloud_version_readers.get_mut(&key) {
                    *count -= 1;
                    if *count == 0 {
                        gate.cloud_version_readers.remove(&key);
                    }
                }
            }
            for object in &self.native {
                let key = (self.repo.clone(), *object);
                if let Some(count) = gate.readers.get_mut(&key) {
                    *count -= 1;
                    if *count == 0 {
                        gate.readers.remove(&key);
                    }
                }
            }
            for id in &self.artifacts {
                let key = (self.repo.clone(), self.owner.principal, *id);
                if let Some(count) = gate.index_artifact_readers.get_mut(&key) {
                    *count -= 1;
                    if *count == 0 {
                        gate.index_artifact_readers.remove(&key);
                    }
                }
            }
            for id in &self.cloud {
                let key = (self.repo.clone(), self.owner.principal, *id);
                if let Some(count) = gate.cloud_readers.get_mut(&key) {
                    *count -= 1;
                    if *count == 0 {
                        gate.cloud_readers.remove(&key);
                    }
                }
            }
        }
    }
}
pub struct CloudVersionReadGuard {
    version: CloudVersion,
    manifest: Vec<u8>,
    _guard: ValidationGuard,
}
impl CloudVersionReadGuard {
    pub(super) fn native_roots(&self) -> &[ObjectKey] {
        &self._guard.native
    }
    pub fn version(&self) -> &CloudVersion {
        &self.version
    }
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest
    }
}
impl Inner {
    fn manifest_limits(&self, mut limits: ManifestLimits) -> Result<ManifestLimits> {
        limits
            .validate()
            .map_err(|_| Error::Invalid("cloud manifest limits"))?;
        let policy = self.config.cloud.manifest_limits;
        limits.max_manifest_bytes = limits.max_manifest_bytes.min(policy.max_manifest_bytes);
        limits.max_decode_bytes = limits.max_decode_bytes.min(policy.max_decode_bytes);
        limits.max_members = limits.max_members.min(policy.max_members);
        limits.max_operation_roots = limits.max_operation_roots.min(policy.max_operation_roots);
        limits.max_native_roots = limits
            .max_native_roots
            .min(policy.max_native_roots)
            .min(self.config.max_closure_objects);
        limits.max_intents = limits.max_intents.min(policy.max_intents);
        limits.max_tree_terms = limits.max_tree_terms.min(policy.max_tree_terms);
        limits.max_name_bytes = limits.max_name_bytes.min(policy.max_name_bytes);
        limits.max_label_bytes = limits.max_label_bytes.min(policy.max_label_bytes);
        limits.max_edges = limits.max_edges.min(policy.max_edges);
        limits.max_total_payload_bytes = limits
            .max_total_payload_bytes
            .min(policy.max_total_payload_bytes);
        Ok(limits)
    }
    fn cloud_workspace(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
    ) -> Result<CloudWorkspace> {
        let record = self
            .db
            .read_tx()
            .get(
                &self.spaces.cloud.workspaces,
                workspace_key(repo, owner, workspace),
            )?
            .map(|bytes| decode::<WorkspaceRecord>(&bytes))
            .transpose()?;
        match record {
            None => Ok(CloudWorkspace {
                workspace,
                generation: 0,
                manifest: None,
            }),
            Some(record) => {
                if record.generation == 0 {
                    return Err(Error::Corrupt("cloud workspace generation"));
                }
                Ok(CloudWorkspace {
                    workspace,
                    generation: record.generation,
                    manifest: Some(ManifestId(record.manifest)),
                })
            }
        }
    }
    fn cloud_replay(
        &self,
        repo: &str,
        owner: CloudOwner,
        request: &PublishCloudRequest,
        fingerprint: [u8; 32],
    ) -> Result<Option<CloudPublication>> {
        let record = self
            .db
            .read_tx()
            .get(
                &self.spaces.cloud.receipts,
                receipt_key(repo, owner, &request.idempotency_key),
            )?
            .map(|bytes| decode::<ReceiptRecord>(&bytes))
            .transpose()?;
        record
            .map(|record| {
                if record.format != 1 {
                    return Err(Error::Corrupt("cloud receipt format"));
                }
                if record.fingerprint != fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
                Ok(publication(&request.idempotency_key, &record.version))
            })
            .transpose()
    }
    fn check_version_work(&self, manifest: &Manifest) -> Result<()> {
        // Reject before allocating the loaded-member array. Each object's
        // separately bounded codec worker is additional transient work.
        let raw = manifest.members.iter().try_fold(0u64, |sum, member| {
            sum.checked_add(member.selected.size_bytes)
                .ok_or(Error::Limit("cloud version raw bytes"))
        })?;
        let slots = manifest
            .members
            .len()
            .checked_mul(std::mem::size_of::<VerifiedObject>())
            .ok_or(Error::Limit("cloud version member allocation"))?;
        if raw > self.config.cloud.manifest_limits.max_total_payload_bytes
            || usize::try_from(raw)
                .ok()
                .and_then(|raw| raw.checked_add(slots))
                .is_none_or(|bytes| bytes > self.config.cloud.max_version_decode_bytes)
        {
            return Err(Error::Limit("cloud version decoded object budget"));
        }
        Ok(())
    }
    fn pin_manifest(
        self: &Arc<Self>,
        repo: &str,
        owner: CloudOwner,
        manifest: &RecoveryManifest,
        gate: &mut Runtime,
    ) -> Result<ValidationGuard> {
        self.check_version_work(manifest)?;
        for member in &manifest.members {
            let record = self
                .cloud_object(repo, owner, member.id)?
                .ok_or(Error::Invalid("cloud manifest member unavailable"))?;
            if record.selected.descriptor()?.selected != member.selected {
                return Err(Error::Invalid(
                    "cloud manifest did not select catalog representation",
                ));
            }
        }
        let artifacts = match manifest {
            RecoveryManifest::V1(_) => Vec::new(),
            RecoveryManifest::V2(value) => value
                .index_roots
                .iter()
                .map(|root| {
                    let id = IndexArtifactId::Root(root.id);
                    let record = self
                        .index_record(repo, owner, id)?
                        .ok_or(Error::Invalid("cloud index root unavailable"))?;
                    if record.selected.descriptor()?.selected != root.selected {
                        return Err(Error::Invalid("cloud index selected root mismatch"));
                    }
                    if gate
                        .index_artifact_readers
                        .get(&(repo.into(), owner.principal, id))
                        == Some(&usize::MAX)
                    {
                        return Err(Error::Limit("cloud index root readers"));
                    }
                    Ok(id)
                })
                .collect::<Result<Vec<_>>>()?,
        };
        for key in &manifest.native_roots {
            self.descriptor(repo, *key)?
                .ok_or(Error::MissingObject(*key))?;
        }
        if manifest.members.iter().any(|m| {
            gate.cloud_readers
                .get(&(repo.to_owned(), owner.principal, m.id))
                .is_some_and(|count| *count == usize::MAX)
        }) || manifest.native_roots.iter().any(|key| {
            gate.readers
                .get(&(repo.to_owned(), *key))
                .is_some_and(|count| *count == usize::MAX)
        }) {
            return Err(Error::Limit("cloud validation reader roots"));
        }
        let native = manifest.native_roots.clone();
        let cloud = manifest.members.iter().map(|m| m.id).collect::<Vec<_>>();
        for key in &native {
            *gate.readers.entry((repo.into(), *key)).or_default() += 1;
        }
        for id in &cloud {
            *gate
                .cloud_readers
                .entry((repo.into(), owner.principal, *id))
                .or_default() += 1;
        }
        for id in &artifacts {
            *gate
                .index_artifact_readers
                .entry((repo.into(), owner.principal, *id))
                .or_default() += 1;
        }
        Ok(ValidationGuard {
            native,
            cloud,
            artifacts,
            version: None,
            repo: repo.into(),
            owner,
            inner: self.clone(),
        })
    }
    fn validate_manifest(
        &self,
        repo: &str,
        owner: CloudOwner,
        manifest: &RecoveryManifest,
        limits: ManifestLimits,
        files: Option<bool>,
    ) -> Result<VersionRoots> {
        self.validate_manifest_packing(repo, owner, manifest, limits, files, &HashSet::new())
    }
    fn validate_manifest_packing(
        &self,
        repo: &str,
        owner: CloudOwner,
        manifest: &RecoveryManifest,
        limits: ManifestLimits,
        files: Option<bool>,
        planned: &HashSet<ObjectKey>,
    ) -> Result<VersionRoots> {
        self.validate_manifest_selection(repo, owner, manifest, limits, files, planned, None)
    }
    fn validate_manifest_selection(
        &self,
        repo: &str,
        owner: CloudOwner,
        manifest: &RecoveryManifest,
        limits: ManifestLimits,
        files: Option<bool>,
        planned: &HashSet<ObjectKey>,
        mut selection: Option<&mut SnapshotSelection<'_>>,
    ) -> Result<VersionRoots> {
        self.check_version_work(manifest)?;
        let mut objects = Vec::with_capacity(manifest.members.len());
        let mut retained = objects
            .capacity()
            .checked_mul(std::mem::size_of::<VerifiedObject>())
            .ok_or(Error::Limit("cloud version member allocation"))?;
        if let Some(selected) = selection
            .as_mut()
            .filter(|selected| selected.collect_anchors)
        {
            let charge = selected
                .maximum
                .checked_mul(std::mem::size_of::<CommitId>())
                .and_then(|n| n.checked_add(4096))
                .ok_or(Error::Limit("cloud selected anchors memory"))?;
            retained = retained
                .checked_add(charge)
                .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
                .ok_or(Error::Limit("cloud selected anchors memory"))?;
            selected
                .anchors
                .try_reserve_exact(selected.maximum)
                .map_err(|_| Error::Limit("cloud selected anchors memory"))?;
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        for member in &manifest.members {
            if let Some(selected) = selection.as_ref() {
                selected.check_deadline()?;
            }
            let (record, metadata_scratch) = if matches!(manifest, RecoveryManifest::V2(_)) {
                let key = object_key(repo, owner, member.id);
                let bytes = self
                    .db
                    .read_tx()
                    .get(&self.spaces.cloud.objects, &key)?
                    .ok_or(Error::Corrupt("retained cloud member missing"))?;
                let scratch = bytes
                    .len()
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(Error::Limit("cloud version decoded object budget"))?;
                if retained
                    .checked_add(scratch)
                    .is_none_or(|n| n > self.config.cloud.max_version_decode_bytes)
                {
                    return Err(Error::Limit("cloud version decoded object budget"));
                }
                let record: ObjectRecord = decode(&bytes)?;
                if record.version != 1 || record.selected.descriptor()?.id != member.id {
                    return Err(Error::Corrupt("cloud catalog identity"));
                }
                (record, scratch)
            } else {
                (
                    self.cloud_object(repo, owner, member.id)?
                        .ok_or(Error::Corrupt("retained cloud member missing"))?,
                    0,
                )
            };
            if record.selected.descriptor()?.selected != member.selected {
                return Err(Error::Corrupt(
                    "retained cloud member representation changed",
                ));
            }
            if matches!(manifest, RecoveryManifest::V2(_)) {
                // Admit the actual configured OP codec worker plus its bounded
                // raw input/copy buffers while prior decoded members and this
                // selected-record JSON remain live. Schema1 policy is unchanged.
                let scratch = usize::try_from(member.selected.size_bytes)
                    .ok()
                    .and_then(|n| n.checked_mul(3))
                    .and_then(|n| n.checked_add(self.config.cloud.max_decode_bytes))
                    .and_then(|n| n.checked_add(metadata_scratch))
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(Error::Limit("cloud version decoded object budget"))?;
                if retained
                    .checked_add(scratch)
                    .is_none_or(|n| n > self.config.cloud.max_version_decode_bytes)
                {
                    return Err(Error::Limit("cloud version decoded object budget"));
                }
            }
            let object = self.verify_cloud_backing(repo, owner, &record, &mut scan)?;
            retained = retained
                .checked_add(
                    object
                        .retained_size_upper_bound()
                        .map_err(|_| Error::Limit("cloud retained object allocation"))?,
                )
                .ok_or(Error::Limit("cloud retained object allocation"))?;
            if retained > self.config.cloud.max_version_decode_bytes {
                return Err(Error::Limit("cloud version decoded object budget"));
            }
            objects.push(object);
        }
        let op_retained = retained;
        let mut extra = ExtraWork::default();
        let mut artifacts = Vec::new();
        let mut frozen_retained = 0usize;
        match manifest {
            RecoveryManifest::V1(value) => {
                verify_coverage(value, &objects, limits)
                    .map_err(|_| Error::Invalid("cloud manifest coverage"))?;
            }
            RecoveryManifest::V2(value) => {
                let mut policy = self.v2_limits(limits)?;
                let loaded =
                    self.load_index_artifacts(repo, owner, value, &mut extra, &mut retained)?;
                let spare_slots = policy
                    .artifacts
                    .max_artifacts
                    .checked_mul(std::mem::size_of::<VerifiedIndexArtifact>())
                    .ok_or(Error::Limit("cloud index artifact allocation"))?;
                policy.artifacts.max_decode_bytes = policy.artifacts.max_decode_bytes.min(
                    self.config
                        .cloud
                        .max_version_decode_bytes
                        .checked_sub(op_retained)
                        .and_then(|n| n.checked_sub(spare_slots))
                        .ok_or(Error::Limit("cloud version decoded object budget"))?,
                );
                let graph =
                    verify_index_artifact_graph(&value.index_roots, &loaded, policy.artifacts)
                        .map_err(|error| v2_error(error, "cloud index artifact coverage"))?;
                verify_coverage_v2(value, &objects, &graph, policy)
                    .map_err(|error| v2_error(error, "cloud v2 manifest coverage"))?;
                let mut steps = 0usize;
                for root in &value.index_roots {
                    if let Some(selected) = selection.as_ref() {
                        selected.check_deadline()?;
                    }
                    // The codec charges one full configured snapshot worker.
                    // Previously retained frozen rows must also fit BEFORE
                    // another reconstruction can allocate its model/scratch.
                    let snapshot_peak = loaded
                        .len()
                        .checked_mul(512)
                        .and_then(|n| n.checked_add(4096))
                        .and_then(|n| n.checked_add(policy.artifacts.snapshot.max_decode_bytes))
                        .and_then(|n| n.checked_add(retained))
                        .ok_or(Error::Limit("cloud frozen snapshot reconstruction memory"))?;
                    if snapshot_peak > self.config.cloud.max_version_decode_bytes {
                        return Err(Error::Limit("cloud frozen snapshot reconstruction memory"));
                    }
                    let snapshot = graph
                        .snapshot(root.id)
                        .map_err(|error| v2_error(error, "cloud index logical snapshot"))?;
                    if let Some(selected) = selection.as_mut() {
                        if selected.collect_anchors && selected.ids.binary_search(&root.id).is_ok()
                        {
                            for id in &snapshot.roots {
                                match selected.anchors.binary_search(id) {
                                    Ok(_) => {}
                                    Err(at) => {
                                        if selected.anchors.len() == selected.maximum {
                                            return Err(Error::Limit(
                                                "cloud selected anchor count",
                                            ));
                                        }
                                        selected.anchors.insert(at, *id);
                                    }
                                }
                            }
                        }
                    }
                    // The codec's graph validation already admitted one whole
                    // reconstructed snapshot. Its temporary walk scratch has
                    // dropped; keep the actual owned vectors and the live graph
                    // lookup/decoded artifacts charged while proving ancestry.
                    let graph_lookup = loaded
                        .len()
                        .checked_mul(512)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(Error::Limit("cloud index graph memory"))?;
                    let available = self
                        .config
                        .cloud
                        .max_version_decode_bytes
                        .checked_sub(retained)
                        .and_then(|n| n.checked_sub(graph_lookup))
                        .and_then(|n| n.checked_sub(snapshot_memory(&snapshot).ok()?))
                        .ok_or(Error::Limit("cloud index native proof memory"))?;
                    let proof_deadline = selection.as_ref().and_then(|selected| selected.deadline);
                    let proof_selection = selection
                        .as_deref_mut()
                        .filter(|selected| selected.ids.binary_search(&root.id).is_ok());
                    let frozen_bytes = self.prove_index_native(
                        repo,
                        &snapshot,
                        &mut extra,
                        &mut steps,
                        available,
                        planned,
                        proof_selection,
                        proof_deadline,
                    )?;
                    frozen_retained = frozen_retained
                        .checked_add(frozen_bytes)
                        .ok_or(Error::Limit("cloud frozen index memory"))?;
                    retained = retained
                        .checked_add(frozen_bytes)
                        .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
                        .ok_or(Error::Limit("cloud frozen index memory"))?;
                }
                artifacts = loaded
                    .iter()
                    .map(|artifact| artifact.descriptor())
                    .collect();
                artifacts.sort_by_key(|artifact| artifact.id);
            }
        }
        let wc = &manifest.working_copy;
        if matches!(manifest, RecoveryManifest::V2(_)) {
            // Decoded artifact graphs and snapshot models have dropped here.
            // OP/VIEWs and the selected closure descriptors remain live while
            // validating the WC commit, even for ROOT-only index snapshots.
            let descriptors = artifacts
                .capacity()
                .checked_mul(std::mem::size_of::<IndexArtifactDescriptor>())
                .and_then(|n| n.checked_add(4096))
                .ok_or(Error::Limit("cloud index native proof memory"))?;
            let available = self
                .config
                .cloud
                .max_version_decode_bytes
                .checked_sub(op_retained)
                .and_then(|n| n.checked_sub(frozen_retained))
                .and_then(|n| n.checked_sub(descriptors))
                .ok_or(Error::Limit("cloud index native proof memory"))?;
            let mut memory = ProofMemory {
                used: 0,
                maximum: available,
            };
            let (actual, scratch) = if wc.snapshot_commit == [0; 32] {
                memory.charge(4096)?;
                (tinytree_model::synthetic_root(), 4096)
            } else {
                self.read_index_commit(
                    repo,
                    CommitId(wc.snapshot_commit),
                    None,
                    &mut extra,
                    &mut memory,
                    planned,
                )?
            };
            let matches = actual.root_tree == wc.tree;
            drop(actual);
            memory.release(scratch);
            if !matches {
                return Err(Error::Invalid(
                    "cloud working-copy checkpoint tree mismatch",
                ));
            }
        } else {
            let actual = if wc.snapshot_commit == [0; 32] {
                tinytree_model::synthetic_root()
            } else {
                let key = ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(wc.snapshot_commit),
                };
                let d = self
                    .descriptor(repo, key)?
                    .ok_or(Error::MissingObject(key))?;
                let mut file = self.open_backing(repo, d)?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                tinytree_model::verify(&d, &bytes, self.config.model_limits)?;
                let tinytree_model::Object::Commit(commit) =
                    tinytree_model::decode(&bytes, self.config.model_limits)?
                else {
                    return Err(Error::Corrupt("cloud snapshot commit kind"));
                };
                commit
            };
            if actual.root_tree != wc.tree {
                return Err(Error::Invalid(
                    "cloud working-copy checkpoint tree mismatch",
                ));
            }
        }
        extra.check(self)?;
        if let Some(files) = files {
            self.closure(repo, manifest.native_roots.iter().copied(), true, files)?;
        }
        Ok(VersionRoots {
            native: manifest.native_roots.clone(),
            artifacts,
            extra,
        })
    }
}

impl Inner {
    fn load_index_artifacts(
        &self,
        repo: &str,
        owner: CloudOwner,
        value: &ManifestV2,
        work: &mut ExtraWork,
        retained: &mut usize,
    ) -> Result<Vec<VerifiedIndexArtifact>> {
        let limits = self.index_limits(self.config.cloud.index_artifacts)?;
        let slots = limits
            .max_artifacts
            .checked_mul(std::mem::size_of::<VerifiedIndexArtifact>())
            .ok_or(Error::Limit("cloud index artifact allocation"))?;
        *retained = retained
            .checked_add(slots)
            .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
            .ok_or(Error::Limit("cloud version decoded object budget"))?;
        let mut loader_scratch = 4096usize;
        *retained = retained
            .checked_add(loader_scratch)
            .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
            .ok_or(Error::Limit("cloud index loader memory"))?;
        let mut loaded = Vec::with_capacity(limits.max_artifacts);
        let mut seen = HashSet::new();
        let mut pending = Vec::new();
        for root in &value.index_roots {
            let id = IndexArtifactId::Root(root.id);
            if !seen.contains(&id) {
                if seen.len() >= limits.max_artifacts {
                    return Err(Error::Limit("cloud index artifact count"));
                }
                *retained = retained
                    .checked_add(512)
                    .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
                    .ok_or(Error::Limit("cloud index loader memory"))?;
                loader_scratch = loader_scratch
                    .checked_add(512)
                    .ok_or(Error::Limit("cloud index loader memory"))?;
                seen.insert(id);
                pending.push(id);
            }
        }
        let mut raw = 0u64;
        let mut steps = 0usize;
        while let Some(id) = pending.pop() {
            let key = artifact_key(repo, owner, id);
            let bytes = self
                .db
                .read_tx()
                .get(&self.spaces.cloud.index_artifacts, &key)?
                .ok_or(Error::Invalid("cloud index artifact missing"))?;
            work.row(&key, &bytes)?;
            work.row(&key, &bytes)?;
            work.row(&key, &bytes)?;
            work.row(&key, &bytes)?;
            work.check(self)?;
            let record = self
                .index_record(repo, owner, id)?
                .ok_or(Error::Invalid("cloud index artifact missing"))?;
            let d = record.selected.descriptor()?;
            raw = raw
                .checked_add(d.selected.size_bytes)
                .filter(|n| *n <= limits.max_total_artifact_bytes)
                .ok_or(Error::Limit("cloud index aggregate payload"))?;
            work.raw(d.selected.size_bytes)?;
            work.check(self)?;
            let mut scan = Scan {
                records: 0,
                bytes: 0,
                payload: 0,
            };
            let raw_memory = usize::try_from(d.selected.size_bytes)
                .map_err(|_| Error::Limit("cloud index raw decode memory"))?;
            let available = self
                .config
                .cloud
                .max_version_decode_bytes
                .checked_sub(*retained)
                .and_then(|n| n.checked_sub(raw_memory))
                .filter(|n| *n >= 4096)
                .ok_or(Error::Limit("cloud index decoded allocation"))?;
            let mut decode_limits = limits;
            decode_limits.max_decode_bytes = decode_limits.max_decode_bytes.min(available);
            let artifact =
                self.verify_index_backing_limited(repo, owner, &record, &mut scan, decode_limits)?;
            *retained = retained
                .checked_add(
                    artifact
                        .retained_size_upper_bound()
                        .map_err(|_| Error::Limit("cloud index decoded allocation"))?,
                )
                .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
                .ok_or(Error::Limit("cloud version decoded object budget"))?;
            let children = super::artifacts::artifact_children(artifact.artifact());
            steps = steps
                .checked_add(1)
                .and_then(|n| n.checked_add(children.len()))
                .filter(|n| *n <= limits.max_graph_steps)
                .ok_or(Error::Limit("cloud index graph work"))?;
            for child in children {
                if !seen.contains(&child) {
                    if seen.len() >= limits.max_artifacts {
                        return Err(Error::Limit("cloud index artifact count"));
                    }
                    *retained = retained
                        .checked_add(512)
                        .filter(|n| *n <= self.config.cloud.max_version_decode_bytes)
                        .ok_or(Error::Limit("cloud index loader memory"))?;
                    loader_scratch = loader_scratch
                        .checked_add(512)
                        .ok_or(Error::Limit("cloud index loader memory"))?;
                    seen.insert(child);
                    pending.push(child);
                }
            }
            loaded.push(artifact);
        }
        drop(pending);
        drop(seen);
        *retained = retained
            .checked_sub(loader_scratch)
            .ok_or(Error::Corrupt("cloud index loader memory ownership"))?;
        Ok(loaded)
    }
    fn read_index_commit(
        &self,
        repo: &str,
        id: CommitId,
        expected: Option<Descriptor>,
        work: &mut ExtraWork,
        memory: &mut ProofMemory,
        planned: &HashSet<ObjectKey>,
    ) -> Result<(tinytree_model::Commit, usize)> {
        if id == CommitId::ROOT {
            return Err(Error::Invalid("intrinsic cloud index commit payload"));
        }
        let key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(id.0),
        };
        let physical = super::super::object_key(repo, key);
        let bytes = self
            .db
            .read_tx()
            .get(&self.spaces.objects, &physical)?
            .ok_or(Error::MissingObject(key))?;
        let metadata = bytes
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::Limit("cloud index native proof memory"))?;
        memory.charge(metadata)?;
        work.row(&physical, &bytes)?;
        work.row(&physical, &bytes)?;
        let d = super::super::decode::<StoredDescriptor>(&bytes)?.descriptor()?;
        if d.key != key || d.size_bytes > self.config.model_limits.max_object_bytes as u64 {
            return Err(Error::Corrupt("cloud index native descriptor"));
        }
        if expected.is_some_and(|expected| expected != d) {
            return Err(Error::Invalid("cloud index draft descriptor mismatch"));
        }
        drop(bytes);
        memory.release(metadata);
        let scratch = usize::try_from(d.size_bytes)
            .ok()
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::Limit("cloud index native proof memory"))?;
        memory.charge(scratch)?;
        // Loose metadata has no packed reconstruction allocation. A packed
        // read must additionally own the actual Store's reconstruction policy.
        let packed = if self.is_packed(repo, key)? || planned.contains(&key) {
            usize::try_from(crate::pack::PACK_READ_CHARGE)
                .map_err(|_| Error::Limit("cloud index packed proof memory"))?
        } else {
            0
        };
        memory.charge(packed)?;
        work.raw(d.size_bytes)?;
        work.check(self)?;
        self.check_backing(repo, d)?;
        let mut file = self.open_backing(repo, d)?;
        let mut raw = Vec::new();
        (&mut file).take(d.size_bytes + 1).read_to_end(&mut raw)?;
        tinytree_model::verify(&d, &raw, self.config.model_limits)?;
        let tinytree_model::Object::Commit(commit) =
            tinytree_model::decode(&raw, self.config.model_limits)?
        else {
            return Err(Error::Corrupt("cloud index native commit kind"));
        };
        drop(raw);
        drop(file);
        memory.release(packed);
        // Caller retains this scratch charge until the owned Commit and its
        // parent vector have actually dropped, before admitting another node.
        Ok((commit, scratch))
    }
    fn prove_index_native(
        &self,
        repo: &str,
        snapshot: &tinytree_cloud::PortableIndexSnapshot,
        work: &mut ExtraWork,
        steps: &mut usize,
        available: usize,
        planned: &HashSet<ObjectKey>,
        mut selection: Option<&mut SnapshotSelection<'_>>,
        deadline: Option<std::time::Instant>,
    ) -> Result<usize> {
        let limits = self.config.cloud.index_artifacts;
        let mut frozen_bytes = 0usize;
        let mut memory = ProofMemory {
            used: 0,
            maximum: available,
        };
        let mut anchors = HashSet::new();
        let mut pending = Vec::new();
        // 512 bytes per unique ID covers hash-table load/capacity slack and the
        // pending Vec's amortized capacity (including minimum allocations).
        for id in &snapshot.roots {
            if *id != CommitId::ROOT && !anchors.contains(id) {
                if anchors.len() >= self.config.max_closure_objects {
                    return Err(Error::Limit("cloud index anchor proof"));
                }
                memory.charge(512)?;
                anchors.insert(*id);
                pending.push(*id);
            }
        }
        while let Some(id) = pending.pop() {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::Limit("cloud snapshot proof deadline"));
            }
            let (mut commit, scratch) =
                self.read_index_commit(repo, id, None, work, &mut memory, planned)?;
            *steps = steps
                .checked_add(1)
                .and_then(|n| n.checked_add(commit.parents.len()))
                .filter(|n| *n <= limits.max_graph_steps)
                .ok_or(Error::Limit("cloud index native proof steps"))?;
            let parents = std::mem::take(&mut commit.parents);
            for parent in parents {
                if parent != CommitId::ROOT && !anchors.contains(&parent) {
                    if anchors.len() >= self.config.max_closure_objects {
                        return Err(Error::Limit("cloud index anchor proof"));
                    }
                    memory.charge(512)?;
                    anchors.insert(parent);
                    pending.push(parent);
                }
            }
            drop(commit);
            memory.release(scratch);
        }
        if let Some(selected) = selection.as_mut() {
            if selected.candidate.is_some_and(|id| anchors.contains(&id)) {
                selected.candidate_found = true;
            }
        }
        // The anchor membership set remains live. Admit the separate draft
        // lookup before collect; no allocation is justified by record count
        // alone without the remaining shared version budget.
        memory.charge(
            snapshot
                .drafts
                .len()
                .checked_mul(256)
                .ok_or(Error::Limit("cloud index native proof memory"))?,
        )?;
        let drafts = snapshot
            .drafts
            .iter()
            .map(|draft| draft.commit())
            .collect::<HashSet<_>>();
        for draft in &snapshot.drafts {
            if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                return Err(Error::Limit("cloud snapshot proof deadline"));
            }
            let (mut commit, scratch) = self.read_index_commit(
                repo,
                draft.commit(),
                Some(draft.descriptor),
                work,
                &mut memory,
                planned,
            )?;
            if commit.change_id != draft.change || commit.parents != draft.parents {
                return Err(Error::Invalid(
                    "cloud index draft canonical fields mismatch",
                ));
            }
            *steps = steps
                .checked_add(1)
                .and_then(|n| n.checked_add(draft.parents.len()))
                .filter(|n| *n <= limits.max_graph_steps)
                .ok_or(Error::Limit("cloud index native proof steps"))?;
            for parent in &draft.parents {
                if *parent != CommitId::ROOT
                    && !drafts.contains(parent)
                    && !anchors.contains(parent)
                {
                    return Err(Error::Invalid(
                        "cloud index draft parent outside exact anchors",
                    ));
                }
            }
            if let Some(selected) = selection.as_mut() {
                if selected.candidate == Some(draft.commit()) {
                    selected.candidate_found = true;
                }
            }
            if let Some(selected) = selection
                .as_mut()
                .filter(|selected| selected.collect_frozen)
            {
                let frozen = &mut selected.frozen;
                let millis = commit.committer.timestamp.millis_since_epoch;
                let has_conflict = commit.root_tree.terms.len() > 1;
                let at = frozen
                    .nodes
                    .binary_search_by_key(&draft.commit().0, |node| node.descriptor.key.id.0);
                match at {
                    Ok(at) => {
                        let previous = &mut frozen.nodes[at];
                        if previous.descriptor != draft.descriptor
                            || previous.change != commit.change_id
                            || previous.parents != commit.parents
                            || previous.committer_millis != millis
                            || previous.has_conflict != has_conflict
                        {
                            return Err(Error::Invalid(
                                "cloud selected draft canonical metadata disagrees",
                            ));
                        }
                        previous.order = previous.order.min(draft.order);
                    }
                    Err(at) => {
                        let maximum = limits
                            .snapshot
                            .max_drafts
                            .min(self.config.max_closure_objects);
                        if frozen.nodes.len() >= maximum {
                            return Err(Error::Limit("cloud selected draft count"));
                        }
                        let edges = frozen
                            .edges
                            .checked_add(commit.parents.len())
                            .filter(|n| {
                                *n <= limits.snapshot.max_edges.min(self.config.max_closure_edges)
                            })
                            .ok_or(Error::Limit("cloud selected draft edges"))?;
                        // Binary search is logarithmic; account sorted insertion's
                        // actual shift work across the complete selected union.
                        *steps = steps
                            .checked_add(frozen.nodes.len() - at)
                            .filter(|n| *n <= limits.max_graph_steps)
                            .ok_or(Error::Limit("cloud index native proof steps"))?;
                        let mut growth = 0usize;
                        if frozen.nodes.len() == frozen.nodes.capacity() {
                            let capacity = frozen
                                .nodes
                                .capacity()
                                .saturating_mul(2)
                                .max(4)
                                .min(maximum);
                            let before = frozen.nodes.capacity();
                            // Reallocation can own old and new buffers at once.
                            // Reserve the FULL new buffer before asking Vec to grow.
                            let predicted = capacity
                                .checked_mul(std::mem::size_of::<CloudSnapshotCommit>())
                                .ok_or(Error::Limit("cloud frozen index memory"))?;
                            memory.charge(predicted)?;
                            frozen
                                .nodes
                                .try_reserve_exact(capacity - frozen.nodes.len())
                                .map_err(|_| Error::Limit("cloud frozen index memory"))?;
                            let actual = frozen
                                .nodes
                                .capacity()
                                .checked_mul(std::mem::size_of::<CloudSnapshotCommit>())
                                .ok_or(Error::Limit("cloud frozen index memory"))?;
                            if actual >= predicted {
                                memory.charge(actual - predicted)?;
                            } else {
                                memory.release(predicted - actual);
                            }
                            let old = before
                                .checked_mul(std::mem::size_of::<CloudSnapshotCommit>())
                                .ok_or(Error::Limit("cloud frozen index memory"))?;
                            memory.release(old);
                            growth = actual
                                .checked_sub(old)
                                .ok_or(Error::Corrupt("cloud frozen index capacity"))?;
                        }
                        let parent_bytes = commit
                            .parents
                            .capacity()
                            .checked_mul(std::mem::size_of::<CommitId>())
                            .ok_or(Error::Limit("cloud frozen index memory"))?;
                        memory.charge(parent_bytes)?;
                        frozen_bytes = frozen_bytes
                            .checked_add(growth)
                            .and_then(|n| n.checked_add(parent_bytes))
                            .ok_or(Error::Limit("cloud frozen index memory"))?;
                        frozen.nodes.insert(
                            at,
                            CloudSnapshotCommit {
                                descriptor: draft.descriptor,
                                change: commit.change_id,
                                parents: std::mem::take(&mut commit.parents),
                                committer_millis: millis,
                                has_conflict,
                                order: draft.order,
                            },
                        );
                        frozen.edges = edges;
                    }
                }
            }
            drop(commit);
            memory.release(scratch);
        }
        Ok(frozen_bytes)
    }
}
impl ValidationGuard {
    fn pin_artifacts(
        &mut self,
        descriptors: &[IndexArtifactDescriptor],
        gate: &mut Runtime,
    ) -> Result<()> {
        let existing = self.artifacts.iter().copied().collect::<HashSet<_>>();
        for d in descriptors {
            if self
                .inner
                .index_record(&self.repo, self.owner, d.id)?
                .is_none_or(|record| record.selected.descriptor().ok() != Some(*d))
            {
                return Err(Error::Corrupt("cloud validated artifact changed"));
            }
            if !existing.contains(&d.id)
                && gate
                    .index_artifact_readers
                    .get(&(self.repo.clone(), self.owner.principal, d.id))
                    == Some(&usize::MAX)
            {
                return Err(Error::Limit("cloud artifact readers"));
            }
        }
        for d in descriptors {
            if !existing.contains(&d.id) {
                *gate
                    .index_artifact_readers
                    .entry((self.repo.clone(), self.owner.principal, d.id))
                    .or_default() += 1;
                self.artifacts.push(d.id);
            }
        }
        Ok(())
    }
}
impl Store {
    pub fn get_cloud_workspace(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
    ) -> Result<CloudWorkspace> {
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        self.inner.cloud_workspace(repo, owner, workspace)
    }
    pub fn cloud_publication(
        &self,
        repo: &str,
        owner: CloudOwner,
        key: &str,
    ) -> Result<Option<CloudPublication>> {
        request_token(key)?;
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        self.inner
            .db
            .read_tx()
            .get(
                &self.inner.spaces.cloud.receipts,
                receipt_key(repo, owner, key),
            )?
            .map(|bytes| {
                let record: ReceiptRecord = decode(&bytes)?;
                if record.format != 1 {
                    return Err(Error::Corrupt("cloud receipt format"));
                }
                Ok(publication(key, &record.version))
            })
            .transpose()
    }
    pub fn read_cloud_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        limits: ManifestLimits,
    ) -> Result<CloudVersionReadGuard> {
        self.read_cloud_version_checked(repo, owner, workspace, generation, limits, false)?
            .ok_or(Error::Corrupt("cloud version schema dispatch"))
    }
    /// Read only schema1. None denotes an indexed schema2 version without
    /// decoding its artifact graph; metadata lookup is not a retention proof.
    pub fn read_cloud_version_v1(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        limits: ManifestLimits,
    ) -> Result<Option<CloudVersionReadGuard>> {
        self.read_cloud_version_checked(repo, owner, workspace, generation, limits, true)
    }
    fn read_cloud_version_checked(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        limits: ManifestLimits,
        v1_only: bool,
    ) -> Result<Option<CloudVersionReadGuard>> {
        self.read_cloud_version_selection(repo, owner, workspace, generation, limits, v1_only, None)
    }
    fn read_cloud_version_selection(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        limits: ManifestLimits,
        v1_only: bool,
        mut selection: Option<&mut SnapshotSelection<'_>>,
    ) -> Result<Option<CloudVersionReadGuard>> {
        let limits = self.inner.manifest_limits(limits)?;
        let (record, manifest_bytes, manifest, mut guard, indexed) = {
            let mut gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            let key = version_key(repo, owner, workspace, generation);
            let bytes = self
                .inner
                .db
                .read_tx()
                .get(&self.inner.spaces.cloud.versions, &key)?
                .ok_or(Error::Invalid("cloud version unavailable"))?;
            let record: VersionRecord = decode(&bytes)?;
            if ![1, 2].contains(&record.format)
                || record.workspace != workspace
                || record.generation != generation
                || generation == 0
            {
                return Err(Error::Corrupt("cloud version identity"));
            }
            if v1_only && record.format == 2 {
                return Ok(None);
            }
            let id = ManifestId(record.manifest);
            let bytes = self
                .inner
                .db
                .read_tx()
                .get(
                    &self.inner.spaces.cloud.manifests,
                    manifest_key(repo, owner, id),
                )?
                .ok_or(Error::Corrupt("cloud version manifest missing"))?
                .to_vec();
            let manifest = self
                .inner
                .decode_recovery(&bytes, id, limits)
                .map_err(|_| Error::Corrupt("cloud version manifest"))?;
            if manifest.format() != record.format {
                return Err(Error::Corrupt("cloud version manifest format"));
            }
            if let Some(selected) = selection.as_ref() {
                let RecoveryManifest::V2(indexed) = &manifest else {
                    return Err(Error::Invalid("cloud snapshots require indexed version"));
                };
                if selected.ids.iter().any(|id| {
                    indexed
                        .index_roots
                        .binary_search_by_key(id, |root| root.id)
                        .is_err()
                }) {
                    return Err(Error::Invalid("cloud selected snapshot outside version"));
                }
            }
            if manifest.workspace != workspace {
                return Err(Error::Corrupt("cloud version workspace"));
            }
            let roots = self
                .inner
                .db
                .read_tx()
                .get(&self.inner.spaces.cloud.roots, &key)?
                .ok_or(Error::Corrupt("cloud version roots missing"))?;
            let indexed = decode_version_roots(
                &roots,
                id,
                limits.max_native_roots,
                self.inner.config.cloud.index_artifacts.max_artifacts,
                record.format,
            )?;
            if indexed.native != manifest.native_roots {
                return Err(Error::Corrupt("cloud version native roots"));
            }
            let reader = (repo.to_owned(), owner.principal, workspace, generation);
            if gate.cloud_version_readers.get(&reader) == Some(&usize::MAX) {
                return Err(Error::Limit("cloud version readers"));
            }
            let mut guard = self.inner.pin_manifest(repo, owner, &manifest, &mut gate)?;
            *gate.cloud_version_readers.entry(reader).or_default() += 1;
            guard.version = Some((workspace, generation));
            (record, bytes, manifest, guard, indexed)
        };
        let verified = self.inner.validate_manifest_selection(
            repo,
            owner,
            &manifest,
            limits,
            Some(false),
            &HashSet::new(),
            selection.as_deref_mut(),
        )?;
        if verified.artifacts != indexed.artifacts || verified.extra != indexed.extra {
            return Err(Error::Corrupt("cloud version artifact proof"));
        }
        {
            let mut gate = self.inner.lock()?;
            guard.pin_artifacts(&verified.artifacts, &mut gate)?;
        }
        Ok(Some(CloudVersionReadGuard {
            version: version(&record),
            manifest: manifest_bytes,
            _guard: guard,
        }))
    }
    pub fn publish_cloud_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        request: &PublishCloudRequest,
        now: i64,
        limits: &CloudPublicationLimits,
    ) -> Result<CloudPublication> {
        request_token(&request.idempotency_key)?;
        limits.retention.validate()?;
        if request.manifest_bytes.len() > CLOUD_BINARY_ROW_BYTES
            || request.staging_leases.len() > self.inner.config.max_closure_objects
            || request
                .staging_leases
                .iter()
                .any(|lease| lease.len() != 64 || !lease.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(Error::Limit("cloud publication request"));
        }
        let fingerprint = fingerprint(request);
        {
            let _gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(receipt) = self.inner.cloud_replay(repo, owner, request, fingerprint)? {
                return Ok(receipt);
            }
        }
        let policy = self.inner.manifest_limits(limits.manifest)?;
        let manifest =
            self.inner
                .decode_recovery(&request.manifest_bytes, request.manifest, policy)?;
        if manifest.workspace != request.workspace {
            return Err(Error::Invalid("cloud manifest workspace"));
        }
        let mut guard = {
            let mut gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(receipt) = self.inner.cloud_replay(repo, owner, request, fingerprint)? {
                return Ok(receipt);
            }
            let current = self.inner.cloud_workspace(repo, owner, request.workspace)?;
            if current.generation != request.expected_generation {
                return Err(Error::StaleCloudWorkspace(current));
            }
            self.inner.pin_manifest(repo, owner, &manifest, &mut gate)?
        };
        let roots = self
            .inner
            .validate_manifest(repo, owner, &manifest, policy, Some(true))?;
        let mut _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        if let Some(receipt) = self.inner.cloud_replay(repo, owner, request, fingerprint)? {
            return Ok(receipt);
        }
        let current = self.inner.cloud_workspace(repo, owner, request.workspace)?;
        if current.generation != request.expected_generation {
            return Err(Error::StaleCloudWorkspace(current));
        }
        if current.manifest == Some(request.manifest) {
            return Err(Error::Invalid("cloud version repeats the current manifest"));
        }
        let generation = current
            .generation
            .checked_add(1)
            .ok_or(Error::Limit("cloud workspace generation"))?;
        if generation > self.inner.config.cloud.max_versions_per_workspace {
            return Err(Error::Limit("cloud retained version quota"));
        }
        // Immutable selected descriptors and native root presence are rechecked at
        // publication. The validation guard has protected them from collection.
        let mut member_metadata_work = 0u64;
        for member in &manifest.members {
            let member_key = object_key(repo, owner, member.id);
            let member_bytes = self
                .inner
                .db
                .read_tx()
                .get(&self.inner.spaces.cloud.objects, &member_key)?
                .ok_or(Error::Corrupt("validated cloud member disappeared"))?;
            member_metadata_work = member_metadata_work
                .checked_add((member_key.len() + member_bytes.len()) as u64 * 2)
                .ok_or(Error::Limit("cloud version validation metadata"))?;
            let selected = self
                .inner
                .cloud_object(repo, owner, member.id)?
                .ok_or(Error::Corrupt("validated cloud member disappeared"))?
                .selected
                .descriptor()?;
            if selected.selected != member.selected {
                return Err(Error::Corrupt("validated cloud member changed"));
            }
        }
        guard.pin_artifacts(&roots.artifacts, &mut _gate)?;
        for key in &roots.native {
            self.inner
                .descriptor(repo, *key)?
                .ok_or(Error::MissingObject(*key))?;
        }
        let mut changes = Vec::new();
        let view = self.inner.db.read_tx();
        let manifest_key = manifest_key(repo, owner, request.manifest);
        match view.get(&self.inner.spaces.cloud.manifests, &manifest_key)? {
            Some(old) if old.as_ref() != request.manifest_bytes.as_slice() => {
                return Err(Error::Corrupt("cloud manifest digest collision"));
            }
            Some(_) => {}
            None => changes.push((
                &self.inner.spaces.cloud.manifests,
                manifest_key,
                Some(request.manifest_bytes.clone()),
            )),
        }
        for parent in &manifest.parents {
            if view
                .get(
                    &self.inner.spaces.cloud.manifests,
                    manifest_key_for(repo, owner, *parent),
                )?
                .is_none()
            {
                return Err(Error::Invalid("cloud manifest parent unavailable"));
            }
        }
        let mut seen = HashSet::new();
        let mut consumed = 0i128;
        for lease in &request.staging_leases {
            if !seen.insert(lease) {
                return Err(Error::Invalid("duplicate cloud staging lease"));
            }
            let mut key = scope(repo, owner);
            key.extend_from_slice(lease.as_bytes());
            let ordinary = view.get(&self.inner.spaces.cloud.staging, &key)?;
            let artifact = view.get(&self.inner.spaces.cloud.index_staging, &key)?;
            match (ordinary, artifact) {
                (Some(bytes), None) => {
                    let stage: StageRecord = decode(&bytes)?;
                    let selected = stage.selected.descriptor()?;
                    if stage.expires <= now {
                        return Err(Error::Invalid("cloud staging lease expired"));
                    }
                    if !manifest
                        .members
                        .binary_search_by_key(&selected.id, |member| member.id)
                        .ok()
                        .is_some_and(|index| manifest.members[index].selected == selected.selected)
                    {
                        return Err(Error::Invalid("unrelated cloud staging lease"));
                    }
                    changes.push((&self.inner.spaces.cloud.staging, key, None));
                }
                (None, Some(bytes)) => {
                    let stage: IndexStage = decode(&bytes)?;
                    let selected = stage.selected.descriptor()?;
                    if stage.expires <= now {
                        return Err(Error::Invalid("cloud staging lease expired"));
                    }
                    if !roots
                        .artifacts
                        .binary_search_by_key(&selected.id, |artifact| artifact.id)
                        .ok()
                        .is_some_and(|index| roots.artifacts[index] == selected)
                    {
                        return Err(Error::Invalid("unrelated cloud index staging lease"));
                    }
                    changes.push((&self.inner.spaces.cloud.index_staging, key, None));
                }
                (Some(_), Some(_)) => return Err(Error::Corrupt("ambiguous cloud staging lease")),
                (None, None) => return Err(Error::Invalid("cloud staging lease unavailable")),
            }
            consumed += 1;
        }
        self.inner.check_cloud_promise_quota(
            repo,
            owner,
            &manifest,
            &roots.artifacts,
            &limits.retention,
        )?;
        let version = VersionRecord {
            format: manifest.format(),
            workspace: request.workspace,
            generation,
            manifest: request.manifest.0,
            previous: current.manifest.map(|id| id.0),
        };
        let version_key = version_key(repo, owner, request.workspace, generation);
        if view
            .get(&self.inner.spaces.cloud.versions, &version_key)?
            .is_some()
            || view
                .get(&self.inner.spaces.cloud.roots, &version_key)?
                .is_some()
        {
            return Err(Error::Corrupt("cloud version slot already occupied"));
        }
        changes.push((
            &self.inner.spaces.cloud.workspaces,
            workspace_key(repo, owner, request.workspace),
            Some(encode(&WorkspaceRecord {
                generation,
                manifest: request.manifest.0,
            })?),
        ));
        changes.push((
            &self.inner.spaces.cloud.versions,
            version_key.clone(),
            Some(encode(&version)?),
        ));
        changes.push((
            &self.inner.spaces.cloud.roots,
            version_key,
            Some(encode_version_roots(
                request.manifest,
                &roots,
                manifest.format(),
            )?),
        ));
        changes.push((
            &self.inner.spaces.cloud.receipts,
            receipt_key(repo, owner, &request.idempotency_key),
            Some(encode(&ReceiptRecord {
                format: 1,
                fingerprint,
                version: version.clone(),
            })?),
        ));
        self.inner.cloud_namespace(repo, owner, true)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        advance_feature(
            &mut tx,
            &self.inner.spaces.meta,
            if manifest.format() == 2 { 4 } else { 3 },
        )?;
        let payload_work = manifest.members.iter().try_fold(0u64, |sum, member| {
            sum.checked_add(member.selected.size_bytes)
                .ok_or(Error::Limit("cloud version validation bytes"))
        })?;
        let payload_work = payload_work
            .checked_add(roots.extra.payload)
            .ok_or(Error::Limit("cloud version validation bytes"))?;
        let metadata_work = (version_key_for(repo, owner, request.workspace, generation).len() * 2
            + manifest_key_for(repo, owner, request.manifest).len()
            + encode(&version)?.len()
            + encode_version_roots(request.manifest, &roots, manifest.format())?.len()
            + request.manifest_bytes.len()) as u64;
        let metadata_work = metadata_work
            .checked_add(member_metadata_work)
            .and_then(|n| n.checked_add(roots.extra.metadata))
            .ok_or(Error::Limit("cloud version validation metadata"))?;
        self.inner.cloud_changes(
            &mut tx,
            repo,
            owner,
            changes,
            Delta {
                stages: -consumed,
                workspaces: i128::from(current.generation == 0),
                versions: 1,
                receipts: 1,
                version_payload_bytes: payload_work as i128,
                version_metadata_bytes: metadata_work as i128,
                version_records: 3
                    + 2 * manifest.members.len() as i128
                    + roots.extra.records as i128,
                ..Default::default()
            },
        )?;
        self.inner.commit(tx)?;
        drop(_gate);
        drop(guard);
        Ok(publication(&request.idempotency_key, &version))
    }
}
fn manifest_key_for(repo: &str, owner: CloudOwner, id: ManifestId) -> Vec<u8> {
    manifest_key(repo, owner, id)
}

pub(super) struct RetainedPlan {
    pub native: HashSet<ObjectKey>,
    pub cloud: HashSet<(CloudOwner, CloudId)>,
    pub index: HashSet<(CloudOwner, IndexArtifactId)>,
    blobs: HashMap<(CloudOwner, [u8; 32]), u64>,
    versions: HashMap<Vec<u8>, VersionRecord>,
    current: HashMap<Vec<u8>, VersionRecord>,
    manifests: HashSet<Vec<u8>>,
    payload_work: HashMap<Vec<u8>, u64>,
    metadata_work: HashMap<Vec<u8>, u64>,
    record_work: HashMap<Vec<u8>, u64>,
}
impl Inner {
    pub(super) fn retained_cloud_plan(
        &self,
        repo: Option<&str>,
        owner: Option<CloudOwner>,
        validate: bool,
        scan: &mut Scan,
    ) -> Result<RetainedPlan> {
        self.retained_cloud_plan_packing(repo, owner, validate, scan, &HashSet::new())
    }
    fn retained_cloud_plan_packing(
        &self,
        repo: Option<&str>,
        owner: Option<CloudOwner>,
        validate: bool,
        scan: &mut Scan,
        planned: &HashSet<ObjectKey>,
    ) -> Result<RetainedPlan> {
        let mut plan = RetainedPlan {
            native: HashSet::new(),
            cloud: HashSet::new(),
            index: HashSet::new(),
            blobs: HashMap::new(),
            versions: HashMap::new(),
            current: HashMap::new(),
            manifests: HashSet::new(),
            payload_work: HashMap::new(),
            metadata_work: HashMap::new(),
            record_work: HashMap::new(),
        };
        let prefix = match (repo, owner) {
            (Some(repo), Some(owner)) => scope(repo, owner),
            (Some(repo), None) => repo_prefix(repo),
            (None, None) => Vec::new(),
            _ => return Err(Error::Invalid("cloud plan scope")),
        };
        let view = self.db.read_tx();
        let limits = self.manifest_limits(self.config.cloud.manifest_limits)?;
        let mut native_by_repo = HashMap::<String, HashSet<ObjectKey>>::new();
        for item in view.prefix(&self.spaces.cloud.versions, prefix) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (actual, principal, suffix) = parse_scope(&key)?;
            self.repository(actual)?;
            if suffix.len() != 40 {
                return Err(Error::Corrupt("cloud version key"));
            }
            let record: VersionRecord = decode(&bytes)?;
            let workspace: [u8; 32] = suffix[..32].try_into().unwrap();
            let generation = u64::from_be_bytes(suffix[32..].try_into().unwrap());
            if ![1, 2].contains(&record.format)
                || record.workspace != workspace
                || record.generation != generation
                || generation == 0
            {
                return Err(Error::Corrupt("cloud version identity"));
            }
            let workspace_key = workspace_key(actual, principal, workspace);
            let previous = plan.current.get(&workspace_key);
            if generation != previous.map_or(1, |old| old.generation + 1)
                || record.previous != previous.map(|old| old.manifest)
            {
                return Err(Error::Corrupt("cloud version history gap"));
            }
            let id = ManifestId(record.manifest);
            let manifest_key = manifest_key(actual, principal, id);
            let manifest_bytes = view
                .get(&self.spaces.cloud.manifests, &manifest_key)?
                .ok_or(Error::Corrupt("cloud manifest missing"))?;
            scan.visit(self, &manifest_key, &manifest_bytes)?;
            let manifest = self
                .decode_recovery(&manifest_bytes, id, limits)
                .map_err(|_| Error::Corrupt("cloud manifest encoding"))?;
            if manifest.format() != record.format {
                return Err(Error::Corrupt("cloud manifest record format"));
            }
            if manifest.workspace != workspace {
                return Err(Error::Corrupt("cloud manifest workspace"));
            }
            self.check_version_work(&manifest)?;
            let root_bytes = view
                .get(&self.spaces.cloud.roots, &key)?
                .ok_or(Error::Corrupt("cloud version roots missing"))?;
            scan.visit(self, &key, &root_bytes)?;
            let indexed = decode_version_roots(
                &root_bytes,
                id,
                limits.max_native_roots,
                self.config.cloud.index_artifacts.max_artifacts,
                record.format,
            )?;
            if indexed.native != manifest.native_roots {
                return Err(Error::Corrupt("cloud native root index changed"));
            }
            for root in indexed.native.iter().copied() {
                plan.native.insert(root);
                if planned.is_empty() || record.format == 2 {
                    native_by_repo
                        .entry(actual.to_owned())
                        .or_default()
                        .insert(root);
                }
            }
            let mut payload_work = 0u64;
            let mut member_metadata_work = 0u64;
            for member in &manifest.members {
                let member_key = object_key(actual, principal, member.id);
                let member_bytes = view
                    .get(&self.spaces.cloud.objects, &member_key)?
                    .ok_or(Error::Corrupt("retained cloud member missing"))?;
                // Descriptor selection and the subsequent decoded-DAG verifier
                // each read this bounded row. Charge both passes prospectively.
                scan.visit(self, &member_key, &member_bytes)?;
                scan.visit(self, &member_key, &member_bytes)?;
                member_metadata_work = member_metadata_work
                    .checked_add((member_key.len() + member_bytes.len()) as u64 * 2)
                    .ok_or(Error::Limit("cloud version validation metadata"))?;
                let record: ObjectRecord = decode(&member_bytes)?;
                let selected = record.selected.descriptor()?;
                if selected.selected != member.selected {
                    return Err(Error::Corrupt(
                        "retained cloud member representation changed",
                    ));
                }
                plan.cloud.insert((principal, member.id));
                if let Some(old) = plan.blobs.insert(
                    (principal, member.selected.blake3),
                    member.selected.size_bytes,
                ) {
                    if old != member.selected.size_bytes {
                        return Err(Error::Corrupt("cloud promised blob length"));
                    }
                }
                // Reopen/GC revalidates each version's declared complete DAG.
                // Publication accounts this repeated work prospectively too.
                scan.payload(self, member.selected.size_bytes)?;
                payload_work = payload_work
                    .checked_add(member.selected.size_bytes)
                    .ok_or(Error::Limit("cloud version validation bytes"))?;
            }
            if validate && (planned.is_empty() || record.format == 2) {
                let verified = self.validate_manifest_packing(
                    actual, principal, &manifest, limits, None, planned,
                )?;
                if verified.artifacts != indexed.artifacts || verified.extra != indexed.extra {
                    return Err(Error::Corrupt("cloud indexed version proof mismatch"));
                }
            }
            for d in &indexed.artifacts {
                if self
                    .index_record(actual, principal, d.id)?
                    .is_none_or(|record| record.selected.descriptor().ok() != Some(*d))
                {
                    return Err(Error::Corrupt("cloud retained artifact selection"));
                }
                plan.index.insert((principal, d.id));
                if plan
                    .blobs
                    .insert((principal, d.selected.blake3), d.selected.size_bytes)
                    .is_some_and(|old| old != d.selected.size_bytes)
                {
                    return Err(Error::Corrupt("cloud index promised blob length"));
                }
            }
            indexed.extra.check(self)?;
            indexed.extra.charge(self, scan)?;
            payload_work = payload_work
                .checked_add(indexed.extra.payload)
                .ok_or(Error::Limit("cloud version validation bytes"))?;
            plan.payload_work.insert(key.to_vec(), payload_work);
            let metadata_work = (key.len() * 2
                + manifest_key.len()
                + bytes.len()
                + root_bytes.len()
                + manifest_bytes.len()) as u64;
            plan.metadata_work.insert(
                key.to_vec(),
                metadata_work
                    .checked_add(member_metadata_work)
                    .and_then(|n| n.checked_add(indexed.extra.metadata))
                    .ok_or(Error::Limit("cloud version validation metadata"))?,
            );
            plan.record_work.insert(
                key.to_vec(),
                3 + 2 * manifest.members.len() as u64 + indexed.extra.records,
            );
            plan.manifests.insert(manifest_key);
            plan.current.insert(workspace_key, record.clone());
            plan.versions.insert(key.to_vec(), record);
        }
        if validate {
            for (repo, roots) in native_by_repo {
                self.closure(&repo, roots, true, false)?;
            }
        }
        Ok(plan)
    }
    /// Caller owns the native maintenance/GC gate. Validate the exact future
    /// reconstruction charge before writing any new pack or location journal.
    pub(crate) fn validate_cloud_pack(
        &self,
        repo: &str,
        planned: &HashSet<ObjectKey>,
    ) -> Result<()> {
        if planned.is_empty() {
            return Ok(());
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let mut indexed = false;
        for entry in self
            .db
            .read_tx()
            .prefix(&self.spaces.cloud.versions, repo_prefix(repo))
        {
            let (key, bytes) = entry.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let record: VersionRecord = decode(&bytes)?;
            if record.format == 2 {
                indexed = true;
                break;
            }
        }
        if !indexed {
            return Ok(());
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        self.retained_cloud_plan_packing(Some(repo), None, true, &mut scan, planned)?;
        Ok(())
    }
    fn legacy_principal_roots(
        &self,
        repo: &str,
        principal: [u8; 32],
        maximum: usize,
    ) -> Result<Vec<ObjectKey>> {
        let prefix = key(
            repo,
            format!("_tinytree/holds/v1/{}/", hex(&principal)).as_bytes(),
        );
        let base = key(repo, b"");
        let view = self.db.read_tx();
        let mut roots = Vec::new();
        for (count, item) in view.prefix(&self.spaces.refs, prefix).enumerate() {
            if count >= maximum {
                return Err(Error::Limit("cloud legacy root scan"));
            }
            let (key, bytes) = item.into_inner()?;
            let suffix = key
                .as_ref()
                .strip_prefix(base.as_slice())
                .ok_or(Error::Corrupt("legacy root key"))?;
            let name =
                std::str::from_utf8(suffix).map_err(|_| Error::Corrupt("legacy root name"))?;
            let (owner, target) =
                parse_retention_reference(name).ok_or(Error::Corrupt("legacy root name"))?;
            if owner.principal != principal {
                return Err(Error::Corrupt("legacy root principal"));
            }
            if let Some(selected) = decode::<StoredReference>(&bytes)?.commit {
                let d = selected.descriptor()?;
                if d.key.kind != Kind::Commit || d.key.id.0 != target.0 {
                    return Err(Error::Corrupt("legacy root target"));
                }
                roots.push(d.key);
            }
        }
        Ok(roots)
    }
    pub(crate) fn cloud_principal_promises(
        &self,
        repo: &str,
        principal: [u8; 32],
        maximum: usize,
    ) -> Result<(Vec<ObjectKey>, u64)> {
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let plan =
            self.retained_cloud_plan(Some(repo), Some(CloudOwner { principal }), false, &mut scan)?;
        if plan.native.len() > maximum
            || plan.cloud.len().saturating_add(plan.index.len()) > maximum
        {
            return Err(Error::Limit("cloud promise quota scan"));
        }
        let bytes = plan.blobs.values().try_fold(0u64, |sum, size| {
            sum.checked_add(*size)
                .ok_or(Error::Limit("cloud promised bytes"))
        })?;
        Ok((plan.native.into_iter().collect(), bytes))
    }
    /// Check the actual strict server-GC post-state. Only cloud publication
    /// introduces this policy; legacy-only repositories retain their behavior.
    pub(crate) fn check_cloud_gc_union(
        &self,
        repo: &str,
        added: &[ObjectKey],
        replacement: Option<(&str, ObjectKey)>,
    ) -> Result<()> {
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let plan = self.retained_cloud_plan(Some(repo), None, false, &mut scan)?;
        if added.is_empty() && plan.versions.is_empty() {
            return Ok(());
        }
        let mut roots = plan.native;
        roots.insert(tinytree_model::empty_tree().descriptor.key);
        roots.extend(added.iter().copied());
        let changed_key = replacement.map(|(name, _)| key(repo, name.as_bytes()));
        let view = self.db.read_tx();
        for item in view.prefix(&self.spaces.refs, key(repo, b"")) {
            let (reference_key, bytes) = item.into_inner()?;
            scan.visit(self, &reference_key, &bytes)?;
            if changed_key.as_deref() == Some(reference_key.as_ref()) {
                continue;
            }
            if let Some(selected) = super::super::decode::<StoredReference>(&bytes)?.commit {
                let descriptor = selected.descriptor()?;
                if descriptor.key.kind != Kind::Commit {
                    return Err(Error::Corrupt("reference target type"));
                }
                self.require_descriptor(repo, descriptor)?;
                roots.insert(descriptor.key);
            }
        }
        if let Some((_, target)) = replacement {
            roots.insert(target);
        }
        self.closure(repo, roots, true, false)?;
        Ok(())
    }
    fn check_cloud_promise_quota(
        &self,
        repo: &str,
        owner: CloudOwner,
        manifest: &RecoveryManifest,
        artifacts: &[IndexArtifactDescriptor],
        limits: &RetentionLimits,
    ) -> Result<()> {
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        self.check_cloud_gc_union(repo, &manifest.native_roots, None)?;
        let mut plan = self.retained_cloud_plan(Some(repo), Some(owner), false, &mut scan)?;
        plan.native.extend(manifest.native_roots.iter().copied());
        plan.native.extend(self.legacy_principal_roots(
            repo,
            owner.principal,
            limits.max_quota_scan_roots,
        )?);
        for member in &manifest.members {
            plan.cloud.insert((owner, member.id));
            if let Some(old) = plan
                .blobs
                .insert((owner, member.selected.blake3), member.selected.size_bytes)
            {
                if old != member.selected.size_bytes {
                    return Err(Error::Corrupt("cloud promised blob length"));
                }
            }
        }
        for d in artifacts {
            plan.index.insert((owner, d.id));
            if plan
                .blobs
                .insert((owner, d.selected.blake3), d.selected.size_bytes)
                .is_some_and(|old| old != d.selected.size_bytes)
            {
                return Err(Error::Corrupt("cloud index promised blob length"));
            }
        }
        if plan.native.len() > limits.max_quota_scan_roots
            || plan.cloud.len().saturating_add(plan.index.len()) > limits.max_quota_scan_roots
        {
            return Err(Error::Limit("cloud promised root quota scan"));
        }
        let closure = self.closure(repo, plan.native, true, false)?;
        let mut bytes = plan.blobs.values().try_fold(0u64, |sum, size| {
            sum.checked_add(*size)
                .ok_or(Error::Limit("cloud promised bytes"))
        })?;
        for key in closure {
            bytes = bytes
                .checked_add(
                    self.descriptor(repo, key)?
                        .ok_or(Error::MissingObject(key))?
                        .size_bytes,
                )
                .ok_or(Error::Limit("cloud promised bytes"))?;
        }
        if bytes > limits.max_unique_bytes_per_principal {
            return Err(Error::Limit("principal promised closure byte quota"));
        }
        Ok(())
    }
    pub(super) fn validate_cloud_versions_on_reopen(
        &self,
        expected: &mut HashMap<Vec<u8>, Accounting>,
        scan: &mut Scan,
    ) -> Result<()> {
        let plan = self.retained_cloud_plan(None, None, true, scan)?;
        let view = self.db.read_tx();
        let mut receipts = HashSet::new();
        for (index, space) in [
            (3, &self.spaces.cloud.manifests),
            (4, &self.spaces.cloud.workspaces),
            (5, &self.spaces.cloud.versions),
            (6, &self.spaces.cloud.roots),
            (7, &self.spaces.cloud.receipts),
        ] {
            for item in view.iter(space) {
                let (key, bytes) = item.into_inner()?;
                scan.visit(self, &key, &bytes)?;
                let (repo, owner, suffix) = parse_scope(&key)?;
                self.repository(repo)?;
                let account = expected.entry(scope(repo, owner)).or_default();
                metadata_account(account, &key, &bytes)?;
                match index {
                    3 => {
                        if suffix.len() != 32 || !plan.manifests.contains(key.as_ref()) {
                            return Err(Error::Corrupt("unreferenced cloud manifest"));
                        }
                    }
                    4 => {
                        if suffix.len() != 32 {
                            return Err(Error::Corrupt("cloud workspace key"));
                        }
                        let record: WorkspaceRecord = decode(&bytes)?;
                        let version = plan
                            .current
                            .get(key.as_ref())
                            .ok_or(Error::Corrupt("cloud workspace version missing"))?;
                        if record.generation != version.generation
                            || record.manifest != version.manifest
                        {
                            return Err(Error::Corrupt("cloud workspace current mismatch"));
                        }
                        adjust(&mut account.workspaces, 1)?;
                    }
                    5 => {
                        let record = plan
                            .versions
                            .get(key.as_ref())
                            .ok_or(Error::Corrupt("cloud version key"))?;
                        adjust(&mut account.versions, 1)?;
                        adjust(
                            &mut account.version_payload_bytes,
                            *plan
                                .payload_work
                                .get(key.as_ref())
                                .ok_or(Error::Corrupt("cloud version validation bytes"))?
                                as i128,
                        )?;
                        adjust(
                            &mut account.version_metadata_bytes,
                            *plan
                                .metadata_work
                                .get(key.as_ref())
                                .ok_or(Error::Corrupt("cloud version validation metadata"))?
                                as i128,
                        )?;
                        adjust(
                            &mut account.version_records,
                            *plan
                                .record_work
                                .get(key.as_ref())
                                .ok_or(Error::Corrupt("cloud version validation records"))?
                                as i128,
                        )?;
                        let _ = record;
                    }
                    6 => {
                        if !plan.versions.contains_key(key.as_ref()) {
                            return Err(Error::Corrupt("unreferenced cloud native roots"));
                        }
                    }
                    7 => {
                        let name = std::str::from_utf8(suffix)
                            .map_err(|_| Error::Corrupt("cloud receipt key"))?;
                        request_token(name)?;
                        let record: ReceiptRecord = decode(&bytes)?;
                        let target = version_key(
                            repo,
                            owner,
                            record.version.workspace,
                            record.version.generation,
                        );
                        let actual = plan
                            .versions
                            .get(&target)
                            .ok_or(Error::Corrupt("cloud receipt version missing"))?;
                        if record.format != 1
                            || encode(actual)? != encode(&record.version)?
                            || !receipts.insert(target)
                        {
                            return Err(Error::Corrupt("cloud receipt replay history"));
                        }
                        adjust(&mut account.receipts, 1)?;
                    }
                    _ => unreachable!(),
                }
            }
        }
        if receipts.len() != plan.versions.len() {
            return Err(Error::Corrupt("cloud version receipt missing"));
        }
        for key in plan.current.keys() {
            if view.get(&self.spaces.cloud.workspaces, key)?.is_none() {
                return Err(Error::Corrupt("cloud workspace current missing"));
            }
        }
        Ok(())
    }
}

fn version_key_for(repo: &str, owner: CloudOwner, workspace: [u8; 32], generation: u64) -> Vec<u8> {
    version_key(repo, owner, workspace, generation)
}

struct SnapshotSelection<'a> {
    ids: &'a [IndexRootId],
    maximum: usize,
    anchors: Vec<CommitId>,
    frozen: FrozenCommits,
    collect_anchors: bool,
    collect_frozen: bool,
    candidate: Option<CommitId>,
    candidate_found: bool,
    deadline: Option<std::time::Instant>,
}
impl SnapshotSelection<'_> {
    fn check_deadline(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(Error::Limit("cloud snapshot proof deadline"));
        }
        Ok(())
    }
}
#[derive(Default)]
struct FrozenCommits {
    nodes: Vec<CloudSnapshotCommit>,
    edges: usize,
}
/// Canonically verified draft metadata from the exact selected snapshots.
/// Original draft order is retained; these commits are not remote anchors.
#[derive(Debug, Eq, PartialEq)]
pub struct CloudSnapshotCommit {
    pub descriptor: Descriptor,
    pub change: tinytree_model::ChangeId,
    pub parents: Vec<CommitId>,
    pub committer_millis: i64,
    pub has_conflict: bool,
    pub order: u64,
}
/// Compact authority for one exact selected snapshot union. Decoded artifact
/// graphs and raw manifests are intentionally not retained between requests.
pub struct CloudSnapshotReadGuard {
    version: CloudVersion,
    selector: super::CloudVersionSelector,
    selected: Vec<IndexRootId>,
    anchors: Vec<CommitId>,
    frozen: Vec<CloudSnapshotCommit>,
    pins: ValidationGuard,
}
impl CloudSnapshotReadGuard {
    pub fn version(&self) -> &CloudVersion {
        &self.version
    }
    pub fn selector(&self) -> super::CloudVersionSelector {
        self.selector
    }
    pub fn selected_roots(&self) -> &[IndexRootId] {
        &self.selected
    }
    pub fn anchors(&self) -> &[CommitId] {
        &self.anchors
    }
    /// All selected frozen drafts, including hidden records, sorted by commit ID.
    /// Canonically identical overlap preserves the minimum original order.
    pub fn frozen_commits(&self) -> &[CloudSnapshotCommit] {
        &self.frozen
    }
    /// Conservative live admission charge, not process/cache RSS. Runtime reader
    /// entries are included even though they reside in Store's shared maps.
    pub fn retained_size_upper_bound(&self) -> Result<usize> {
        let entries = self
            .pins
            .native
            .len()
            .checked_add(self.pins.cloud.len())
            .and_then(|n| n.checked_add(self.pins.artifacts.len()))
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::Limit("cloud scope retained memory"))?;
        [
            Some(std::mem::size_of::<Self>()),
            self.selected
                .capacity()
                .checked_mul(std::mem::size_of::<IndexRootId>()),
            self.anchors
                .capacity()
                .checked_mul(std::mem::size_of::<CommitId>()),
            self.frozen
                .capacity()
                .checked_mul(std::mem::size_of::<CloudSnapshotCommit>()),
            self.frozen.iter().try_fold(0usize, |sum, node| {
                sum.checked_add(
                    node.parents
                        .capacity()
                        .checked_mul(std::mem::size_of::<CommitId>())?,
                )
            }),
            self.pins
                .native
                .capacity()
                .checked_mul(std::mem::size_of::<ObjectKey>()),
            self.pins
                .cloud
                .capacity()
                .checked_mul(std::mem::size_of::<CloudId>()),
            self.pins
                .artifacts
                .capacity()
                .checked_mul(std::mem::size_of::<IndexArtifactId>()),
            Some(self.pins.repo.capacity()),
            self.pins
                .repo
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(1024))
                .and_then(|n| n.checked_mul(entries)),
            Some(4096),
        ]
        .into_iter()
        .try_fold(0usize, |sum, term| {
            sum.checked_add(term.ok_or(Error::Limit("cloud scope retained memory"))?)
                .ok_or(Error::Limit("cloud scope retained memory"))
        })
    }
}
impl Store {
    pub fn open_cloud_snapshot(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: super::CloudVersionSelector,
        selected: &[IndexRootId],
        max_anchors: usize,
    ) -> Result<CloudSnapshotReadGuard> {
        if selector.generation == 0
            || selected.is_empty()
            || selected.len() > self.inner.config.cloud.index_artifacts.max_snapshots
            || max_anchors == 0
            || max_anchors > self.inner.config.max_closure_objects
            || selected.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::Invalid("cloud snapshot selection"));
        }
        let mut selection = SnapshotSelection {
            ids: selected,
            maximum: max_anchors,
            anchors: Vec::new(),
            frozen: FrozenCommits::default(),
            collect_anchors: true,
            collect_frozen: true,
            candidate: None,
            candidate_found: false,
            deadline: None,
        };
        let value = self
            .read_cloud_version_selection(
                repo,
                owner,
                selector.workspace,
                selector.generation,
                self.inner.config.cloud.manifest_limits,
                false,
                Some(&mut selection),
            )?
            .ok_or(Error::Corrupt("cloud snapshot schema dispatch"))?;
        if value.version.manifest != selector.manifest
            || value.manifest.len() as u64 != selector.selected.size_bytes
            || blake3::hash(&value.manifest).as_bytes() != &selector.selected.blake3
        {
            return Err(Error::Invalid("cloud snapshot version selection"));
        }
        if selection.anchors.is_empty() {
            return Err(Error::Corrupt("cloud selected anchors empty"));
        }
        let CloudVersionReadGuard {
            version,
            manifest,
            _guard,
        } = value;
        drop(manifest);
        Ok(CloudSnapshotReadGuard {
            version,
            selector,
            selected: selected.to_vec(),
            anchors: selection.anchors,
            frozen: selection.frozen.nodes,
            pins: _guard,
        })
    }
    pub(super) fn read_cloud_snapshot_commit_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: super::CloudVersionSelector,
        selected: &[IndexRootId],
        commit: CommitId,
        deadline: Option<std::time::Instant>,
    ) -> Result<CloudVersionReadGuard> {
        if selector.generation == 0
            || commit == CommitId::ROOT
            || selected.is_empty()
            || selected.len() > self.inner.config.cloud.index_artifacts.max_snapshots
            || selected.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::Invalid("cloud snapshot commit selection"));
        }
        let mut selection = SnapshotSelection {
            ids: selected,
            maximum: 0,
            anchors: Vec::new(),
            frozen: FrozenCommits::default(),
            collect_anchors: false,
            collect_frozen: false,
            candidate: Some(commit),
            candidate_found: false,
            deadline,
        };
        let value = self
            .read_cloud_version_selection(
                repo,
                owner,
                selector.workspace,
                selector.generation,
                self.inner.config.cloud.manifest_limits,
                false,
                Some(&mut selection),
            )?
            .ok_or(Error::Corrupt("cloud snapshot schema dispatch"))?;
        if value.version.manifest != selector.manifest
            || value.manifest.len() as u64 != selector.selected.size_bytes
            || blake3::hash(&value.manifest).as_bytes() != &selector.selected.blake3
        {
            return Err(Error::Invalid("cloud snapshot version selection"));
        }
        if !selection.candidate_found {
            return Err(Error::Invalid(
                "native commit is outside selected cloud snapshots",
            ));
        }
        Ok(value)
    }
}
