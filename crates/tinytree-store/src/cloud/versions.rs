//! Atomic workspace versions. All published history is retained in this format.
use super::*;
use tinytree_cloud::{Manifest, ManifestId, ManifestLimits, decode_manifest, verify_coverage};

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
struct ValidationGuard {
    native: Vec<ObjectKey>,
    cloud: Vec<CloudId>,
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
        manifest: &Manifest,
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
        Ok(ValidationGuard {
            native,
            cloud,
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
        manifest: &Manifest,
        limits: ManifestLimits,
        files: Option<bool>,
    ) -> Result<Vec<ObjectKey>> {
        self.check_version_work(manifest)?;
        let mut objects = Vec::with_capacity(manifest.members.len());
        let mut retained = objects
            .capacity()
            .checked_mul(std::mem::size_of::<VerifiedObject>())
            .ok_or(Error::Limit("cloud version member allocation"))?;
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        for member in &manifest.members {
            let record = self
                .cloud_object(repo, owner, member.id)?
                .ok_or(Error::Corrupt("retained cloud member missing"))?;
            if record.selected.descriptor()?.selected != member.selected {
                return Err(Error::Corrupt(
                    "retained cloud member representation changed",
                ));
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
        let verified = verify_coverage(manifest, &objects, limits)
            .map_err(|_| Error::Invalid("cloud manifest coverage"))?;
        let wc = verified.working_copy();
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
        if let Some(files) = files {
            self.closure(repo, verified.native_roots().iter().copied(), true, files)?;
        }
        Ok(verified.native_roots().to_vec())
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
        let limits = self.inner.manifest_limits(limits)?;
        let (record, manifest_bytes, manifest, guard) = {
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
            if record.format != 1
                || record.workspace != workspace
                || record.generation != generation
                || generation == 0
            {
                return Err(Error::Corrupt("cloud version identity"));
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
            let manifest = decode_manifest(&bytes, id, limits)
                .map_err(|_| Error::Corrupt("cloud version manifest"))?;
            if manifest.workspace != workspace {
                return Err(Error::Corrupt("cloud version workspace"));
            }
            let roots = self
                .inner
                .db
                .read_tx()
                .get(&self.inner.spaces.cloud.roots, &key)?
                .ok_or(Error::Corrupt("cloud version roots missing"))?;
            if decode_roots(&roots, id, limits.max_native_roots)? != manifest.native_roots {
                return Err(Error::Corrupt("cloud version native roots"));
            }
            let reader = (repo.to_owned(), owner.principal, workspace, generation);
            if gate.cloud_version_readers.get(&reader) == Some(&usize::MAX) {
                return Err(Error::Limit("cloud version readers"));
            }
            let mut guard = self.inner.pin_manifest(repo, owner, &manifest, &mut gate)?;
            *gate.cloud_version_readers.entry(reader).or_default() += 1;
            guard.version = Some((workspace, generation));
            (record, bytes, manifest, guard)
        };
        self.inner
            .validate_manifest(repo, owner, &manifest, limits, Some(false))?;
        Ok(CloudVersionReadGuard {
            version: version(&record),
            manifest: manifest_bytes,
            _guard: guard,
        })
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
        let manifest = decode_manifest(&request.manifest_bytes, request.manifest, policy)
            .map_err(|_| Error::Invalid("cloud manifest encoding"))?;
        if manifest.workspace != request.workspace {
            return Err(Error::Invalid("cloud manifest workspace"));
        }
        let guard = {
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
        let _gate = self.inner.lock()?;
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
        for key in &roots {
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
            let bytes = view
                .get(&self.inner.spaces.cloud.staging, &key)?
                .ok_or(Error::Invalid("cloud staging lease unavailable"))?;
            let stage: StageRecord = decode(&bytes)?;
            let selected = stage.selected.descriptor()?;
            if stage.expires <= now {
                return Err(Error::Invalid("cloud staging lease expired"));
            }
            if !manifest
                .members
                .iter()
                .any(|member| member.id == selected.id && member.selected == selected.selected)
            {
                return Err(Error::Invalid("unrelated cloud staging lease"));
            }
            changes.push((&self.inner.spaces.cloud.staging, key, None));
            consumed += 1;
        }
        self.inner
            .check_cloud_promise_quota(repo, owner, &manifest, &limits.retention)?;
        let version = VersionRecord {
            format: 1,
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
            Some(encode_roots(request.manifest, &roots)?),
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
        advance_feature(&mut tx, &self.inner.spaces.meta, 3)?;
        let payload_work = manifest.members.iter().try_fold(0u64, |sum, member| {
            sum.checked_add(member.selected.size_bytes)
                .ok_or(Error::Limit("cloud version validation bytes"))
        })?;
        let metadata_work = (version_key_for(repo, owner, request.workspace, generation).len() * 2
            + manifest_key_for(repo, owner, request.manifest).len()
            + encode(&version)?.len()
            + encode_roots(request.manifest, &roots)?.len()
            + request.manifest_bytes.len()) as u64;
        let metadata_work = metadata_work
            .checked_add(member_metadata_work)
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
                version_records: 3 + 2 * manifest.members.len() as i128,
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
        let mut plan = RetainedPlan {
            native: HashSet::new(),
            cloud: HashSet::new(),
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
            if record.format != 1
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
            let manifest = decode_manifest(&manifest_bytes, id, limits)
                .map_err(|_| Error::Corrupt("cloud manifest encoding"))?;
            if manifest.workspace != workspace {
                return Err(Error::Corrupt("cloud manifest workspace"));
            }
            self.check_version_work(&manifest)?;
            let root_bytes = view
                .get(&self.spaces.cloud.roots, &key)?
                .ok_or(Error::Corrupt("cloud version roots missing"))?;
            scan.visit(self, &key, &root_bytes)?;
            let roots = decode_roots(&root_bytes, id, limits.max_native_roots)?;
            if roots != manifest.native_roots {
                return Err(Error::Corrupt("cloud native root index changed"));
            }
            for root in roots {
                plan.native.insert(root);
                native_by_repo
                    .entry(actual.to_owned())
                    .or_default()
                    .insert(root);
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
            if validate {
                self.validate_manifest(actual, principal, &manifest, limits, None)?;
            }
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
                    .ok_or(Error::Limit("cloud version validation metadata"))?,
            );
            plan.record_work
                .insert(key.to_vec(), 3 + 2 * manifest.members.len() as u64);
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
        if plan.native.len() > maximum || plan.cloud.len() > maximum {
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
        manifest: &Manifest,
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
        if plan.native.len() > limits.max_quota_scan_roots
            || plan.cloud.len() > limits.max_quota_scan_roots
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
