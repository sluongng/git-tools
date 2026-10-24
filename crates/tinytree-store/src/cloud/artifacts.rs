//! Typed portable-index artifacts. Upload/reader protection is temporary;
//! immutable workspace publication is a separate retention promise.
use super::*;
use tinytree_cloud::{
    IndexArtifact, IndexArtifactDescriptor, IndexArtifactId, IndexArtifactLimits, IndexNodeId,
    IndexNodePayload, IndexRootId, VerifiedIndexArtifact,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexSelected {
    kind: u8,
    id: [u8; 32],
    digest: [u8; 32],
    size: u64,
}
impl IndexSelected {
    fn new(value: IndexArtifactDescriptor) -> Self {
        let (kind, id) = match value.id {
            IndexArtifactId::Root(id) => (1, id.0),
            IndexArtifactId::Node(id) => (2, id.0),
        };
        Self {
            kind,
            id,
            digest: value.selected.blake3,
            size: value.selected.size_bytes,
        }
    }
    pub(super) fn descriptor(&self) -> Result<IndexArtifactDescriptor> {
        let id = match self.kind {
            1 => IndexArtifactId::Root(IndexRootId(self.id)),
            2 => IndexArtifactId::Node(IndexNodeId(self.id)),
            _ => return Err(Error::Corrupt("cloud index artifact kind")),
        };
        Ok(IndexArtifactDescriptor {
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
pub(super) struct IndexRecord {
    version: u8,
    pub(super) selected: IndexSelected,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexStage {
    pub(super) selected: IndexSelected,
    pub(super) expires: i64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudIndexWriteReceipt {
    pub descriptor: IndexArtifactDescriptor,
    pub staging_lease: String,
    pub expires_at: i64,
}
pub(super) fn artifact_key(repo: &str, owner: CloudOwner, id: IndexArtifactId) -> Vec<u8> {
    let mut key = scope(repo, owner);
    match id {
        IndexArtifactId::Root(id) => {
            key.push(1);
            key.extend_from_slice(&id.0);
        }
        IndexArtifactId::Node(id) => {
            key.push(2);
            key.extend_from_slice(&id.0);
        }
    }
    key
}
fn artifact_stage_id(value: IndexArtifactDescriptor) -> [u8; 32] {
    let selected = IndexSelected::new(value);
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-index-stage-v1\0");
    h.update(&[selected.kind]);
    h.update(&selected.id);
    h.update(&selected.digest);
    h.update(&selected.size.to_be_bytes());
    *h.finalize().as_bytes()
}
fn artifact_stage_key(repo: &str, owner: CloudOwner, token: [u8; 32]) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.extend_from_slice(hex(&token).as_bytes());
    key
}
// Outgoing canonical references are read from the bounded artifact itself,
// never expanded into a potentially unreadable JSON dependency summary.
pub(super) fn artifact_children(value: &IndexArtifact) -> Vec<IndexArtifactId> {
    match value {
        IndexArtifact::Root(root) => std::iter::once(root.anchors)
            .chain(root.drafts)
            .map(IndexArtifactId::Node)
            .collect(),
        IndexArtifact::Node(node) => match &node.payload {
            IndexNodePayload::Branch(values) => values
                .iter()
                .map(|child| IndexArtifactId::Node(child.node))
                .collect(),
            _ => Vec::new(),
        },
    }
}
pub(super) fn outgoing(value: &IndexArtifact) -> (Vec<IndexArtifactId>, Vec<ObjectKey>) {
    let mut children = Vec::new();
    let mut native = Vec::new();
    let mut commit = |id: tinytree_model::CommitId| {
        if id != tinytree_model::CommitId::ROOT {
            native.push(ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(id.0),
            });
        }
    };
    match value {
        IndexArtifact::Root(root) => {
            children.push(IndexArtifactId::Node(root.anchors));
            if let Some(drafts) = root.drafts {
                children.push(IndexArtifactId::Node(drafts));
            }
        }
        IndexArtifact::Node(node) => match &node.payload {
            IndexNodePayload::Branch(values) => {
                children.extend(values.iter().map(|value| IndexArtifactId::Node(value.node)))
            }
            IndexNodePayload::Anchors(values) => {
                for id in values {
                    commit(*id);
                }
            }
            IndexNodePayload::Drafts(values) => {
                for draft in values {
                    commit(draft.commit());
                    for id in &draft.parents {
                        commit(*id);
                    }
                }
            }
        },
    }
    (children, native)
}
pub struct CloudIndexReadLease {
    file: File,
    descriptor: IndexArtifactDescriptor,
    repo: String,
    owner: CloudOwner,
    inner: Arc<Inner>,
}
impl CloudIndexReadLease {
    pub fn descriptor(&self) -> IndexArtifactDescriptor {
        self.descriptor
    }
}
impl Read for CloudIndexReadLease {
    fn read(&mut self, value: &mut [u8]) -> io::Result<usize> {
        self.file.read(value)
    }
}
impl Drop for CloudIndexReadLease {
    fn drop(&mut self) {
        if let Ok(mut gate) = self.inner.gate.lock() {
            let key = (self.repo.clone(), self.owner.principal, self.descriptor.id);
            if let Some(n) = gate.index_artifact_readers.get_mut(&key) {
                *n -= 1;
                if *n == 0 {
                    gate.index_artifact_readers.remove(&key);
                }
            }
        }
    }
}
impl Inner {
    pub(super) fn index_limits(
        &self,
        mut limits: IndexArtifactLimits,
    ) -> Result<IndexArtifactLimits> {
        limits
            .validate()
            .map_err(|_| Error::Invalid("cloud index artifact limits"))?;
        let policy = self.config.cloud.index_artifacts;
        limits.max_artifact_bytes = limits.max_artifact_bytes.min(policy.max_artifact_bytes);
        limits.max_artifacts = limits.max_artifacts.min(policy.max_artifacts);
        limits.max_snapshots = limits.max_snapshots.min(policy.max_snapshots);
        limits.max_total_artifact_bytes = limits
            .max_total_artifact_bytes
            .min(policy.max_total_artifact_bytes);
        limits.max_decode_bytes = limits.max_decode_bytes.min(policy.max_decode_bytes);
        limits.max_graph_steps = limits.max_graph_steps.min(policy.max_graph_steps);
        let actual = policy.snapshot;
        limits.snapshot.max_encoded_bytes = limits
            .snapshot
            .max_encoded_bytes
            .min(actual.max_encoded_bytes);
        limits.snapshot.max_decode_bytes = limits
            .snapshot
            .max_decode_bytes
            .min(actual.max_decode_bytes);
        limits.snapshot.max_roots = limits.snapshot.max_roots.min(actual.max_roots);
        limits.snapshot.max_drafts = limits.snapshot.max_drafts.min(actual.max_drafts);
        limits.snapshot.max_parents = limits.snapshot.max_parents.min(actual.max_parents);
        limits.snapshot.max_edges = limits.snapshot.max_edges.min(actual.max_edges);
        limits.snapshot.max_metadata_bytes = limits
            .snapshot
            .max_metadata_bytes
            .min(actual.max_metadata_bytes);
        Ok(limits)
    }
    pub(super) fn index_record(
        &self,
        repo: &str,
        owner: CloudOwner,
        id: IndexArtifactId,
    ) -> Result<Option<IndexRecord>> {
        self.db
            .read_tx()
            .get(
                &self.spaces.cloud.index_artifacts,
                artifact_key(repo, owner, id),
            )?
            .map(|bytes| {
                let value: IndexRecord = decode(&bytes)?;
                if value.version != 1 || value.selected.descriptor()?.id != id {
                    return Err(Error::Corrupt("cloud index catalog identity"));
                }
                Ok(value)
            })
            .transpose()
    }
    pub(super) fn verify_index_backing(
        &self,
        repo: &str,
        owner: CloudOwner,
        record: &IndexRecord,
        scan: &mut Scan,
    ) -> Result<VerifiedIndexArtifact> {
        self.verify_index_backing_policy(
            repo,
            owner,
            record,
            scan,
            self.config.cloud.index_artifacts,
            false,
        )
    }
    pub(super) fn verify_index_backing_limited(
        &self,
        repo: &str,
        owner: CloudOwner,
        record: &IndexRecord,
        scan: &mut Scan,
        limits: IndexArtifactLimits,
    ) -> Result<VerifiedIndexArtifact> {
        self.verify_index_backing_policy(repo, owner, record, scan, limits, true)
    }
    fn verify_index_backing_policy(
        &self,
        repo: &str,
        owner: CloudOwner,
        record: &IndexRecord,
        scan: &mut Scan,
        limits: IndexArtifactLimits,
        budgeted: bool,
    ) -> Result<VerifiedIndexArtifact> {
        let d = record.selected.descriptor()?;
        scan.payload(self, d.selected.size_bytes)?;
        let limits = self.index_limits(limits)?;
        if d.selected.size_bytes > limits.max_artifact_bytes as u64 {
            return Err(Error::Corrupt("cloud index stored payload size"));
        }
        self.cloud_namespace(repo, owner, false)?;
        let path = self.cloud_path(repo, owner, d.selected.blake3);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| Error::Corrupt("cloud index backing missing"))?;
        if !metadata.is_file() || metadata.len() != d.selected.size_bytes {
            return Err(Error::Corrupt("cloud index backing type or size"));
        }
        let bytes = fs::read(path)?;
        tinytree_cloud::decode_index_artifact(&bytes, d, limits).map_err(|error| match error {
            tinytree_cloud::Error::Limit(_) if budgeted => {
                Error::Limit("cloud index decoded allocation")
            }
            _ => Error::Corrupt("cloud index backing verification"),
        })
    }
}
impl Store {
    pub fn lookup_cloud_index_artifacts(
        &self,
        repo: &str,
        owner: CloudOwner,
        ids: &[IndexArtifactId],
    ) -> Result<Vec<Option<IndexArtifactDescriptor>>> {
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        if ids.len() > 1024 {
            return Err(Error::Limit("cloud index lookup batch"));
        }
        ids.iter()
            .map(|id| {
                self.inner
                    .index_record(repo, owner, *id)?
                    .map(|record| record.selected.descriptor())
                    .transpose()
            })
            .collect()
    }
    pub fn read_cloud_index_artifact(
        &self,
        repo: &str,
        owner: CloudOwner,
        id: IndexArtifactId,
        limits: IndexArtifactLimits,
    ) -> Result<CloudIndexReadLease> {
        let limits = self.inner.index_limits(limits)?;
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let record = self
            .inner
            .index_record(repo, owner, id)?
            .ok_or(Error::Invalid("cloud index artifact unavailable"))?;
        let d = record.selected.descriptor()?;
        if d.selected.size_bytes > limits.max_artifact_bytes as u64 {
            return Err(Error::Limit("cloud index artifact bytes"));
        }
        let path = self.inner.cloud_path(repo, owner, d.selected.blake3);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| Error::Corrupt("cloud index backing missing"))?;
        if !metadata.is_file() || metadata.len() != d.selected.size_bytes {
            return Err(Error::Corrupt("cloud index backing type or size"));
        }
        let file = File::open(path)?;
        let count = gate
            .index_artifact_readers
            .entry((repo.into(), owner.principal, id))
            .or_default();
        *count = count
            .checked_add(1)
            .ok_or(Error::Limit("cloud index active readers"))?;
        drop(gate);
        let mut lease = CloudIndexReadLease {
            file,
            descriptor: d,
            repo: repo.into(),
            owner,
            inner: self.inner.clone(),
        };
        let mut bytes = Vec::new();
        (&mut lease.file)
            .take(d.selected.size_bytes + 1)
            .read_to_end(&mut bytes)?;
        tinytree_cloud::decode_index_artifact(&bytes, d, limits)
            .map_err(|_| Error::Corrupt("cloud index backing verification"))?;
        lease.file.seek(SeekFrom::Start(0))?;
        Ok(lease)
    }
}

impl Store {
    pub fn put_cloud_index_artifact(
        &self,
        repo: &str,
        owner: CloudOwner,
        proposed: IndexArtifactDescriptor,
        bytes: &[u8],
        now: i64,
        limits: IndexArtifactLimits,
    ) -> Result<CloudIndexWriteReceipt> {
        let limits = self.inner.index_limits(limits)?;
        let id = proposed.id;
        let digest = proposed.selected;
        if bytes.len() > limits.max_artifact_bytes || digest.size_bytes != bytes.len() as u64 {
            return Err(Error::Limit("cloud payload bytes"));
        }
        let expires = now
            .checked_add(self.inner.config.staging_ttl_seconds)
            .ok_or(Error::Invalid("cloud staging expiry"))?;
        let reservation = self.inner.reserve_cloud_catalog(
            repo,
            owner,
            &self.inner.spaces.cloud.index_artifacts,
            artifact_key(repo, owner, id),
            digest,
        )?;
        tinytree_cloud::decode_index_artifact(bytes, proposed, limits)
            .map_err(|_| Error::Invalid("cloud index artifact verification"))?;
        let mut temporary = NamedTempFile::new_in(self.inner.root.join("staging"))?;
        temporary.write_all(bytes)?;
        temporary.as_file().sync_all()?;
        {
            let _gate = self.inner.lock()?;
            self.inner.cloud_namespace(repo, owner, true)?;
        }
        let mut candidate: Option<CloudIndexReadLease> = None;
        let mut raw_candidate: Option<File> = None;
        loop {
            let _gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(record) = self.inner.index_record(repo, owner, id)? {
                let selected = record.selected.descriptor()?;
                if selected != proposed {
                    return Err(Error::Corrupt(
                        "cloud index selected representation changed",
                    ));
                }
                if candidate
                    .as_ref()
                    .is_none_or(|candidate| candidate.descriptor != selected)
                {
                    drop(_gate);
                    candidate = Some(self.read_cloud_index_artifact(
                        repo,
                        owner,
                        id,
                        self.inner.config.cloud.index_artifacts,
                    )?);
                    continue;
                }
                let lease = artifact_stage_id(selected);
                let mut stage = scope(repo, owner);
                stage.extend_from_slice(hex(&lease).as_bytes());
                let previous = self
                    .inner
                    .db
                    .read_tx()
                    .get(&self.inner.spaces.cloud.index_staging, &stage)?;
                let old = previous
                    .as_ref()
                    .map(|bytes| decode::<IndexStage>(bytes))
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
                        &self.inner.spaces.cloud.index_staging,
                        stage,
                        Some(encode(&IndexStage {
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
                return Ok(CloudIndexWriteReceipt {
                    descriptor: selected,
                    staging_lease: hex(&lease),
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
            let record = IndexRecord {
                version: 1,
                selected: IndexSelected::new(proposed),
            };
            let lease = artifact_stage_id(proposed);
            let mut stage = scope(repo, owner);
            stage.extend_from_slice(hex(&lease).as_bytes());
            let mut tx = self
                .inner
                .db
                .write_tx()
                .durability(Some(PersistMode::SyncAll));
            advance_feature(&mut tx, &self.inner.spaces.meta, 4)?;
            self.inner.cloud_changes(
                &mut tx,
                repo,
                owner,
                vec![
                    (
                        &self.inner.spaces.cloud.index_artifacts,
                        artifact_key(repo, owner, id),
                        Some(encode(&record)?),
                    ),
                    (&self.inner.spaces.cloud.blobs, key, Some(encode(&blob)?)),
                    (
                        &self.inner.spaces.cloud.index_staging,
                        stage,
                        Some(encode(&IndexStage {
                            selected: IndexSelected::new(proposed),
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
            return Ok(CloudIndexWriteReceipt {
                descriptor: proposed,
                staging_lease: hex(&lease),
                expires_at: expires,
            });
        }
    }
}

impl Inner {
    pub(super) fn validate_index_catalog(
        &self,
        feature: u8,
        expected: &mut HashMap<Vec<u8>, Accounting>,
        blobs: &mut HashMap<Vec<u8>, (u64, u64)>,
        scan: &mut Scan,
    ) -> Result<()> {
        let view = self.db.read_tx();
        if feature < 4 {
            if view
                .iter(&self.spaces.cloud.index_artifacts)
                .next()
                .is_some()
                || view.iter(&self.spaces.cloud.index_staging).next().is_some()
            {
                return Err(Error::Corrupt("cloud index feature marker missing"));
            }
            return Ok(());
        }
        for entry in view.iter(&self.spaces.cloud.index_artifacts) {
            let (key, bytes) = entry.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (repo, owner, _) = parse_scope(&key)?;
            self.repository(repo)?;
            let record: IndexRecord = decode(&bytes)?;
            let d = record.selected.descriptor()?;
            if record.version != 1 || key.as_ref() != artifact_key(repo, owner, d.id).as_slice() {
                return Err(Error::Corrupt("cloud index catalog identity"));
            }
            self.verify_index_backing(repo, owner, &record, scan)?;
            let account = expected.entry(scope(repo, owner)).or_default();
            metadata_account(account, &key, &bytes)?;
            adjust(&mut account.objects, 1)?;
            adjust(&mut account.selected_bytes, d.selected.size_bytes as i128)?;
            let blob = blobs
                .entry(blob_key(repo, owner, d.selected.blake3))
                .or_insert((d.selected.size_bytes, 0));
            if blob.0 != d.selected.size_bytes {
                return Err(Error::Corrupt("cloud index raw blob length"));
            }
            blob.1 = blob
                .1
                .checked_add(1)
                .ok_or(Error::Corrupt("cloud index reference count"))?;
        }
        for entry in view.iter(&self.spaces.cloud.index_staging) {
            let (key, bytes) = entry.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (repo, owner, _) = parse_scope(&key)?;
            self.repository(repo)?;
            let stage: IndexStage = decode(&bytes)?;
            let d = stage.selected.descriptor()?;
            if key.as_ref() != artifact_stage_key(repo, owner, artifact_stage_id(d)).as_slice()
                || self
                    .index_record(repo, owner, d.id)?
                    .is_none_or(|record| record.selected.descriptor().ok() != Some(d))
            {
                return Err(Error::Corrupt("cloud index staging identity"));
            }
            let account = expected.entry(scope(repo, owner)).or_default();
            metadata_account(account, &key, &bytes)?;
            adjust(&mut account.stages, 1)?;
        }
        Ok(())
    }
    pub(super) fn index_gc_into(
        &self,
        repo: &str,
        now: i64,
        gate: &Runtime,
        published: &HashSet<(CloudOwner, IndexArtifactId)>,
        scan: &mut Scan,
        plan: &mut CloudGcPlan,
        victim_refs: &mut HashMap<Vec<u8>, (CloudOwner, u64)>,
    ) -> Result<()> {
        let view = self.db.read_tx();
        let prefix = repo_prefix(repo);
        let mut objects = HashMap::<(CloudOwner, IndexArtifactId), (Vec<u8>, IndexRecord)>::new();
        for entry in view.prefix(&self.spaces.cloud.index_artifacts, &prefix) {
            let (key, bytes) = entry.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (actual, owner, _) = parse_scope(&key)?;
            let record: IndexRecord = decode(&bytes)?;
            let d = record.selected.descriptor()?;
            if actual != repo
                || record.version != 1
                || key.as_ref() != artifact_key(repo, owner, d.id).as_slice()
            {
                return Err(Error::Corrupt("cloud index catalog identity"));
            }
            objects.insert((owner, d.id), (key.to_vec(), record));
        }
        let mut marked = published.clone();
        if marked.iter().any(|key| !objects.contains_key(key)) {
            return Err(Error::Corrupt("cloud published index artifact missing"));
        }
        let mut pending = Vec::new();
        for entry in view.prefix(&self.spaces.cloud.index_staging, &prefix) {
            let (key, bytes) = entry.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (actual, owner, _) = parse_scope(&key)?;
            let stage: IndexStage = decode(&bytes)?;
            let d = stage.selected.descriptor()?;
            if actual != repo
                || key.as_ref() != artifact_stage_key(repo, owner, artifact_stage_id(d)).as_slice()
                || objects
                    .get(&(owner, d.id))
                    .is_none_or(|(_, record)| record.selected.descriptor().ok() != Some(d))
            {
                return Err(Error::Corrupt("cloud index staging identity"));
            }
            if stage.expires > now {
                if marked.insert((owner, d.id)) {
                    pending.push((owner, d.id));
                }
            } else {
                let group = plan.groups.entry(owner).or_default();
                group.changes.push(GcChange {
                    space: 10,
                    key: key.to_vec(),
                    value: None,
                });
                group.delta.stages -= 1;
                plan.expired += 1;
            }
        }
        for ((actual, principal, id), _) in &gate.index_artifact_readers {
            if actual == repo {
                let key = (
                    CloudOwner {
                        principal: *principal,
                    },
                    *id,
                );
                if !objects.contains_key(&key) {
                    return Err(Error::Corrupt("cloud index active reader missing"));
                }
                if marked.insert(key) {
                    pending.push(key);
                }
            }
        }
        let mut steps = 0usize;
        while let Some((owner, id)) = pending.pop() {
            let Some((_, record)) = objects.get(&(owner, id)) else {
                continue;
            };
            let artifact = self.verify_index_backing(repo, owner, record, scan)?;
            let (children, native) = outgoing(artifact.artifact());
            steps = steps
                .checked_add(1)
                .and_then(|n| n.checked_add(children.len()))
                .and_then(|n| n.checked_add(native.len()))
                .ok_or(Error::Limit("cloud index staging graph"))?;
            if steps > self.config.cloud.index_artifacts.max_graph_steps {
                return Err(Error::Limit("cloud index staging graph"));
            }
            for key in native {
                plan.transient.insert(key);
                if plan.transient.len() > self.config.max_closure_objects {
                    return Err(Error::Limit("cloud index native staging roots"));
                }
            }
            for child in children {
                if objects.contains_key(&(owner, child)) && marked.insert((owner, child)) {
                    pending.push((owner, child));
                }
            }
            if marked.len() > self.config.max_retention_validation_records {
                return Err(Error::Limit("cloud index staging graph"));
            }
        }
        for ((owner, id), (key, record)) in objects {
            if marked.contains(&(owner, id)) {
                continue;
            }
            let d = record.selected.descriptor()?;
            let group = plan.groups.entry(owner).or_default();
            group.changes.push(GcChange {
                space: 9,
                key,
                value: None,
            });
            group.delta.objects -= 1;
            group.delta.selected_bytes -= d.selected.size_bytes as i128;
            plan.objects += 1;
            let entry = victim_refs
                .entry(blob_key(repo, owner, d.selected.blake3))
                .or_insert((owner, 0));
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or(Error::Corrupt("cloud index victim references"))?;
        }
        Ok(())
    }
}
