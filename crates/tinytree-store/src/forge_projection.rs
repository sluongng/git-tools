//! Sanitized native workspace artifacts and actor-owned file upload receipts.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model::{ChangeId, CopyId, Object, TreeId, TreeValue};
const PREFIX: &[u8] = b"forge/projections/v1/";
const RETIRED: &[u8] = b"forge/projection-retired/v1/";
const UPLOAD: &[u8] = b"forge/uploads/v1/";
const MAX_PROJECTIONS: u64 = 4096;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub use tinytree_forge_core::projection::{ForgeCopyOrigin, ForgeProjection};
use tinytree_forge_core::projection::{
    ProjectionContext, ProjectionError, ReviewProjection, SelectedEntry, SelectedValue,
    build_projection,
};

impl From<ProjectionError> for Error {
    fn from(error: ProjectionError) -> Self {
        match error {
            ProjectionError::Invalid(reason) => Self::Invalid(reason),
            ProjectionError::Limit(reason) => Self::Limit(reason),
            ProjectionError::Model(error) => Self::Model(error),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionRecord {
    record_key: String,
    binding: ForgeProjection,
    objects: Vec<(u8, [u8; 32])>,
    copies: Vec<ForgeCopyOrigin>,
}
const MAX_CATALOG_BYTES: usize = 8 * 1024 * 1024;
fn validate_catalog(record: &ProjectionRecord) -> Result<()> {
    record
        .binding
        .account_id
        .validate()
        .map_err(|_| Error::Corrupt("forge projection account"))?;
    if record.record_key.len() > 512
        || record.binding.policy_epoch.0 == 0
        || record.binding.canonical_revision == CommitId::ROOT.0
        || record.binding.projected_commit == CommitId::ROOT.0
        || record.objects.len() > 30_002
        || record.copies.len() > 10_000
        || !record.objects.windows(2).all(|p| p[0] < p[1])
    {
        return Err(Error::Corrupt("forge projection catalog shape"));
    }
    for (kind, _) in &record.objects {
        Kind::try_from(*kind).map_err(|_| Error::Corrupt("forge projection object kind"))?;
    }
    if !record
        .objects
        .binary_search(&(Kind::Commit as u8, record.binding.projected_commit))
        .is_ok()
        || !record
            .objects
            .binary_search(&(Kind::Tree as u8, record.binding.projected_tree))
            .is_ok()
    {
        return Err(Error::Corrupt("forge projection roots catalog"));
    }
    let mut paths = 0usize;
    let mut identities = BTreeSet::new();
    for origin in &record.copies {
        super::forge_accounts::valid_path(&origin.canonical_path, false)
            .map_err(|_| Error::Corrupt("forge projection path"))?;
        paths = paths
            .checked_add(origin.canonical_path.len())
            .ok_or(Error::Corrupt("forge projection path budget"))?;
        if paths > 4 * 1024 * 1024
            || origin.canonical_revision != record.binding.canonical_revision
            || !record
                .objects
                .binary_search(&(Kind::CopyHistory as u8, origin.projected_copy))
                .is_ok()
            || !record
                .objects
                .binary_search(&(Kind::File as u8, origin.file))
                .is_ok()
            || !identities.insert(origin.projected_copy)
        {
            return Err(Error::Corrupt("forge projection copy catalog"));
        }
    }
    Ok(())
}
fn decode_catalog(bytes: &[u8]) -> Result<ProjectionRecord> {
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(Error::Corrupt("forge projection catalog size"));
    }
    let record: ProjectionRecord = serde_json::from_slice(bytes)
        .map_err(|_| Error::Corrupt("forge projection catalog encoding"))?;
    validate_catalog(&record)?;
    Ok(record)
}
fn encode_catalog(record: &ProjectionRecord) -> Result<Vec<u8>> {
    validate_catalog(record)?;
    let bytes = encode(record)?;
    if bytes.len() > MAX_CATALOG_BYTES {
        return Err(Error::Limit("forge projection catalog bytes"));
    }
    Ok(bytes)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    account: AccountId,
    view: ViewVersion,
    epoch: PolicyEpoch,
    principal: String,
    path: String,
    descriptor: StoredDescriptor,
    expires_at: i64,
}
fn projection_context(
    view: &ResolvedView,
    canonical: CommitId,
    review: Option<(ChangeId, CommitId)>,
) -> ProjectionContext {
    ProjectionContext {
        account_id: view.account().clone(),
        view_version: view.version(),
        policy_epoch: view.epoch(),
        canonical_revision: canonical,
        review: review.map(|(change_id, projected_parent)| ReviewProjection {
            change_id,
            projected_parent,
        }),
    }
}
fn projection_key(view: &ResolvedView) -> Vec<u8> {
    tinytree_forge_core::projection::projection_key(&projection_context(
        view,
        view.revision(),
        None,
    ))
}
fn reference(binding: &ForgeProjection) -> String {
    format!(
        "_tinytree/forge/projections/v1/{}/{}/{}",
        hex(&binding.view_version.0),
        binding.policy_epoch.0,
        hex(&binding.projected_commit)
    )
}
fn retired_projection_key(record: &ProjectionRecord) -> Vec<u8> {
    format!(
        "forge/projection-retired/v1/{}/{}/{}",
        record.binding.account_id.0,
        hex(&record.binding.view_version.0),
        blake3::hash(record.record_key.as_bytes()).to_hex()
    )
    .into_bytes()
}
fn validate_record_key(record: &ProjectionRecord, physical: &[u8]) -> Result<()> {
    let expected = format!(
        "forge/projections/v1/{}/{}/{}",
        record.binding.account_id.0,
        hex(&record.binding.view_version.0),
        record.binding.policy_epoch.0
    );
    if physical != record.record_key.as_bytes()
        || !(record.record_key == expected
            || record
                .record_key
                .starts_with(&format!("{expected}/review/")))
    {
        return Err(Error::Corrupt("forge projection binding"));
    }
    Ok(())
}
fn upload_key(view: &ResolvedView, actor: &ForgeActor, path: &str, id: [u8; 32]) -> Vec<u8> {
    let mut h = blake3::Hasher::new();
    for bytes in [actor.principal.as_bytes(), path.as_bytes(), id.as_slice()] {
        h.update(&(bytes.len() as u64).to_le_bytes());
        h.update(bytes);
    }
    format!(
        "forge/uploads/v1/{}/{}/{}/{}",
        view.account().0,
        hex(&view.version().0),
        view.epoch().0,
        h.finalize().to_hex()
    )
    .into_bytes()
}
impl Store {
    /// Materialize only the authorized projection, retaining canonical path names.
    /// Canonical parents, descriptions, signatures and copy ancestry are absent.
    pub fn forge_project_view(&self, view: &ResolvedView, now: i64) -> Result<ForgeProjection> {
        self.forge_project_impl(view, view.revision(), None, now)
    }
    /// Trusted review integration only: C proves the candidate/landed binding.
    /// Canonical ancestry stays hidden; only the projected parent is retained.
    pub fn forge_project_review_head(
        &self,
        view: &ResolvedView,
        canonical: CommitId,
        change: ChangeId,
        parent: CommitId,
        now: i64,
    ) -> Result<ForgeProjection> {
        if parent != CommitId::ROOT {
            self.forge_authorized_object_descriptor(
                view,
                None,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(parent.0),
                },
                now,
            )?;
        }
        self.forge_project_impl(view, canonical, Some((change, parent)), now)
    }
    fn forge_project_impl(
        &self,
        view: &ResolvedView,
        canonical: CommitId,
        review: Option<(ChangeId, CommitId)>,
        now: i64,
    ) -> Result<ForgeProjection> {
        self.forge_check_view(view)?;
        let context = projection_context(view, canonical, review);
        let pk = tinytree_forge_core::projection::projection_key(&context);
        {
            let _gate = self.inner.lock_read()?;
            let tx = self.inner.db.read_tx();
            self.inner.forge_check_view_in(&tx, view)?;
            if let Some(bytes) = tx.get(&self.inner.spaces.meta, &pk)? {
                return Ok(decode_catalog(&bytes)?.binding);
            }
        }
        let files = self.forge_view_files(&view.at_revision(canonical), 10_000)?;
        let mut selected = Vec::with_capacity(files.len());
        for file in files {
            let value = match file.descriptor.key.kind {
                Kind::File => SelectedValue::File {
                    executable: file.executable,
                    canonical_copy: file.copy_id,
                },
                Kind::Symlink => {
                    let (_, bytes) = self.read_metadata(view.repository(), file.descriptor.key)?;
                    let target =
                        match tinytree_model::decode(&bytes, self.inner.config.model_limits)? {
                            Object::Symlink(target) => target,
                            _ => return Err(Error::Corrupt("forge symlink type")),
                        };
                    SelectedValue::Symlink {
                        target_bytes: target.len() as u64,
                    }
                }
                _ => return Err(Error::Invalid("forge projected entry kind")),
            };
            selected.push(SelectedEntry {
                canonical_path: file.canonical_path,
                descriptor: file.descriptor,
                value,
            });
        }
        let built = build_projection(&context, selected, self.inner.config.model_limits)?;
        if built.record_key != pk {
            return Err(Error::Corrupt("forge projection key"));
        }
        let descriptor = built
            .objects
            .last()
            .ok_or(Error::Corrupt("forge projection commit"))?
            .descriptor;
        if descriptor.key.kind != Kind::Commit
            || descriptor.key.id.0 != built.binding.projected_commit
        {
            return Err(Error::Corrupt("forge projection commit"));
        }
        for object in built.objects {
            self.put(view.repository(), object.descriptor, &object.bytes, now)?;
        }
        let binding = built.binding;
        let copies = built.copies;
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        self.inner.forge_check_view_in(&tx, view)?;
        if let Some(bytes) = tx.get(&self.inner.spaces.meta, &pk)? {
            return Ok(decode_catalog(&bytes)?.binding);
        }
        let count_key = format!("forge/projection-count/v1/{}", view.account().0);
        let count = tx
            .get(&self.inner.spaces.meta, &count_key)?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if count >= MAX_PROJECTIONS {
            return Err(Error::Limit("forge projections"));
        }
        let closure = self
            .inner
            .closure(view.repository(), [descriptor.key], true, false)?;
        let mut objects: Vec<_> = closure
            .into_iter()
            .map(|k| (k.kind as u8, k.id.0))
            .collect();
        objects.sort();
        if objects.len() > 30_002 {
            return Err(Error::Limit("forge projected closure"));
        }
        tx.insert(
            &self.inner.spaces.refs,
            key(view.repository(), reference(&binding).as_bytes()),
            encode(&StoredReference {
                generation: 1,
                commit: Some(descriptor.into()),
            })?,
        );
        tx.insert(
            &self.inner.spaces.meta,
            &pk,
            encode_catalog(&ProjectionRecord {
                record_key: String::from_utf8(pk.clone())
                    .map_err(|_| Error::Corrupt("forge projection key"))?,
                binding: binding.clone(),
                objects,
                copies,
            })?,
        );
        tx.insert(&self.inner.spaces.meta, count_key, encode(&(count + 1))?);
        self.inner.commit(tx)?;
        Ok(binding)
    }
    /// Serves only keys in the sanitized snapshot, never canonical ancestry.
    pub fn forge_read_projected_object(
        &self,
        view: &ResolvedView,
        object: ObjectKey,
        now: i64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        self.forge_project_view(view, now)?;
        {
            let _gate = self.inner.lock_read()?;
            let tx = self.inner.db.read_tx();
            self.inner.forge_check_view_in(&tx, view)?;
            let record: ProjectionRecord = decode_catalog(
                &tx.get(&self.inner.spaces.meta, projection_key(view))?
                    .ok_or(Error::ForgeAccessDenied)?,
            )?;
            if record
                .objects
                .binary_search(&(object.kind as u8, object.id.0))
                .is_err()
            {
                return Err(Error::ForgeAccessDenied);
            }
        }
        let mut lease = self.read(view.repository(), object)?;
        if lease.descriptor().size_bytes > max_bytes as u64 {
            return Err(Error::Limit("forge projected object"));
        }
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes)?;
        self.forge_check_view(view)?;
        Ok(bytes)
    }
    /// Stage caller-supplied native metadata only after every referenced leaf and
    /// parent is proven visible or owned by this actor at the current epoch.
    pub fn forge_upload_object(
        &self,
        view: &ResolvedView,
        actor: &ForgeActor,
        canonical_path: &str,
        descriptor: Descriptor,
        bytes: &[u8],
        now: i64,
    ) -> Result<WriteReceipt> {
        if descriptor.key.kind == Kind::File {
            if tinytree_model::describe_file(bytes) != descriptor {
                return Err(Error::Invalid("forge upload descriptor"));
            }
            return self.forge_upload_file(view, actor, canonical_path, bytes, now);
        }
        if bytes.len() > 1024 * 1024 {
            return Err(Error::Limit("forge upload metadata"));
        }
        tinytree_model::verify(&descriptor, bytes, self.inner.config.model_limits)?;
        self.forge_project_view(view, now)?;
        let receipt = self.put(view.repository(), descriptor, bytes, now)?;
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        self.inner.forge_check_view_in(&tx, view)?;
        self.inner.forge_authorize_in(
            &tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        self.inner
            .forge_validate_metadata_in(&tx, view, actor, canonical_path, descriptor, now)?;
        if descriptor.key.kind == Kind::Tree && !canonical_path.is_empty() {
            super::forge_accounts::valid_path(canonical_path, false)?;
            self.inner.closure_at_tree_path(
                view.repository(),
                TreeId(descriptor.key.id.0),
                canonical_path,
            )?;
        } else {
            self.inner
                .closure(view.repository(), [descriptor.key], true, false)?;
        }
        let uk = upload_key(view, actor, canonical_path, descriptor.key.id.0);
        let ck = format!("forge/upload-count/v1/{}", view.account().0);
        let count = tx
            .get(&self.inner.spaces.meta, &ck)?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if tx.get(&self.inner.spaces.meta, &uk)?.is_none() {
            if count >= 4096 {
                return Err(Error::Limit("forge upload receipts"));
            }
            tx.insert(&self.inner.spaces.meta, ck, encode(&(count + 1))?);
        }
        tx.insert(
            &self.inner.spaces.meta,
            uk,
            encode(&UploadRecord {
                account: view.account().clone(),
                view: view.version(),
                epoch: view.epoch(),
                principal: actor.principal.clone(),
                path: canonical_path.into(),
                descriptor: descriptor.into(),
                expires_at: receipt.expires_at,
            })?,
        );
        self.inner.commit(tx)?;
        Ok(receipt)
    }
    pub fn forge_authorized_object_descriptor(
        &self,
        view: &ResolvedView,
        actor: Option<&ForgeActor>,
        object: ObjectKey,
        now: i64,
    ) -> Result<Descriptor> {
        self.forge_project_view(view, now)?;
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        self.inner.forge_check_view_in(&tx, view)?;
        let projection: ProjectionRecord = decode_catalog(
            &tx.get(&self.inner.spaces.meta, projection_key(view))?
                .ok_or(Error::ForgeAccessDenied)?,
        )?;
        let mut allowed = projection
            .objects
            .binary_search(&(object.kind as u8, object.id.0))
            .is_ok();
        if !allowed {
            let prefix = format!(
                "{}/review/",
                String::from_utf8(projection_key(view))
                    .map_err(|_| Error::Corrupt("forge projection key"))?
            );
            let mut count = 0;
            for entry in tx.prefix(&self.inner.spaces.meta, prefix) {
                let (_, bytes) = entry.into_inner()?;
                count += 1;
                if count > 4096 {
                    return Err(Error::Corrupt("forge projection count"));
                }
                let r: ProjectionRecord = decode_catalog(&bytes)?;
                if r.binding.account_id == *view.account()
                    && r.binding.view_version == view.version()
                    && r.binding.policy_epoch == view.epoch()
                    && r.objects
                        .binary_search(&(object.kind as u8, object.id.0))
                        .is_ok()
                {
                    allowed = true;
                    break;
                }
            }
        }
        if !allowed {
            if let Some(actor) = actor {
                let current = self.inner.forge_resolve_view_in(
                    &tx,
                    view.account(),
                    Some(actor),
                    view.version(),
                )?;
                if current.epoch() != view.epoch() {
                    return Err(Error::StaleForgePolicy);
                }
                let prefix = format!(
                    "forge/uploads/v1/{}/{}/{}/",
                    view.account().0,
                    hex(&view.version().0),
                    view.epoch().0
                );
                let mut count = 0;
                for entry in tx.prefix(&self.inner.spaces.meta, prefix) {
                    let (_, bytes) = entry.into_inner()?;
                    count += 1;
                    if count > 4096 {
                        return Err(Error::Corrupt("forge upload receipt count"));
                    }
                    let r: UploadRecord = decode(&bytes)?;
                    if r.principal == actor.principal
                        && r.expires_at > now
                        && r.descriptor.descriptor()?.key == object
                    {
                        allowed = true;
                        break;
                    }
                }
            }
        }
        if !allowed {
            return Err(Error::ForgeAccessDenied);
        }
        self.inner
            .descriptor(view.repository(), object)?
            .ok_or(Error::ForgeAccessDenied)
    }
    pub fn forge_read_object_chunk(
        &self,
        view: &ResolvedView,
        actor: Option<&ForgeActor>,
        object: ObjectKey,
        now: i64,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(Descriptor, Vec<u8>, bool)> {
        if max_bytes > 64 * 1024 {
            return Err(Error::Limit("forge object chunk"));
        }
        let descriptor = self.forge_authorized_object_descriptor(view, actor, object, now)?;
        if offset > descriptor.size_bytes {
            return Err(Error::Invalid("forge object offset"));
        }
        if max_bytes == 0 {
            return Ok((descriptor, vec![], offset == descriptor.size_bytes));
        }
        let mut lease = self.read(view.repository(), object)?;
        lease.file.seek(SeekFrom::Start(offset))?;
        let length = (descriptor.size_bytes - offset).min(max_bytes as u64) as usize;
        let mut bytes = vec![0; length];
        lease.read_exact(&mut bytes)?;
        self.forge_check_view(view)?;
        Ok((
            descriptor,
            bytes,
            offset + length as u64 == descriptor.size_bytes,
        ))
    }
    /// Uploaded metadata is readable only by its submitting authenticated actor.
    pub fn forge_read_authorized_object(
        &self,
        view: &ResolvedView,
        actor: Option<&ForgeActor>,
        object: ObjectKey,
        now: i64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        match self.forge_read_projected_object(view, object, now, max_bytes) {
            Ok(bytes) => return Ok(bytes),
            Err(Error::ForgeAccessDenied) => (),
            Err(error) => return Err(error),
        }
        let actor = actor.ok_or(Error::ForgeAccessDenied)?;
        {
            let _gate = self.inner.lock_read()?;
            let tx = self.inner.db.read_tx();
            self.inner.forge_check_view_in(&tx, view)?;
            let actor_view = self.inner.forge_resolve_view_in(
                &tx,
                view.account(),
                Some(actor),
                view.version(),
            )?;
            if actor_view.epoch() != view.epoch() {
                return Err(Error::StaleForgePolicy);
            }
            let prefix = format!(
                "forge/uploads/v1/{}/{}/{}/",
                view.account().0,
                hex(&view.version().0),
                view.epoch().0
            );
            let mut found = false;
            let mut count = 0;
            for entry in tx.prefix(&self.inner.spaces.meta, prefix) {
                let (_, bytes) = entry.into_inner()?;
                count += 1;
                if count > 4096 {
                    return Err(Error::Corrupt("forge upload receipt count"));
                }
                let r: UploadRecord = decode(&bytes)?;
                if r.principal == actor.principal
                    && r.expires_at > now
                    && r.descriptor.descriptor()?.key == object
                {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(Error::ForgeAccessDenied);
            }
        }
        let mut lease = self.read(view.repository(), object)?;
        if lease.descriptor().size_bytes > max_bytes as u64 {
            return Err(Error::Limit("forge uploaded object"));
        }
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes)?;
        self.forge_check_view(view)?;
        Ok(bytes)
    }
    /// Receipt proves caller-supplied bytes, not merely knowledge of a digest.
    pub fn forge_upload_file(
        &self,
        view: &ResolvedView,
        actor: &ForgeActor,
        canonical_path: &str,
        bytes: &[u8],
        now: i64,
    ) -> Result<WriteReceipt> {
        if bytes.len() > 1024 * 1024 {
            return Err(Error::Limit("forge upload file"));
        }
        let descriptor = tinytree_model::describe_file(bytes);
        self.forge_upload_object_stream(
            view,
            actor,
            canonical_path,
            descriptor,
            &mut io::Cursor::new(bytes),
            now,
        )
    }
    /// Bounded streaming session finalization; raw FILE bytes are never buffered
    /// wholesale in memory. Metadata remains a bounded native envelope.
    pub fn forge_upload_object_stream(
        &self,
        view: &ResolvedView,
        actor: &ForgeActor,
        canonical_path: &str,
        descriptor: Descriptor,
        input: &mut impl Read,
        now: i64,
    ) -> Result<WriteReceipt> {
        if descriptor.key.kind != Kind::File {
            if descriptor.size_bytes > 1024 * 1024 {
                return Err(Error::Limit("forge upload metadata"));
            }
            let mut bytes = Vec::new();
            input.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
            return self.forge_upload_object(view, actor, canonical_path, descriptor, &bytes, now);
        }
        if descriptor.size_bytes > 64 * 1024 * 1024 {
            return Err(Error::Limit("forge streamed file"));
        }
        if !view.allows_path(canonical_path) {
            return Err(Error::ForgeAccessDenied);
        }
        self.forge_check_view(view)?;
        let actor_view = self.forge_resolve_view(
            view.account(),
            Some(actor),
            view.name(),
            Some(view.version()),
        )?;
        if !actor_view.allows_path(canonical_path) {
            return Err(Error::ForgeAccessDenied);
        }
        self.forge_authorize(
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let receipt = self.put_stream(view.repository(), descriptor, input, now)?;
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        self.inner.forge_check_view_in(&tx, view)?;
        self.inner.forge_authorize_in(
            &tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let uk = upload_key(view, actor, canonical_path, descriptor.key.id.0);
        let ck = format!("forge/upload-count/v1/{}", view.account().0);
        let count = tx
            .get(&self.inner.spaces.meta, &ck)?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if tx.get(&self.inner.spaces.meta, &uk)?.is_none() {
            if count >= 4096 {
                return Err(Error::Limit("forge upload receipts"));
            }
            tx.insert(&self.inner.spaces.meta, ck, encode(&(count + 1))?);
        }
        tx.insert(
            &self.inner.spaces.meta,
            uk,
            encode(&UploadRecord {
                account: view.account().clone(),
                view: view.version(),
                epoch: view.epoch(),
                principal: actor.principal.clone(),
                path: canonical_path.into(),
                descriptor: descriptor.into(),
                expires_at: receipt.expires_at,
            })?,
        );
        self.inner.commit(tx)?;
        Ok(receipt)
    }
}
impl Inner {
    /// Resolve only an exact server-created projected base for this policy epoch.
    pub(crate) fn forge_projection_base_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        projected: CommitId,
    ) -> Result<CommitId> {
        self.forge_check_view_in(tx, view)?;
        self.forge_projection_relation_in(
            tx,
            view.account(),
            view.version(),
            view.epoch(),
            projected,
        )
    }
    /// Trusted immutable ledger relation only, not a live object-read capability.
    /// Historical retained review metadata survives ACL changes/view retirement.
    pub(crate) fn forge_projection_relation_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        version: ViewVersion,
        epoch: PolicyEpoch,
        projected: CommitId,
    ) -> Result<CommitId> {
        account.validate()?;
        let base = format!(
            "forge/projections/v1/{}/{}/{}",
            account.0,
            hex(&version.0),
            epoch.0
        );
        let mut count = 0;
        for entry in tx.prefix(&self.spaces.meta, base.as_bytes()) {
            let (physical, bytes) = entry.into_inner()?;
            count += 1;
            if count > 4096 {
                return Err(Error::Corrupt("forge projection count"));
            }
            if bytes.len() > 8 * 1024 * 1024 {
                return Err(Error::Corrupt("forge projection record"));
            }
            let record: ProjectionRecord = decode_catalog(&bytes)?;
            if record.binding.account_id != *account
                || record.binding.view_version != version
                || record.binding.policy_epoch != epoch
            {
                continue;
            }
            if physical.as_ref() != record.record_key.as_bytes()
                || !(record.record_key == base
                    || record.record_key.starts_with(&format!("{base}/review/")))
                || record.binding.canonical_revision == CommitId::ROOT.0
                || record
                    .objects
                    .binary_search(&(Kind::Commit as u8, record.binding.projected_commit))
                    .is_err()
            {
                return Err(Error::Corrupt("forge historical projection binding"));
            }
            if record.binding.projected_commit == projected.0 {
                return Ok(CommitId(record.binding.canonical_revision));
            }
        }
        Err(Error::ForgeAccessDenied)
    }

    fn forge_validate_owned_entry_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        path: &str,
        descriptor: Descriptor,
        now: i64,
    ) -> Result<()> {
        let record: ProjectionRecord = decode_catalog(
            &tx.get(&self.spaces.meta, projection_key(view))?
                .ok_or(Error::ForgeAccessDenied)?,
        )?;
        if record
            .objects
            .binary_search(&(descriptor.key.kind as u8, descriptor.key.id.0))
            .is_ok()
        {
            return Ok(());
        }
        let prefix = format!(
            "{}/review/",
            String::from_utf8(projection_key(view))
                .map_err(|_| Error::Corrupt("forge projection key"))?
        );
        let mut count = 0;
        for entry in tx.prefix(&self.spaces.meta, prefix) {
            let (_, bytes) = entry.into_inner()?;
            count += 1;
            if count > 4096 {
                return Err(Error::Corrupt("forge projection count"));
            }
            let r: ProjectionRecord = decode_catalog(&bytes)?;
            if r.binding.account_id == *view.account()
                && r.binding.view_version == view.version()
                && r.binding.policy_epoch == view.epoch()
                && r.objects
                    .binary_search(&(descriptor.key.kind as u8, descriptor.key.id.0))
                    .is_ok()
            {
                return Ok(());
            }
        }
        let r: UploadRecord = decode(
            &tx.get(
                &self.spaces.meta,
                upload_key(view, actor, path, descriptor.key.id.0),
            )?
            .ok_or(Error::ForgeAccessDenied)?,
        )?;
        if r.account != *view.account()
            || r.view != view.version()
            || r.epoch != view.epoch()
            || r.principal != actor.principal
            || r.path != path
            || r.descriptor.descriptor()? != descriptor
            || r.expires_at <= now
        {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(())
    }
    fn forge_validate_metadata_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        path: &str,
        descriptor: Descriptor,
        now: i64,
    ) -> Result<()> {
        let actor_view =
            self.forge_resolve_view_in(tx, view.account(), Some(actor), view.version())?;
        let mut pending = vec![(path.to_owned(), descriptor, 0)];
        let mut entries = 0usize;
        while let Some((path, descriptor, depth)) = pending.pop() {
            if depth > 64 {
                return Err(Error::Limit("forge upload tree depth"));
            }
            match self.metadata(view.repository(), descriptor)? {
                Object::Commit(commit) => {
                    if !path.is_empty()
                        || depth != 0
                        || commit.root_tree.terms.len() != 1
                        || commit.predecessors.len() > 128
                        || commit.parents.len() != 1
                    {
                        return Err(Error::Invalid("forge uploaded commit shape"));
                    }
                    for predecessor in &commit.predecessors {
                        let descriptor = self
                            .descriptor(
                                view.repository(),
                                ObjectKey {
                                    kind: Kind::Commit,
                                    id: ObjectId(predecessor.0),
                                },
                            )?
                            .ok_or(Error::ForgeAccessDenied)?;
                        self.forge_validate_owned_entry_in(tx, view, actor, "", descriptor, now)?;
                    }
                    let parent = commit.parents[0];
                    if parent != CommitId::ROOT {
                        let d = self
                            .descriptor(
                                view.repository(),
                                ObjectKey {
                                    kind: Kind::Commit,
                                    id: ObjectId(parent.0),
                                },
                            )?
                            .ok_or(Error::ForgeAccessDenied)?;
                        self.forge_validate_owned_entry_in(tx, view, actor, "", d, now)?;
                    }
                    let d = self
                        .descriptor(
                            view.repository(),
                            ObjectKey {
                                kind: Kind::Tree,
                                id: ObjectId(commit.root_tree.terms[0].0),
                            },
                        )?
                        .ok_or(Error::ForgeAccessDenied)?;
                    pending.push((String::new(), d, depth + 1));
                }
                Object::Tree(tree) => {
                    for entry in tree.entries {
                        entries += 1;
                        if entries > 10_000 {
                            return Err(Error::Limit("forge upload tree entries"));
                        }
                        let full = if path.is_empty() {
                            entry.name
                        } else {
                            format!("{path}/{}", entry.name)
                        };
                        let (key, size) = match entry.value {
                            TreeValue::Tree(id) => (
                                ObjectKey {
                                    kind: Kind::Tree,
                                    id: ObjectId(id.0),
                                },
                                None,
                            ),
                            TreeValue::File {
                                id,
                                size_bytes,
                                copy_id,
                                ..
                            } => {
                                if let Some(copy) = copy_id {
                                    self.forge_validate_copy_in(tx, view, actor, &full, copy, now)?;
                                }
                                (
                                    ObjectKey {
                                        kind: Kind::File,
                                        id: ObjectId(id.0),
                                    },
                                    Some(size_bytes),
                                )
                            }
                            TreeValue::Symlink { id, .. } => (
                                ObjectKey {
                                    kind: Kind::Symlink,
                                    id: ObjectId(id.0),
                                },
                                None,
                            ),
                        };
                        let d = self
                            .descriptor(view.repository(), key)?
                            .ok_or(Error::ForgeAccessDenied)?;
                        if size.is_some_and(|s| s != d.size_bytes) {
                            return Err(Error::Invalid("forge uploaded file size"));
                        }
                        if key.kind == Kind::Tree {
                            pending.push((full, d, depth + 1));
                        } else {
                            if !view.allows_path(&full) || !actor_view.allows_path(&full) {
                                return Err(Error::ForgeAccessDenied);
                            }
                            self.forge_validate_owned_entry_in(tx, view, actor, &full, d, now)?;
                        }
                    }
                }
                Object::Symlink(_) => {
                    if !view.allows_path(&path) || !actor_view.allows_path(&path) {
                        return Err(Error::ForgeAccessDenied);
                    }
                }
                Object::CopyHistory(history) => {
                    if history.current_path != path
                        || !view.allows_path(&path)
                        || !actor_view.allows_path(&path)
                        || history.parents.len() > 16
                    {
                        return Err(Error::ForgeAccessDenied);
                    }
                    for parent in history.parents {
                        let d = self
                            .descriptor(
                                view.repository(),
                                ObjectKey {
                                    kind: Kind::CopyHistory,
                                    id: ObjectId(parent.0),
                                },
                            )?
                            .ok_or(Error::ForgeAccessDenied)?;
                        let Object::CopyHistory(origin) = self.metadata(view.repository(), d)?
                        else {
                            return Err(Error::Corrupt("forge copy parent"));
                        };
                        if !view.allows_path(&origin.current_path)
                            || !actor_view.allows_path(&origin.current_path)
                        {
                            return Err(Error::ForgeAccessDenied);
                        }
                        self.forge_validate_owned_entry_in(
                            tx,
                            view,
                            actor,
                            &origin.current_path,
                            d,
                            now,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
    /// Server-owned provenance mapping; never serialized into projected objects.
    pub(crate) fn forge_projected_copy_origin_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        copy: CopyId,
    ) -> Result<ForgeCopyOrigin> {
        self.forge_check_view_in(tx, view)?;
        let base = String::from_utf8(projection_key(view))
            .map_err(|_| Error::Corrupt("forge projection key"))?;
        let mut count = 0;
        for entry in tx.prefix(&self.spaces.meta, base.as_bytes()) {
            let (_, bytes) = entry.into_inner()?;
            count += 1;
            if count > 4096 {
                return Err(Error::Corrupt("forge projection count"));
            }
            let record: ProjectionRecord = decode_catalog(&bytes)?;
            if record.binding.account_id == *view.account()
                && record.binding.view_version == view.version()
                && record.binding.policy_epoch == view.epoch()
            {
                if let Some(origin) = record
                    .copies
                    .into_iter()
                    .find(|c| c.projected_copy == copy.0)
                {
                    return Ok(origin);
                }
            }
        }
        Err(Error::ForgeAccessDenied)
    }
    pub(crate) fn forge_validate_copy_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        path: &str,
        copy: CopyId,
        now: i64,
    ) -> Result<()> {
        self.forge_check_view_in(tx, view)?;
        let current =
            self.forge_resolve_view_in(tx, view.account(), Some(actor), view.version())?;
        if !view.allows_path(path) || !current.allows_path(path) {
            return Err(Error::ForgeAccessDenied);
        }
        let descriptor = self
            .descriptor(
                view.repository(),
                ObjectKey {
                    kind: Kind::CopyHistory,
                    id: ObjectId(copy.0),
                },
            )?
            .ok_or(Error::ForgeAccessDenied)?;
        self.forge_validate_owned_entry_in(tx, view, actor, path, descriptor, now)?;
        let Object::CopyHistory(history) = self.metadata(view.repository(), descriptor)? else {
            return Err(Error::Corrupt("forge copy type"));
        };
        if history.current_path != path {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(())
    }
    /// Admission of a submitted source commit requires its exact supplied-byte
    /// receipt. Knowing a stored commit ID, or a visible leaf, is not ownership.
    pub(crate) fn forge_validate_review_head_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        head: CommitId,
        now: i64,
    ) -> Result<()> {
        self.forge_check_view_in(tx, view)?;
        self.forge_authorize_in(
            tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let current =
            self.forge_resolve_view_in(tx, view.account(), Some(actor), view.version())?;
        if current.epoch() != view.epoch() {
            return Err(Error::StaleForgePolicy);
        }
        let bytes = tx
            .get(&self.spaces.meta, upload_key(view, actor, "", head.0))?
            .ok_or(Error::ForgeAccessDenied)?;
        let receipt: UploadRecord = decode(&bytes)?;
        let descriptor = receipt.descriptor.descriptor()?;
        if receipt.account != *view.account()
            || receipt.view != view.version()
            || receipt.epoch != view.epoch()
            || receipt.principal != actor.principal
            || !receipt.path.is_empty()
            || receipt.expires_at <= now
            || descriptor.key.kind != Kind::Commit
            || descriptor.key.id.0 != head.0
        {
            return Err(Error::ForgeAccessDenied);
        }
        self.require_descriptor(view.repository(), descriptor)
    }
    /// Changed regular-file/symlink authorization for the trusted review builder.
    pub(crate) fn forge_validate_entry_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        path: &str,
        descriptor: Descriptor,
        now: i64,
    ) -> Result<()> {
        if descriptor.key.kind == Kind::File {
            return self.forge_validate_file_in(tx, view, actor, path, descriptor, now);
        }
        if descriptor.key.kind != Kind::Symlink {
            return Err(Error::ForgeAccessDenied);
        }
        self.forge_check_view_in(tx, view)?;
        self.forge_authorize_in(
            tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let current =
            self.forge_resolve_view_in(tx, view.account(), Some(actor), view.version())?;
        if !view.allows_path(path) || !current.allows_path(path) {
            return Err(Error::ForgeAccessDenied);
        }
        self.require_descriptor(view.repository(), descriptor)?;
        self.forge_validate_owned_entry_in(tx, view, actor, path, descriptor, now)
    }
    /// Caller holds Store gate; prevents private-digest laundering in review trees.
    pub(crate) fn forge_validate_file_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
        actor: &ForgeActor,
        canonical_path: &str,
        descriptor: Descriptor,
        now: i64,
    ) -> Result<()> {
        self.forge_check_view_in(tx, view)?;
        self.forge_authorize_in(
            tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let actor_view =
            self.forge_resolve_view_in(tx, view.account(), Some(actor), view.version())?;
        if descriptor.key.kind != Kind::File
            || !view.allows_path(canonical_path)
            || !actor_view.allows_path(canonical_path)
        {
            return Err(Error::ForgeAccessDenied);
        }
        self.require_descriptor(view.repository(), descriptor)?;
        self.forge_validate_owned_entry_in(tx, view, actor, canonical_path, descriptor, now)
    }
    pub(crate) fn forge_retire_projection_roots_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        account: &AccountId,
        version: ViewVersion,
    ) -> Result<()> {
        let prefix = format!("forge/projections/v1/{}/{}/", account.0, hex(&version.0));
        let mut keys = Vec::new();
        for entry in tx.prefix(&self.spaces.meta, prefix.as_bytes()) {
            let (physical, _) = entry.into_inner()?;
            if keys.len() >= MAX_PROJECTIONS as usize || physical.len() > 512 {
                return Err(Error::Limit("forge projection retirement"));
            }
            keys.push(physical.to_vec());
        }
        for physical in keys {
            let bytes = tx
                .get(&self.spaces.meta, &physical)?
                .ok_or(Error::Corrupt("forge projection record"))?;
            let record = decode_catalog(&bytes)?;
            validate_record_key(&record, &physical)?;
            if record.binding.account_id != *account || record.binding.view_version != version {
                return Err(Error::Corrupt("forge projection retirement binding"));
            }
            let marker = retired_projection_key(&record);
            let root = key(
                &account.repository_id(),
                reference(&record.binding).as_bytes(),
            );
            if let Some(value) = tx.get(&self.spaces.meta, &marker)? {
                if value.as_ref() != record.record_key.as_bytes()
                    || tx.get(&self.spaces.refs, &root)?.is_some()
                {
                    return Err(Error::Corrupt("forge projection retirement"));
                }
            } else {
                let reference: StoredReference = decode(
                    &tx.get(&self.spaces.refs, &root)?
                        .ok_or(Error::Corrupt("forge projection root"))?,
                )?;
                let descriptor = reference
                    .commit
                    .ok_or(Error::Corrupt("forge projection root"))?
                    .descriptor()?;
                if reference.generation != 1
                    || descriptor.key.kind != Kind::Commit
                    || descriptor.key.id.0 != record.binding.projected_commit
                {
                    return Err(Error::Corrupt("forge projection root"));
                }
                tx.remove(&self.spaces.refs, root);
                tx.insert(&self.spaces.meta, marker, record.record_key.as_bytes());
            }
        }
        Ok(())
    }
    pub(crate) fn validate_forge_projections_on_reopen(&self) -> Result<()> {
        let tx = self.db.read_tx();
        let mut counts = BTreeMap::<String, u64>::new();
        for entry in tx.prefix(&self.spaces.meta, PREFIX) {
            let (physical, bytes) = entry.into_inner()?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err(Error::Corrupt("forge projection record"));
            }
            let r: ProjectionRecord = decode_catalog(&bytes)?;
            r.binding.account_id.validate()?;
            let repo = r.binding.account_id.repository_id();
            let expected = format!(
                "forge/projections/v1/{}/{}/{}",
                r.binding.account_id.0,
                hex(&r.binding.view_version.0),
                r.binding.policy_epoch.0
            );
            if physical.as_ref() != r.record_key.as_bytes()
                || !(r.record_key == expected
                    || r.record_key.starts_with(&format!("{expected}/review/")))
                || r.objects.len() > 30_002
                || !r.objects.windows(2).all(|p| p[0] < p[1])
            {
                return Err(Error::Corrupt("forge projection binding"));
            }
            if let Some(marker) = tx.get(&self.spaces.meta, retired_projection_key(&r))? {
                if tx
                    .get(
                        &self.spaces.meta,
                        format!(
                            "forge/view-retired/v1/{}/{}",
                            r.binding.account_id.0,
                            hex(&r.binding.view_version.0)
                        ),
                    )?
                    .as_deref()
                    != Some(&[1][..])
                    || marker.as_ref() != r.record_key.as_bytes()
                    || tx
                        .get(
                            &self.spaces.refs,
                            key(&repo, reference(&r.binding).as_bytes()),
                        )?
                        .is_some()
                {
                    return Err(Error::Corrupt("forge retired projection root"));
                }
                *counts.entry(r.binding.account_id.0).or_default() += 1;
                continue;
            }
            let reference: StoredReference = decode(
                &tx.get(
                    &self.spaces.refs,
                    key(&repo, reference(&r.binding).as_bytes()),
                )?
                .ok_or(Error::Corrupt("forge projection root"))?,
            )?;
            let descriptor = reference
                .commit
                .ok_or(Error::Corrupt("forge projection root"))?
                .descriptor()?;
            if reference.generation != 1
                || descriptor.key.kind != Kind::Commit
                || descriptor.key.id.0 != r.binding.projected_commit
            {
                return Err(Error::Corrupt("forge projection root"));
            }
            let closure: BTreeSet<_> = self
                .closure(&repo, [descriptor.key], true, false)?
                .into_iter()
                .map(|k| (k.kind as u8, k.id.0))
                .collect();
            if closure != r.objects.into_iter().collect() {
                return Err(Error::Corrupt("forge projection closure"));
            }
            if r.copies.len() > 10_000 {
                return Err(Error::Corrupt("forge projected copy count"));
            }
            let mut copies = BTreeSet::new();
            for origin in r.copies {
                if origin.canonical_revision != r.binding.canonical_revision
                    || !copies.insert(origin.projected_copy)
                    || !closure.contains(&(Kind::CopyHistory as u8, origin.projected_copy))
                    || !closure.contains(&(Kind::File as u8, origin.file))
                {
                    return Err(Error::Corrupt("forge projected copy binding"));
                }
                let d = self
                    .descriptor(
                        &repo,
                        ObjectKey {
                            kind: Kind::CopyHistory,
                            id: ObjectId(origin.projected_copy),
                        },
                    )?
                    .ok_or(Error::Corrupt("forge projected copy"))?;
                let Object::CopyHistory(copy) = self.metadata(&repo, d)? else {
                    return Err(Error::Corrupt("forge projected copy"));
                };
                if copy.current_path != origin.canonical_path || !copy.parents.is_empty() {
                    return Err(Error::Corrupt("forge projected copy ancestry"));
                }
            }
            *counts.entry(r.binding.account_id.0).or_default() += 1;
        }
        let mut retired_count = 0usize;
        for entry in tx.prefix(&self.spaces.meta, RETIRED) {
            let (physical, value) = entry.into_inner()?;
            retired_count += 1;
            if retired_count > 1024 * MAX_PROJECTIONS as usize || value.len() > 512 {
                return Err(Error::Corrupt("forge retired projection count"));
            }
            let record = decode_catalog(
                &tx.get(&self.spaces.meta, &value)?
                    .ok_or(Error::Corrupt("forge retired projection record"))?,
            )?;
            validate_record_key(&record, &value)?;
            if physical.as_ref() != retired_projection_key(&record).as_slice()
                || tx
                    .get(
                        &self.spaces.refs,
                        key(
                            &record.binding.account_id.repository_id(),
                            reference(&record.binding).as_bytes(),
                        ),
                    )?
                    .is_some()
            {
                return Err(Error::Corrupt("forge retired projection binding"));
            }
        }
        for (account, count) in counts {
            if count > MAX_PROJECTIONS
                || tx
                    .get(
                        &self.spaces.meta,
                        format!("forge/projection-count/v1/{account}"),
                    )?
                    .map(|b| decode::<u64>(&b))
                    .transpose()?
                    != Some(count)
            {
                return Err(Error::Corrupt("forge projection count"));
            }
        }
        for entry in tx.prefix(&self.spaces.meta, UPLOAD) {
            let (_, bytes) = entry.into_inner()?;
            if bytes.len() > 16 * 1024 {
                return Err(Error::Corrupt("forge upload record"));
            }
            let r: UploadRecord = decode(&bytes)?;
            r.account.validate()?;
            r.descriptor.descriptor()?;
        }
        Ok(())
    }
}
