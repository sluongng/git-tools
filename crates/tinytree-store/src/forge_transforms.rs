//! Bounded cross-account projections, explicit provenance and retained pins.
//!
//! Inputs are authorized view snapshots. Source ancestry is never copied into
//! the destination: projected inputs receive a synthetic, parentless artifact.
//! The durable pin and its reserved reference share one Store transaction.
use super::*;
use crate::forge_accounts::{AccountId, ForgeAction, ForgeActor, ResolvedView};
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model::{ChangeId, Commit, FileId, Object, Signature, TreeId, TreeValue};
#[cfg(test)]
use tinytree_model::{Timestamp, Tree, TreeEntry, TreeMerge};

const PREFIX: &[u8] = b"forge/transforms/v1/";
const MAX_RUNS: usize = 1024;
const RETIRE: &[u8] = b"forge/transform-retire/v1/";
use tinytree_forge_core::transforms::{
    self as transforms, FrozenCommitter as Person, MAX_BYTES, MAX_FILE_BYTES, MAX_FILES,
    MAX_RECORD_BYTES, ProjectedFile, TransformIdentityInput as Input, join_path as join,
    validate_path as path,
};
pub use tinytree_forge_core::transforms::{
    ForgeDependencyPin, ForgeLineageEdge, ForgeLineageHint, ForgeLineageOperation,
    ForgePathMapping, ForgePinChunk, ForgePinFile, ForgePinPage, ForgeTransformOrigin,
    ForgeTransformRun, ForgeTransformSpec, ForgeTransformView,
};
impl From<transforms::Error> for Error {
    fn from(error: transforms::Error) -> Self {
        match error {
            transforms::Error::Invalid(reason) => Self::Invalid(reason),
            transforms::Error::Limit(reason) => Self::Limit(reason),
            transforms::Error::Corrupt(reason) => Self::Corrupt(reason),
            transforms::Error::Model(error) => Self::Model(error),
        }
    }
}
#[derive(Clone)]
pub struct ForgeTransformRequest {
    pub request_id: String,
    pub before: ResolvedView,
    pub after: ResolvedView,
    pub destination: ResolvedView,
    pub spec: ForgeTransformSpec,
    pub change_id: ChangeId,
    pub lineage: Vec<ForgeLineageHint>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    input: Input,
    committer: Person,
    run: Option<ForgeTransformRun>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetireReplay {
    account: String,
    principal: String,
    view: String,
    request: String,
    id: [u8; 32],
    fingerprint: [u8; 32],
    pin: ForgeDependencyPin,
}
fn retire_prefix(repo: &str) -> Vec<u8> {
    [RETIRE, repo.as_bytes(), b"/"].concat()
}
fn retire_key(repo: &str, principal: &str, request: &str) -> Vec<u8> {
    [
        retire_prefix(repo),
        hex(&tinytree_forge_core::identity::operation_id(
            principal, request,
        ))
        .into_bytes(),
    ]
    .concat()
}

impl Inner {
    /// Admit only a service-produced, durably retained exact transform output.
    /// Caller-supplied provenance identifiers alone confer no authority.
    pub(crate) fn forge_validate_transform_review_in(
        &self,
        tx: &impl fjall::Readable,
        account: &AccountId,
        actor: &ForgeActor,
        run_id: [u8; 32],
        base: CommitId,
        head: CommitId,
        view_version: crate::forge_accounts::ViewVersion,
    ) -> Result<()> {
        self.forge_authorize_in(tx, account, actor, ForgeAction::Write, None)?;
        let repo = account.repository_id();
        let bytes = tx
            .get(&self.spaces.meta, record_key(&repo, &run_id))?
            .ok_or(Error::ForgeAccessDenied)?;
        let record: Record = bounded_decode(&bytes)?;
        if transforms::run_id(&record.input)? != run_id {
            return Err(Error::Corrupt("transform record identity"));
        }
        let run = record.run.ok_or(Error::ForgeAccessDenied)?;
        if run.id != run_id
            || run.destination_account != account.0
            || run.destination_base != base.0
            || run.output_head != head.0
            || run.destination_view != view_version.0
            || run.pin.retired
        {
            return Err(Error::ForgeAccessDenied);
        }
        let selected = self.forge_resolve_view_in(tx, account, Some(actor), view_version)?;
        if run
            .destination_paths
            .iter()
            .any(|path| !selected.allows_path(path))
        {
            return Err(Error::ForgeAccessDenied);
        }
        for (name, expected) in [
            (
                format!("_tinytree/forge/pins/v1/{}", hex(&run_id)),
                run.pin.artifact,
            ),
            (
                format!("_tinytree/forge/output/v1/{}", hex(&run_id)),
                run.output_head,
            ),
        ] {
            let retained = reference(
                name.clone(),
                tx.get(&self.spaces.refs, key(&repo, name.as_bytes()))?
                    .as_deref(),
            )?;
            if retained.generation != run.pin.generation
                || retained.commit.map(|d| d.key.id.0) != Some(expected)
            {
                return Err(Error::Corrupt("transform retention reference"));
            }
        }
        self.closure(
            &repo,
            [
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(head.0),
                },
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(run.pin.artifact),
                },
            ],
            true,
            true,
        )?;
        Ok(())
    }
}

impl Store {
    /// Prepare an exact transformed proposal and a destination-owned durable pin.
    /// This never advances main. The review service consumes `output_head`.
    pub fn forge_prepare_transform(
        &self,
        actor: &ForgeActor,
        request: &ForgeTransformRequest,
        committer: &Signature,
        now: i64,
    ) -> Result<ForgeTransformRun> {
        request_token(&request.request_id)?;
        request.spec.validate()?;
        if request.lineage.len() > MAX_FILES {
            return Err(Error::Limit("transform lineage hints"));
        }
        if request.before.account() != request.after.account()
            || request.after.account() == request.destination.account()
        {
            return Err(Error::Invalid("transform account endpoints"));
        }
        self.forge_authorize_view_export(&request.before)?;
        self.forge_authorize_view_export(&request.after)?;
        self.forge_authorize(
            request.destination.account(),
            actor,
            ForgeAction::Write,
            Some(request.destination.epoch()),
        )?;
        // Reject unsupported canonical paths before persisting an intent or
        // copying any source bytes. The destination root contributes depth.
        for source in [&request.before, &request.after] {
            let mut paths = transforms::TransformPathBudget::default();
            for file in self.forge_view_files(source, MAX_FILES)? {
                if let Some(mapped) = request.spec.map_path(&file.path)? {
                    let canonical = join(request.destination.root_prefix(), &mapped);
                    paths.observe(&canonical)?;
                    if !request.destination.allows_path(&canonical) {
                        return Err(Error::Invalid("transform destination path denied"));
                    }
                }
            }
        }
        let input = Input {
            spec: request.spec.id()?,
            source_account: request.after.account().0.clone(),
            before: request.before.revision().0,
            after: request.after.revision().0,
            before_view: request.before.version().0,
            after_view: request.after.version().0,
            before_view_name: request.before.name().into(),
            after_view_name: request.after.name().into(),
            destination_account: request.destination.account().0.clone(),
            destination_view: request.destination.version().0,
            destination_view_name: request.destination.name().into(),
            destination_base: request.destination.revision().0,
            change_id: request.change_id.0,
            lineage: request.lineage.clone(),
        };
        let id = transforms::run_id(&input)?;
        let repo = request.destination.repository();
        let run_key = record_key(repo, &id);
        let record = {
            let _guard = self.inner.lock()?;
            self.inner.repository(repo)?;
            let request_key = request_key(repo, &request.request_id);
            let view = self.inner.db.read_tx();
            self.inner
                .forge_authorize_view_export_in(&view, &request.before)?;
            self.inner
                .forge_authorize_view_export_in(&view, &request.after)?;
            self.inner.forge_authorize_in(
                &view,
                request.destination.account(),
                actor,
                ForgeAction::Write,
                Some(request.destination.epoch()),
            )?;
            self.inner
                .forge_check_view_in(&view, &request.destination)?;
            if let Some(existing) = view.get(&self.inner.spaces.meta, &request_key)? {
                if existing.as_ref() != id {
                    return Err(Error::IdempotencyConflict);
                }
            }
            if let Some(bytes) = view.get(&self.inner.spaces.meta, &run_key)? {
                let record: Record = bounded_decode(&bytes)?;
                if view.get(&self.inner.spaces.meta, &request_key)?.is_none() {
                    if view
                        .prefix(&self.inner.spaces.meta, request_prefix(repo))
                        .take(MAX_RUNS * 4)
                        .count()
                        >= MAX_RUNS * 4
                    {
                        return Err(Error::Limit("transform request records"));
                    }
                    let mut tx = self
                        .inner
                        .db
                        .write_tx()
                        .durability(Some(PersistMode::SyncAll));
                    tx.insert(&self.inner.spaces.meta, request_key, id);
                    self.inner.commit(tx)?;
                }
                if let Some(run) = &record.run {
                    return Ok(run.clone());
                }
                record
            } else {
                if view
                    .prefix(&self.inner.spaces.meta, record_prefix(repo))
                    .take(MAX_RUNS)
                    .count()
                    >= MAX_RUNS
                {
                    return Err(Error::Limit("transform run records"));
                }
                let record = Record {
                    input,
                    committer: Person::capture(committer),
                    run: None,
                };
                let mut tx = self
                    .inner
                    .db
                    .write_tx()
                    .durability(Some(PersistMode::SyncAll));
                tx.insert(&self.inner.spaces.meta, &run_key, bounded_encode(&record)?);
                tx.insert(&self.inner.spaces.meta, request_key, id);
                self.inner.commit(tx)?;
                record
            }
        };

        let mut guards = Vec::new();
        let before =
            self.project_transform_input(&request.before, &request.spec, repo, now, &mut guards)?;
        let after =
            self.project_transform_input(&request.after, &request.spec, repo, now, &mut guards)?;
        let target = self.read_commit(repo, CommitId(record.input.destination_base))?;
        if target.root_tree.terms.len() != 1 {
            return Err(Error::Invalid("conflicted transform destination"));
        }
        let _target_guard = self.pin_root(
            repo,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(record.input.destination_base),
            },
        )?;
        let destination = self.flatten_tree(repo, target.root_tree.terms[0])?;
        for mapped in before.keys().chain(after.keys()) {
            if !request
                .destination
                .allows_path(&join(request.destination.root_prefix(), mapped))
            {
                return Err(Error::Invalid("transform destination path denied"));
            }
        }
        let mut lineage = self.verify_transform_lineage(request)?;
        for file in self.forge_view_files(&request.after, MAX_FILES)? {
            if let Some(mapped) = request.spec.map_path(&file.path)? {
                lineage.push(ForgeLineageEdge {
                    operation: ForgeLineageOperation::ImportedOrigin,
                    source_account: record.input.source_account.clone(),
                    source_revision: record.input.after,
                    source_path: file.canonical_path,
                    destination_path: join(request.destination.root_prefix(), &mapped),
                    destination_revision: [0; 32],
                });
            }
        }
        let build = transforms::build_transform(
            &record.input,
            &record.committer,
            request.destination.root_prefix(),
            &before,
            &after,
            &destination,
            lineage,
            self.inner.config.model_limits,
        )?;
        for object in &build.objects {
            self.put(repo, object.descriptor, &object.bytes, now)?;
            guards.push(self.pin_root(repo, object.descriptor.key)?);
        }
        let run = build.run;
        let output_head = self
            .descriptor(
                repo,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(run.output_head),
                },
            )?
            .ok_or(Error::Corrupt("transform output missing"))?;
        let artifact = self
            .descriptor(
                repo,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(run.pin.artifact),
                },
            )?
            .ok_or(Error::Corrupt("transform artifact missing"))?;
        self.forge_authorize_view_export(&request.before)?;
        self.forge_authorize_view_export(&request.after)?;
        self.forge_authorize(
            request.destination.account(),
            actor,
            ForgeAction::Write,
            Some(request.destination.epoch()),
        )?;
        let reference_name = run.pin.reference.clone();
        let _guard = self.inner.lock()?;
        let view = self.inner.db.read_tx();
        self.inner
            .forge_authorize_view_export_in(&view, &request.before)?;
        self.inner
            .forge_authorize_view_export_in(&view, &request.after)?;
        self.inner.forge_authorize_in(
            &view,
            request.destination.account(),
            actor,
            ForgeAction::Write,
            Some(request.destination.epoch()),
        )?;
        self.inner
            .forge_check_view_in(&view, &request.destination)?;
        if let Some(bytes) = self
            .inner
            .db
            .read_tx()
            .get(&self.inner.spaces.meta, &run_key)?
        {
            if let Some(run) = bounded_decode::<Record>(&bytes)?.run {
                return Ok(run);
            }
        }
        self.inner
            .closure(repo, [artifact.key, output_head.key], true, true)?;
        self.inner.check_cloud_gc_union(
            repo,
            &[output_head.key],
            Some((&reference_name, artifact.key)),
        )?;
        let record = Record {
            run: Some(run.clone()),
            ..record
        };
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(
            &self.inner.spaces.refs,
            key(repo, reference_name.as_bytes()),
            encode(&StoredReference {
                generation: 1,
                commit: Some(artifact.into()),
            })?,
        );
        // The output is protected independently until review owns its roots.
        tx.insert(
            &self.inner.spaces.refs,
            key(
                repo,
                format!("_tinytree/forge/output/v1/{}", hex(&id)).as_bytes(),
            ),
            encode(&StoredReference {
                generation: 1,
                commit: Some(output_head.into()),
            })?,
        );
        tx.insert(&self.inner.spaces.meta, &run_key, bounded_encode(&record)?);
        self.inner.commit(tx)?;
        Ok(run)
    }

    /// Inspect a destination-owned run, redacting inaccessible origin metadata.
    pub fn forge_transform_run(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
    ) -> Result<ForgeTransformView> {
        let repo = account.repository_id();
        self.forge_authorize(account, actor, ForgeAction::Read, None)?;
        let record = self.transform_record(&repo, &id)?;
        self.authorize_transform_destination(account, actor, &record)?;
        let run = record.run.ok_or(Error::Invalid("transform not prepared"))?;
        let source = AccountId(run.source_account.clone());
        let origin = self
            .forge_resolve_view(
                &source,
                Some(actor),
                &record.input.before_view_name,
                Some(crate::forge_accounts::ViewVersion(run.source_before_view)),
            )
            .and_then(|before| {
                self.forge_authorize_view_export(&before)?;
                let after = self.forge_resolve_view(
                    &source,
                    Some(actor),
                    &record.input.after_view_name,
                    Some(crate::forge_accounts::ViewVersion(run.source_after_view)),
                )?;
                self.forge_authorize_view_export(&after)
            })
            .ok()
            .map(|_| ForgeTransformOrigin {
                account: run.source_account.clone(),
                before: run.source_before,
                after: run.source_after,
                lineage: run.lineage.clone(),
            });
        Ok(ForgeTransformView {
            id: run.id,
            destination_account: run.destination_account,
            destination_view: run.destination_view,
            destination_base: run.destination_base,
            output_head: run.output_head,
            pin: run.pin,
            origin,
        })
    }

    /// Resolve the exact retained dependency artifact; never fall back to main
    /// or a Git tag after retirement or missing bytes.
    pub fn forge_resolve_pin(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
    ) -> Result<ReadLease> {
        self.forge_authorize(account, actor, ForgeAction::Read, None)?;
        let repo = account.repository_id();
        let record = self.transform_record(&repo, &id)?;
        self.authorize_transform_destination(account, actor, &record)?;
        let run = record.run.ok_or(Error::Invalid("transform not prepared"))?;
        if run.pin.retired {
            return Err(Error::Invalid("dependency pin retired"));
        }
        self.read(
            &repo,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(run.pin.artifact),
            },
        )
    }

    pub fn forge_pin_files(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
        after: Option<&str>,
        limit: usize,
    ) -> Result<ForgePinPage> {
        if limit == 0 || limit > 256 {
            return Err(Error::Limit("dependency pin page"));
        }
        if let Some(path_) = after {
            path(path_, false)?;
        }
        let (guard, files) = self.pin_file_snapshot(account, actor, id)?;
        let mut selected = files
            .into_iter()
            .filter(|(path, _)| after.is_none_or(|after| path.as_str() > after))
            .take(limit + 1)
            .collect::<Vec<_>>();
        let next_path = if selected.len() > limit {
            selected.pop();
            selected.last().map(|(path, _)| path.clone())
        } else {
            None
        };
        let files = selected
            .into_iter()
            .map(|(path, value)| match value {
                TreeValue::File {
                    id,
                    executable,
                    size_bytes,
                    ..
                } => Ok(ForgePinFile {
                    path,
                    file_id: id.0,
                    size_bytes,
                    executable,
                }),
                _ => Err(Error::Corrupt("dependency pin file kind")),
            })
            .collect::<Result<Vec<_>>>()?;
        let record = self.transform_record(&account.repository_id(), &id)?;
        self.authorize_transform_destination(account, actor, &record)?;
        Ok(ForgePinPage {
            pin_id: id,
            artifact: guard.descriptor().key.id.0,
            files,
            next_path,
        })
    }

    pub fn forge_read_pin_file(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
        file: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ForgePinChunk> {
        path(file, false)?;
        if max_bytes == 0 || max_bytes > 64 * 1024 {
            return Err(Error::Limit("dependency pin chunk"));
        }
        let (_guard, files) = self.pin_file_snapshot(account, actor, id)?;
        let Some(TreeValue::File {
            id: file_id,
            size_bytes,
            ..
        }) = files.get(file)
        else {
            return Err(Error::ForgeAccessDenied);
        };
        if offset > *size_bytes {
            return Err(Error::Invalid("dependency pin offset"));
        }
        let mut lease = self.read(
            &account.repository_id(),
            ObjectKey {
                kind: Kind::File,
                id: ObjectId(file_id.0),
            },
        )?;
        lease.file.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::with_capacity(max_bytes.min((*size_bytes - offset) as usize));
        lease.take(max_bytes as u64).read_to_end(&mut bytes)?;
        let record = self.transform_record(&account.repository_id(), &id)?;
        self.authorize_transform_destination(account, actor, &record)?;
        let next_offset = offset + bytes.len() as u64;
        Ok(ForgePinChunk {
            pin_id: id,
            file_id: file_id.0,
            offset,
            bytes,
            next_offset,
            eof: next_offset == *size_bytes,
        })
    }

    fn pin_file_snapshot(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
    ) -> Result<(ReadLease, BTreeMap<String, TreeValue>)> {
        let guard = self.forge_resolve_pin(account, actor, id)?;
        let repo = account.repository_id();
        let artifact = self.read_commit(&repo, CommitId(guard.descriptor().key.id.0))?;
        if artifact.parents != [CommitId::ROOT] || artifact.root_tree.terms.len() != 1 {
            return Err(Error::Corrupt("dependency artifact root"));
        }
        let (_, payload) = self.read_metadata(
            &repo,
            ObjectKey {
                kind: Kind::Tree,
                id: ObjectId(artifact.root_tree.terms[0].0),
            },
        )?;
        let Object::Tree(tree) = tinytree_model::decode(&payload, self.inner.config.model_limits)?
        else {
            return Err(Error::Corrupt("dependency artifact tree"));
        };
        let root = tree
            .entries
            .into_iter()
            .find_map(|e| match (e.name.as_str(), e.value) {
                ("after", TreeValue::Tree(root)) => Some(root),
                _ => None,
            })
            .ok_or(Error::Corrupt("dependency artifact after root"))?;
        Ok((guard, self.flatten_tree(&repo, root)?))
    }

    /// Trusted review/manifest integration records a durable owner of this pin.
    /// Call before acknowledging a new referring review/current/history root.
    pub fn forge_protect_pin(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        id: [u8; 32],
        root: &str,
        present: bool,
    ) -> Result<ForgeDependencyPin> {
        self.forge_authorize(account, actor, ForgeAction::Admin, None)?;
        request_token(root)?;
        let repo = account.repository_id();
        let _guard = self.inner.lock()?;
        let mut record = self.transform_record(&repo, &id)?;
        self.inner.forge_authorize_in(
            &self.inner.db.read_tx(),
            account,
            actor,
            ForgeAction::Admin,
            None,
        )?;
        let run = record
            .run
            .as_mut()
            .ok_or(Error::Invalid("transform not prepared"))?;
        if run.pin.retired {
            return Err(Error::Invalid("dependency pin retired"));
        }
        if present {
            if run.pin.protected_roots.len() >= MAX_RUNS && !run.pin.protected_roots.contains(root)
            {
                return Err(Error::Limit("dependency pin roots"));
            }
            run.pin.protected_roots.insert(root.into());
        } else {
            run.pin.protected_roots.remove(root);
        }
        let result = run.pin.clone();
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(
            &self.inner.spaces.meta,
            record_key(&repo, &id),
            bounded_encode(&record)?,
        );
        self.inner.commit(tx)?;
        Ok(result)
    }

    /// Explicitly retire an unreferenced pin. Historical roots must first be
    /// retired by their owning service; removing the current selection is not
    /// sufficient. Ref tombstones and pin state share the same transaction.
    pub fn forge_retire_pin(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        view_name: &str,
        id: [u8; 32],
        request_id: &str,
    ) -> Result<ForgeDependencyPin> {
        request_token(request_id)?;
        if !view_name.is_empty() {
            tinytree_forge_core::accounts::validate_name(view_name)?;
        }
        let fingerprint =
            transforms::retire_fingerprint(&account.0, &actor.principal, view_name, id)?;
        let repo = account.repository_id();
        let _guard = self.inner.lock()?;
        let db = self.inner.db.read_tx();
        self.inner
            .forge_authorize_in(&db, account, actor, ForgeAction::Admin, None)?;
        let replay_key = retire_key(&repo, &actor.principal, request_id);
        let replay = db
            .get(&self.inner.spaces.meta, &replay_key)?
            .map(|bytes| bounded_decode::<RetireReplay>(&bytes))
            .transpose()?;
        if replay
            .as_ref()
            .is_some_and(|r| r.fingerprint != fingerprint)
        {
            return Err(Error::IdempotencyConflict);
        }
        let mut record = self.transform_record(&repo, &id)?;
        let selected = self.inner.forge_resolve_view_in(
            &db,
            account,
            Some(actor),
            crate::forge_accounts::ViewVersion(record.input.destination_view),
        )?;
        if !view_name.is_empty() && view_name != selected.name() {
            return Err(Error::ForgeAccessDenied);
        }
        let run = record
            .run
            .as_mut()
            .ok_or(Error::Invalid("transform not prepared"))?;
        if run
            .destination_paths
            .iter()
            .any(|p| !selected.allows_path(p))
        {
            return Err(Error::ForgeAccessDenied);
        }
        if let Some(replay) = replay {
            return Ok(replay.pin);
        }
        if db
            .prefix(&self.inner.spaces.meta, retire_prefix(&repo))
            .take(MAX_RUNS * 4)
            .count()
            >= MAX_RUNS * 4
        {
            return Err(Error::Limit("transform retirement records"));
        }
        if !run.pin.retired {
            if !run.pin.protected_roots.is_empty() {
                return Err(Error::Invalid("dependency pin still referenced"));
            }
            run.pin.retired = true;
            run.pin.generation = run
                .pin
                .generation
                .checked_add(1)
                .ok_or(Error::Limit("dependency pin generation"))?;
        }
        let result = run.pin.clone();
        let replay = RetireReplay {
            account: account.0.clone(),
            principal: actor.principal.clone(),
            view: view_name.into(),
            request: request_id.into(),
            id,
            fingerprint,
            pin: result.clone(),
        };
        let replay_bytes = bounded_encode(&replay)?;
        let record_bytes = bounded_encode(&record)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        for reference in [
            &result.reference,
            &format!("_tinytree/forge/output/v1/{}", hex(&id)),
        ] {
            tx.insert(
                &self.inner.spaces.refs,
                key(&repo, reference.as_bytes()),
                encode(&StoredReference {
                    generation: result.generation,
                    commit: None,
                })?,
            );
        }
        tx.insert(
            &self.inner.spaces.meta,
            record_key(&repo, &id),
            record_bytes,
        );
        tx.insert(&self.inner.spaces.meta, replay_key, replay_bytes);
        self.inner.commit(tx)?;
        Ok(result)
    }

    fn transform_record(&self, repo: &str, id: &[u8; 32]) -> Result<Record> {
        let bytes = self
            .inner
            .db
            .read_tx()
            .get(&self.inner.spaces.meta, record_key(repo, id))?
            .ok_or(Error::Invalid("unknown transform run"))?;
        bounded_decode(&bytes)
    }

    fn authorize_transform_destination(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        record: &Record,
    ) -> Result<()> {
        let view = self.forge_resolve_view(
            account,
            Some(actor),
            &record.input.destination_view_name,
            Some(crate::forge_accounts::ViewVersion(
                record.input.destination_view,
            )),
        )?;
        if let Some(run) = &record.run {
            if run
                .destination_paths
                .iter()
                .any(|path| !view.allows_path(path))
            {
                return Err(Error::ForgeAccessDenied);
            }
        }
        Ok(())
    }

    fn verify_transform_lineage(
        &self,
        request: &ForgeTransformRequest,
    ) -> Result<Vec<ForgeLineageEdge>> {
        let before = self
            .forge_view_files(&request.before, MAX_FILES)?
            .into_iter()
            .map(|f| (f.path.clone(), f))
            .collect::<BTreeMap<_, _>>();
        let after = self
            .forge_view_files(&request.after, MAX_FILES)?
            .into_iter()
            .map(|f| (f.path.clone(), f))
            .collect::<BTreeMap<_, _>>();
        let mut edges = Vec::new();
        let mut targets = BTreeSet::new();
        for hint in &request.lineage {
            path(&hint.source_path, false)?;
            path(&hint.target_path, false)?;
            if hint.source_path == hint.target_path || !targets.insert(&hint.target_path) {
                return Err(Error::Invalid("transform lineage endpoint"));
            }
            let source = before
                .get(&hint.source_path)
                .ok_or(Error::Invalid("transform lineage source"))?;
            let target = after
                .get(&hint.target_path)
                .ok_or(Error::Invalid("transform lineage target"))?;
            use tinytree_forge_core::transform_lineage::{
                self, Endpoint, Operation, RelationInput,
            };
            let operation = match hint.operation {
                ForgeLineageOperation::Rename => Operation::Rename,
                ForgeLineageOperation::Copy => Operation::Copy,
                ForgeLineageOperation::ImportedOrigin => {
                    return Err(Error::Invalid("import origin is service derived"));
                }
            };
            let target_copy = target
                .copy_id
                .ok_or(Error::Invalid("unverified transform copy target"))?;
            let (descriptor, payload) = self.read_metadata(
                request.after.repository(),
                ObjectKey {
                    kind: Kind::CopyHistory,
                    id: ObjectId(target_copy),
                },
            )?;
            transform_lineage::verify_relation_bytes(
                &RelationInput {
                    operation,
                    source_path: hint.source_path.clone(),
                    target_path: hint.target_path.clone(),
                    source: Endpoint {
                        canonical_path: source.canonical_path.clone(),
                        file: source.descriptor,
                        canonical_copy: source.copy_id.map(tinytree_model::CopyId),
                    },
                    target: Endpoint {
                        canonical_path: target.canonical_path.clone(),
                        file: target.descriptor,
                        canonical_copy: target.copy_id.map(tinytree_model::CopyId),
                    },
                    target_existed_before: before.contains_key(&hint.target_path),
                    source_exists_after: after.contains_key(&hint.source_path),
                },
                descriptor,
                &payload,
                self.inner.config.model_limits,
            )
            .map_err(|error| match error {
                transform_lineage::LineageError::Invalid(reason) => Error::Invalid(reason),
                transform_lineage::LineageError::Model(error) => Error::Model(error),
            })?;
            let mapped = request
                .spec
                .map_path(&hint.target_path)?
                .ok_or(Error::Invalid("unmapped transform lineage target"))?;
            edges.push(ForgeLineageEdge {
                operation: hint.operation.clone(),
                source_account: request.before.account().0.clone(),
                source_revision: request.before.revision().0,
                source_path: source.canonical_path.clone(),
                destination_path: join(request.destination.root_prefix(), &mapped),
                destination_revision: [0; 32],
            });
        }
        Ok(edges)
    }

    /// Called by Store reopen after account metadata validation. Pending intents
    /// promise no bytes; completed pins are rooted by ordinary native references
    /// and therefore participate in the existing authoritative GC walk.
    pub fn forge_validate_transforms_on_reopen(&self) -> Result<()> {
        let _guard = self.inner.lock()?;
        let view = self.inner.db.read_tx();
        let mut count = 0usize;
        let mut total_bytes = 0u64;
        let mut expected_refs = BTreeSet::new();
        for item in view.prefix(&self.inner.spaces.meta, RETIRE) {
            let (stored_key, bytes) = item.into_inner()?;
            count += 1;
            total_bytes = total_bytes
                .checked_add((stored_key.len() + bytes.len()) as u64)
                .ok_or(Error::Limit("forge transform reopen bytes"))?;
            if count > self.inner.config.max_retention_validation_records
                || total_bytes > self.inner.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("forge transform reopen records"));
            }
            let replay: RetireReplay = bounded_decode(&bytes)?;
            let account = AccountId(replay.account.clone());
            account.validate()?;
            request_token(&replay.request)?;
            let repo = account.repository_id();
            let record: Record = bounded_decode(
                &view
                    .get(&self.inner.spaces.meta, record_key(&repo, &replay.id))?
                    .ok_or(Error::Corrupt("transform retirement target missing"))?,
            )?;
            if stored_key.as_ref() != retire_key(&repo, &replay.principal, &replay.request)
                || replay.fingerprint
                    != transforms::retire_fingerprint(
                        &replay.account,
                        &replay.principal,
                        &replay.view,
                        replay.id,
                    )?
                || record.run.as_ref().map(|run| &run.pin) != Some(&replay.pin)
                || !replay.pin.retired
                || replay.pin.id != replay.id
            {
                return Err(Error::Corrupt("transform retirement identity"));
            }
        }
        for item in view.prefix(&self.inner.spaces.meta, PREFIX) {
            let (stored_key, bytes) = item.into_inner()?;
            count += 1;
            total_bytes = total_bytes
                .checked_add((stored_key.len() + bytes.len()) as u64)
                .ok_or(Error::Limit("forge transform reopen bytes"))?;
            if count > self.inner.config.max_retention_validation_records
                || total_bytes > self.inner.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("forge transform reopen records"));
            }
            let suffix = &stored_key[PREFIX.len()..];
            if let Some(delimiter) = suffix.windows(9).position(|s| s == b"/request/") {
                let repo = std::str::from_utf8(&suffix[..delimiter])
                    .map_err(|_| Error::Corrupt("transform request repository"))?;
                validate_repository_id(repo)?;
                let request = std::str::from_utf8(&suffix[delimiter + 9..])
                    .map_err(|_| Error::Corrupt("transform request token"))?;
                request_token(request)?;
                let id: [u8; 32] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Corrupt("transform request identity"))?;
                if view
                    .get(&self.inner.spaces.meta, record_key(repo, &id))?
                    .is_none()
                {
                    return Err(Error::Corrupt("transform request target missing"));
                }
                continue;
            }
            let record: Record = bounded_decode(&bytes)?;
            let id = transforms::run_id(&record.input)?;
            let account = AccountId(record.input.destination_account.clone());
            account.validate()?;
            let repo = account.repository_id();
            if self
                .inner
                .forge_account_for_repository_in(&view, &repo)?
                .as_ref()
                != Some(&account)
            {
                return Err(Error::Corrupt("transform account missing"));
            }
            if stored_key.as_ref() != record_key(&repo, &id) {
                return Err(Error::Corrupt("transform record identity"));
            }
            if let Some(run) = record.run {
                if run.id != id
                    || run.pin.id != id
                    || run.destination_account != account.0
                    || run.destination_base != record.input.destination_base
                    || run.destination_view != record.input.destination_view
                    || run.source_account != record.input.source_account
                    || run.source_before != record.input.before
                    || run.source_after != record.input.after
                    || run.source_before_view != record.input.before_view
                    || run.source_after_view != record.input.after_view
                    || run.spec != record.input.spec
                    || run.pin.reference != format!("_tinytree/forge/pins/v1/{}", hex(&id))
                    || run.pin.generation != if run.pin.retired { 2 } else { 1 }
                    || run.pin.protected_roots.len() > MAX_RUNS
                {
                    return Err(Error::Corrupt("dependency pin record"));
                }
                for (reference_name, expected) in [
                    (run.pin.reference, run.pin.artifact),
                    (
                        format!("_tinytree/forge/output/v1/{}", hex(&id)),
                        run.output_head,
                    ),
                ] {
                    expected_refs.insert(key(&repo, reference_name.as_bytes()));
                    let reference = reference(
                        reference_name.clone(),
                        view.get(
                            &self.inner.spaces.refs,
                            key(&repo, reference_name.as_bytes()),
                        )?
                        .as_deref(),
                    )?;
                    if reference.generation != run.pin.generation
                        || reference.commit.map(|d| d.key.id.0)
                            != (!run.pin.retired).then_some(expected)
                    {
                        return Err(Error::Corrupt("dependency pin reference"));
                    }
                }
            }
        }
        for item in view.prefix(&self.inner.spaces.refs, b"") {
            let (physical, bytes) = item.into_inner()?;
            count += 1;
            total_bytes = total_bytes
                .checked_add((physical.len() + bytes.len()) as u64)
                .ok_or(Error::Limit("forge transform reopen bytes"))?;
            if count > self.inner.config.max_retention_validation_records
                || total_bytes > self.inner.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("forge transform reopen roots"));
            }
            let delimiter = physical
                .iter()
                .position(|b| *b == 0)
                .ok_or(Error::Corrupt("reference repository key"))?;
            let suffix = &physical[delimiter + 1..];
            if (suffix.starts_with(b"_tinytree/forge/pins/v1/")
                || suffix.starts_with(b"_tinytree/forge/output/v1/"))
                && !expected_refs.contains(physical.as_ref())
            {
                return Err(Error::Corrupt("orphan transform root"));
            }
        }
        Ok(())
    }

    fn project_transform_input(
        &self,
        source: &ResolvedView,
        spec: &ForgeTransformSpec,
        repo: &str,
        now: i64,
        guards: &mut Vec<RootReadGuard>,
    ) -> Result<BTreeMap<String, ProjectedFile>> {
        let files = self.forge_view_files(source, MAX_FILES)?;
        let selected = files
            .into_iter()
            .map(|file| transforms::SelectedSourceFile {
                relative_path: file.path,
                canonical_path: file.canonical_path,
                descriptor: file.descriptor,
                executable: file.executable,
                copy_id: file.copy_id,
            })
            .collect::<Vec<_>>();
        let mut state = transforms::SelectionState::default();
        let mut result = BTreeMap::new();
        for page in selected.chunks(128) {
            let mapped = transforms::map_selection_page(spec, "", page, state)?;
            state = mapped.state;
            for file in mapped.files {
                let bytes =
                    self.forge_read_view_file(source, &file.source_relative_path, MAX_FILE_BYTES)?;
                let receipt = self.put(repo, file.descriptor, &bytes, now)?;
                guards.push(self.pin_root(repo, receipt.descriptor.key)?);
                if result
                    .insert(
                        file.mapped_path,
                        ProjectedFile {
                            id: FileId(file.descriptor.key.id.0),
                            size: file.descriptor.size_bytes,
                            executable: file.executable,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Invalid("duplicate mapped transform path"));
                }
            }
        }
        Ok(result)
    }

    fn read_commit(&self, repo: &str, id: CommitId) -> Result<Commit> {
        let (_, bytes) = self.read_metadata(
            repo,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(id.0),
            },
        )?;
        match tinytree_model::decode(&bytes, self.inner.config.model_limits)? {
            Object::Commit(commit) => Ok(commit),
            _ => Err(Error::Corrupt("transform commit kind")),
        }
    }

    #[cfg(test)]
    fn write_object(
        &self,
        repo: &str,
        object: Object,
        now: i64,
        guards: &mut Vec<RootReadGuard>,
    ) -> Result<Descriptor> {
        let object = tinytree_model::encode(&object, self.inner.config.model_limits)?;
        self.put(repo, object.descriptor, &object.bytes, now)?;
        guards.push(self.pin_root(repo, object.descriptor.key)?);
        Ok(object.descriptor)
    }

    fn flatten_tree(&self, repo: &str, root: TreeId) -> Result<BTreeMap<String, TreeValue>> {
        let mut pending = vec![(String::new(), root, 0)];
        let mut result = BTreeMap::new();
        let mut bytes = 0usize;
        let mut objects = 0usize;
        while let Some((prefix, id, depth)) = pending.pop() {
            if depth > 128 || objects >= MAX_FILES {
                return Err(Error::Limit("transform destination tree"));
            }
            objects += 1;
            let (_, payload) = self.read_metadata(
                repo,
                ObjectKey {
                    kind: Kind::Tree,
                    id: ObjectId(id.0),
                },
            )?;
            bytes = bytes
                .checked_add(payload.len())
                .ok_or(Error::Limit("transform destination metadata"))?;
            if bytes > MAX_BYTES as usize {
                return Err(Error::Limit("transform destination metadata"));
            }
            let Object::Tree(tree) =
                tinytree_model::decode(&payload, self.inner.config.model_limits)?
            else {
                return Err(Error::Corrupt("transform tree kind"));
            };
            for entry in tree.entries {
                let path = join(&prefix, &entry.name);
                match entry.value {
                    TreeValue::Tree(id) => pending.push((path, id, depth + 1)),
                    value => {
                        if result.len() >= MAX_FILES {
                            return Err(Error::Limit("transform destination files"));
                        }
                        result.insert(path, value);
                    }
                }
            }
        }
        Ok(result)
    }

    #[cfg(test)]
    fn write_tree(
        &self,
        repo: &str,
        files: &BTreeMap<String, TreeValue>,
        now: i64,
        guards: &mut Vec<RootReadGuard>,
    ) -> Result<TreeId> {
        let tree = transforms::build_tree(files, self.inner.config.model_limits)?;
        for object in tree.objects {
            self.put(repo, object.descriptor, &object.bytes, now)?;
            guards.push(self.pin_root(repo, object.descriptor.key)?);
        }
        Ok(tree.root)
    }
}

fn record_prefix(repo: &str) -> Vec<u8> {
    [PREFIX, repo.as_bytes(), b"/run/"].concat()
}
fn record_key(repo: &str, id: &[u8; 32]) -> Vec<u8> {
    [record_prefix(repo), hex(id).into_bytes()].concat()
}
fn request_prefix(repo: &str) -> Vec<u8> {
    [PREFIX, repo.as_bytes(), b"/request/"].concat()
}
fn request_key(repo: &str, id: &str) -> Vec<u8> {
    [request_prefix(repo), id.as_bytes().to_vec()].concat()
}
fn bounded_encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = encode(value)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Limit("transform record bytes"));
    }
    Ok(bytes)
}
fn bounded_decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Corrupt("transform record bytes"));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt("transform record encoding"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forge_accounts::{ForgeViewVisibility, ViewSpec};

    fn person() -> Signature {
        Signature {
            name: "Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        }
    }

    fn snapshot(
        store: &Store,
        account: &AccountId,
        values: &[(&str, &[u8])],
    ) -> (CommitId, Vec<Descriptor>) {
        let repo = account.repository_id();
        let mut guards = Vec::new();
        let mut descriptors = Vec::new();
        let mut files = BTreeMap::new();
        for (path, bytes) in values {
            let descriptor = tinytree_model::describe_file(bytes);
            store.put(&repo, descriptor, bytes, 0).unwrap();
            guards.push(store.pin_root(&repo, descriptor.key).unwrap());
            files.insert(
                (*path).into(),
                TreeValue::File {
                    id: FileId(descriptor.key.id.0),
                    executable: false,
                    size_bytes: descriptor.size_bytes,
                    copy_id: None,
                },
            );
            descriptors.push(descriptor);
        }
        let tree = store.write_tree(&repo, &files, 0, &mut guards).unwrap();
        let commit = Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![tree],
                labels: vec![String::new()],
            },
            change_id: ChangeId([9; 16]),
            description: "Fixture snapshot".into(),
            author: person(),
            committer: person(),
            secure_signature: None,
        };
        let descriptor = store
            .write_object(&repo, Object::Commit(commit), 0, &mut guards)
            .unwrap();
        (CommitId(descriptor.key.id.0), descriptors)
    }

    fn view(
        store: &Store,
        account: &AccountId,
        actor: &ForgeActor,
        revision: CommitId,
        root: &str,
    ) -> ResolvedView {
        let epoch = store.forge_get_account(account, actor).unwrap().epoch;
        store
            .forge_publish_view(
                account,
                actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: root.into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Public,
                    revision: revision.0,
                    export_allowed: true,
                },
                epoch,
                true,
            )
            .unwrap()
    }

    struct Fixture {
        directory: tempfile::TempDir,
        store: Store,
        source: AccountId,
        destination: AccountId,
        actor: ForgeActor,
        request: ForgeTransformRequest,
        secret: Descriptor,
        public: Descriptor,
    }
    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path(), Config::default()).unwrap();
        let source = AccountId("source".into());
        let destination = AccountId("destination".into());
        let actor = ForgeActor {
            principal: "fixture-owner".into(),
        };
        store.forge_provision_account(&source, &actor).unwrap();
        store.forge_provision_account(&destination, &actor).unwrap();
        let source_initial = store
            .references(&source.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap();
        let before = view(
            &store,
            &source,
            &actor,
            CommitId(source_initial.key.id.0),
            "public",
        );
        let (source_after, source_files) = snapshot(
            &store,
            &source,
            &[
                ("public/README.md", b"public body\n"),
                ("private/secret.txt", b"private body\n"),
            ],
        );
        let after = view(&store, &source, &actor, source_after, "public");
        let before = store
            .forge_resolve_view(&source, Some(&actor), "workspace", Some(before.version()))
            .unwrap();
        let dest_head = store
            .references(&destination.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap();
        let destination_view = view(
            &store,
            &destination,
            &actor,
            CommitId(dest_head.key.id.0),
            "",
        );
        let request = ForgeTransformRequest {
            request_id: "import-fixture".into(),
            before,
            after,
            destination: destination_view,
            spec: ForgeTransformSpec {
                mappings: vec![ForgePathMapping {
                    source_prefix: "".into(),
                    destination_prefix: "vendor".into(),
                }],
            },
            change_id: ChangeId([42; 16]),
            lineage: vec![],
        };
        Fixture {
            directory,
            store,
            source,
            destination,
            actor,
            request,
            secret: source_files[1],
            public: source_files[0],
        }
    }

    #[test]
    fn deep_shared_mapping_is_bounded_before_tree_construction() {
        let mut fixture = fixture();
        let paths = (0..500)
            .map(|n| format!("public/file-{n:03}.txt"))
            .collect::<Vec<_>>();
        let files = paths
            .iter()
            .map(|path| (path.as_str(), b"".as_slice()))
            .collect::<Vec<_>>();
        let (head, _) = snapshot(&fixture.store, &fixture.source, &files);
        fixture.request.after = view(
            &fixture.store,
            &fixture.source,
            &fixture.actor,
            head,
            "public",
        );
        fixture.request.before = fixture
            .store
            .forge_resolve_view(
                &fixture.source,
                Some(&fixture.actor),
                "workspace",
                Some(fixture.request.before.version()),
            )
            .unwrap();
        let prefix = vec!["a".repeat(60); 63].join("/");
        fixture.request.spec.mappings[0].destination_prefix = prefix.clone();
        // Every individual mapping is valid. The shared prefix used to retain
        // a fresh suffix String at each ancestor for every shallow source file.
        fixture.request.spec.validate().unwrap();
        assert!(
            fixture
                .request
                .spec
                .map_path("file-000.txt")
                .unwrap()
                .unwrap()
                .len()
                < 4096
        );
        let before = fixture
            .store
            .references(&fixture.destination.repository_id(), &["main".into()])
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 0),
            Err(Error::Limit("transform tree path bytes"))
        ));
        assert_eq!(
            fixture
                .store
                .references(&fixture.destination.repository_id(), &["main".into()])
                .unwrap(),
            before
        );
        // Retry remains a bounded failure, never an acknowledged retained pin.
        assert!(matches!(
            fixture
                .store
                .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 1),
            Err(Error::Limit("transform tree path bytes"))
        ));
        fixture.store.forge_validate_transforms_on_reopen().unwrap();
        // The same actual 500-file public snapshot is supported under an
        // admitted mapping, rather than hidden by a smaller test-only file cap.
        fixture.request.request_id = "admitted-500-file-map".into();
        fixture.request.spec.mappings[0].destination_prefix = "vendor".into();
        let run = fixture
            .store
            .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 2)
            .unwrap();
        assert_eq!(run.destination_paths.len(), 500);
        let page = fixture
            .store
            .forge_pin_files(&fixture.destination, &fixture.actor, run.id, None, 256)
            .unwrap();
        assert_eq!(page.files.len(), 256);
        assert!(page.next_path.is_some());
        assert_eq!(
            fixture
                .store
                .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 3)
                .unwrap(),
            run
        );
        // An admitted deep mapping traverses the same component trie and
        // emits the exact sorted native tree without suffix-map copies.
        let deep = vec!["a".repeat(30); 63].join("/");
        fixture.request.request_id = "admitted-deep-single-file".into();
        fixture.request.spec.mappings[0] = ForgePathMapping {
            source_prefix: "file-000.txt".into(),
            destination_prefix: deep.clone(),
        };
        let run = fixture
            .store
            .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 4)
            .unwrap();
        assert_eq!(run.destination_paths, vec![deep.clone()]);
        let retained = fixture
            .store
            .forge_pin_files(&fixture.destination, &fixture.actor, run.id, None, 1)
            .unwrap();
        assert_eq!(retained.files.len(), 1);
        assert_eq!(retained.files[0].path, deep);
        assert_eq!(retained.files[0].size_bytes, 0);
    }

    #[test]
    fn canonical_transform_depth_is_admitted_before_any_intent_or_byte_copy() {
        let mut fixture = fixture();
        let destination_base = fixture.request.destination.revision();
        fixture.request.destination = view(
            &fixture.store,
            &fixture.destination,
            &fixture.actor,
            destination_base,
            "focus/inner",
        );
        fixture.request.spec.mappings[0].source_prefix = "README.md".into();
        let repo = fixture.destination.repository_id();
        let before_main = fixture.store.references(&repo, &["main".into()]).unwrap();
        let before_objects = [Kind::File, Kind::Tree, Kind::Commit]
            .map(|kind| fixture.store.list_objects(&repo, kind, None, 1024).unwrap());
        // A tiny 80-component mapping and a supported relative path whose root
        // makes it too deep must both fail before reserving this request ID.
        for components in [80, 63] {
            fixture.request.spec.mappings[0].destination_prefix =
                vec!["part"; components].join("/");
            for now in [0, 1] {
                assert!(matches!(
                    fixture.store.forge_prepare_transform(
                        &fixture.actor,
                        &fixture.request,
                        &person(),
                        now
                    ),
                    Err(Error::Limit("transform tree depth"))
                ));
                assert_eq!(
                    fixture.store.references(&repo, &["main".into()]).unwrap(),
                    before_main
                );
                assert_eq!(
                    [Kind::File, Kind::Tree, Kind::Commit]
                        .map(|kind| fixture.store.list_objects(&repo, kind, None, 1024).unwrap()),
                    before_objects
                );
                assert!(
                    fixture
                        .store
                        .descriptor(&repo, fixture.public.key)
                        .unwrap()
                        .is_none()
                );
            }
        }
        // Correcting the same request ID succeeds: neither rejected mapping
        // persisted an intent. Two view-root components plus 62 mapped ones is
        // the supported 64-component canonical boundary, not an off-by-one.
        let mapped = vec!["part"; 62].join("/");
        fixture.request.spec.mappings[0].destination_prefix = mapped.clone();
        let run = fixture
            .store
            .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 2)
            .unwrap();
        assert_eq!(run.destination_paths, vec![format!("focus/inner/{mapped}")]);
        let output = view(
            &fixture.store,
            &fixture.destination,
            &fixture.actor,
            CommitId(run.output_head),
            "focus/inner",
        );
        assert_eq!(
            fixture
                .store
                .forge_read_view_file(&output, &mapped, 1024)
                .unwrap(),
            b"public body\n"
        );
        let projected = fixture.store.forge_project_view(&output, 3).unwrap();
        assert_eq!(
            fixture
                .store
                .forge_read_projected_object(&output, fixture.public.key, 3, 1024)
                .unwrap(),
            b"public body\n"
        );
        assert_ne!(projected.projected_commit, [0; 32]);
        assert_eq!(
            fixture.store.references(&repo, &["main".into()]).unwrap(),
            before_main
        );
    }

    #[test]
    fn retained_transform_catalog_above_legacy_record_size_replays_and_reopens() {
        let mut fixture = fixture();
        let paths = (0..80)
            .map(|n| format!("public/source-{n:03}.txt"))
            .collect::<Vec<_>>();
        let files = paths
            .iter()
            .map(|path| (path.as_str(), b"retained source\n".as_slice()))
            .collect::<Vec<_>>();
        let (head, _) = snapshot(&fixture.store, &fixture.source, &files);
        fixture.request.after = view(
            &fixture.store,
            &fixture.source,
            &fixture.actor,
            head,
            "public",
        );
        fixture.request.before = fixture
            .store
            .forge_resolve_view(
                &fixture.source,
                Some(&fixture.actor),
                "workspace",
                Some(fixture.request.before.version()),
            )
            .unwrap();
        fixture.request.request_id = "large-retained-catalog".into();
        let run = fixture
            .store
            .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 0)
            .unwrap();
        assert!(
            serde_json::to_vec(&run).unwrap().len() > 16 * 1024,
            "exercise an admitted catalog above the unrelated legacy metadata bound"
        );
        assert_eq!(run.destination_paths.len(), paths.len());
        assert_eq!(
            fixture
                .store
                .forge_prepare_transform(&fixture.actor, &fixture.request, &person(), 1)
                .unwrap(),
            run
        );
        let Fixture {
            directory,
            store,
            destination,
            actor,
            ..
        } = fixture;
        store.gc(&destination.repository_id(), 10000).unwrap();
        drop(store);
        let store = Store::open_existing(directory.path(), Config::default()).unwrap();
        let restored = store
            .forge_transform_run(&destination, &actor, run.id)
            .unwrap();
        assert_eq!(restored.id, run.id);
        assert_eq!(restored.output_head, run.output_head);
        let retained = store
            .forge_resolve_pin(&destination, &actor, run.id)
            .unwrap();
        assert_eq!(retained.descriptor().key.id.0, run.pin.artifact);
    }

    #[test]
    fn filtered_transform_is_durable_idempotent_and_does_not_publish_main() {
        let Fixture {
            directory,
            store,
            source,
            destination,
            actor,
            request,
            secret,
            public,
        } = fixture();
        let main_before = store
            .references(&destination.repository_id(), &["main".into()])
            .unwrap()[0]
            .clone();
        let run = store
            .forge_prepare_transform(&actor, &request, &person(), 0)
            .unwrap();
        assert_eq!(
            store
                .forge_prepare_transform(&actor, &request, &person(), 50)
                .unwrap(),
            run
        );
        assert_eq!(
            store
                .references(&destination.repository_id(), &["main".into()])
                .unwrap()[0],
            main_before
        );
        assert!(
            store
                .descriptor(&destination.repository_id(), secret.key)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .descriptor(&destination.repository_id(), public.key)
                .unwrap(),
            Some(public)
        );
        assert!(
            store
                .descriptor(
                    &destination.repository_id(),
                    ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId(request.after.revision().0)
                    }
                )
                .unwrap()
                .is_none(),
            "source ancestry must not be imported"
        );
        store.gc(&source.repository_id(), 10000).unwrap();
        store.gc(&destination.repository_id(), 10000).unwrap();
        drop(store);
        let store = Store::open_existing(directory.path(), Config::default()).unwrap();
        store.forge_validate_transforms_on_reopen().unwrap();
        let artifact = store
            .forge_resolve_pin(&destination, &actor, run.id)
            .unwrap();
        let artifact_commit = store
            .read_commit(
                &destination.repository_id(),
                CommitId(artifact.descriptor().key.id.0),
            )
            .unwrap();
        assert_eq!(artifact_commit.parents, vec![CommitId::ROOT]);
        let files = store
            .flatten_tree(
                &destination.repository_id(),
                artifact_commit.root_tree.terms[0],
            )
            .unwrap();
        assert_eq!(
            files.keys().cloned().collect::<Vec<_>>(),
            vec!["after/vendor/README.md"]
        );
    }

    #[test]
    fn destination_retention_survives_source_retirement_and_gc_until_explicit_release() {
        let Fixture {
            directory: _directory,
            store,
            source,
            destination,
            actor,
            request,
            public,
            ..
        } = fixture();
        let run = store
            .forge_prepare_transform(&actor, &request, &person(), 0)
            .unwrap();
        let old = request.after.version();
        let empty = store
            .references(&source.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap();
        view(&store, &source, &actor, CommitId(empty.key.id.0), "public");
        let epoch = store.forge_get_account(&source, &actor).unwrap().epoch;
        store
            .forge_retire_view(&source, &actor, old, epoch)
            .unwrap();
        store.gc(&source.repository_id(), 10000).unwrap();
        assert!(
            store
                .descriptor(&source.repository_id(), public.key)
                .unwrap()
                .is_none()
        );
        store
            .forge_resolve_pin(&destination, &actor, run.id)
            .unwrap();
        let files = store
            .forge_pin_files(&destination, &actor, run.id, None, 10)
            .unwrap();
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.files[0].path, "vendor/README.md");
        let first = store
            .forge_read_pin_file(&destination, &actor, run.id, "vendor/README.md", 0, 4)
            .unwrap();
        let rest = store
            .forge_read_pin_file(&destination, &actor, run.id, "vendor/README.md", 4, 64)
            .unwrap();
        assert_eq!([first.bytes, rest.bytes].concat(), b"public body\n");
        assert!(rest.eof);
        assert!(
            store
                .forge_transform_run(&destination, &actor, run.id)
                .unwrap()
                .origin
                .is_none()
        );
        assert!(
            store
                .forge_retire_pin(
                    &destination,
                    &actor,
                    request.destination.name(),
                    run.id,
                    "retire-fixture"
                )
                .is_err()
        );
        store
            .forge_protect_pin(
                &destination,
                &actor,
                run.id,
                &format!("transform-{}", hex(&run.id)),
                false,
            )
            .unwrap();
        store
            .forge_retire_pin(
                &destination,
                &actor,
                request.destination.name(),
                run.id,
                "retire-fixture",
            )
            .unwrap();
        let retired = store
            .forge_retire_pin(
                &destination,
                &actor,
                request.destination.name(),
                run.id,
                "retire-fixture",
            )
            .unwrap();
        assert_eq!(retired.generation, 2);
        assert!(retired.retired);
        assert!(matches!(
            store.forge_retire_pin(
                &destination,
                &actor,
                request.destination.name(),
                [9; 32],
                "retire-fixture"
            ),
            Err(Error::IdempotencyConflict)
        ));
        assert!(matches!(
            store.forge_retire_pin(&destination, &actor, "other", run.id, "retire-fixture"),
            Err(Error::IdempotencyConflict)
        ));
        store.gc(&destination.repository_id(), 10000).unwrap();
        assert!(matches!(
            store.forge_resolve_pin(&destination, &actor, run.id),
            Err(Error::Invalid("dependency pin retired"))
        ));
        assert!(
            store
                .descriptor(&destination.repository_id(), public.key)
                .unwrap()
                .is_none()
        );
        store.forge_validate_transforms_on_reopen().unwrap();
        drop(store);
        let reopened = Store::open_existing(_directory.path(), Config::default()).unwrap();
        let replay = reopened
            .forge_retire_pin(
                &destination,
                &actor,
                request.destination.name(),
                run.id,
                "retire-fixture",
            )
            .unwrap();
        assert_eq!(
            replay, retired,
            "retirement replay owns no collected payload and must remain exact after restart"
        );
    }

    #[test]
    fn transform_mapping_rejects_ambiguous_paths_and_request_rebinding() {
        let invalid = ForgeTransformSpec {
            mappings: vec![
                ForgePathMapping {
                    source_prefix: "".into(),
                    destination_prefix: "root".into(),
                },
                ForgePathMapping {
                    source_prefix: "nested".into(),
                    destination_prefix: "other".into(),
                },
            ],
        };
        assert!(invalid.validate().is_err());
        let Fixture {
            directory: _directory,
            store,
            actor,
            mut request,
            ..
        } = fixture();
        store
            .forge_prepare_transform(&actor, &request, &person(), 0)
            .unwrap();
        request.change_id = ChangeId([43; 16]);
        assert!(matches!(
            store.forge_prepare_transform(&actor, &request, &person(), 0),
            Err(Error::IdempotencyConflict)
        ));
        request.spec.mappings[0].destination_prefix = "../outside".into();
        assert!(request.spec.validate().is_err());
    }

    #[test]
    fn explicit_rename_and_copy_use_native_proof_without_importing_private_ancestry() {
        for operation in [ForgeLineageOperation::Rename, ForgeLineageOperation::Copy] {
            let Fixture {
                directory: _directory,
                store,
                source,
                destination,
                actor,
                mut request,
                ..
            } = fixture();
            let source_repo = source.repository_id();
            let mut guards = Vec::new();
            let bytes = b"lineage content";
            let file = tinytree_model::describe_file(bytes);
            store.put(&source_repo, file, bytes, 0).unwrap();
            guards.push(store.pin_root(&source_repo, file.key).unwrap());
            let source_copy = store
                .write_object(
                    &source_repo,
                    Object::CopyHistory(tinytree_model::CopyHistory {
                        current_path: "public/old.txt".into(),
                        parents: vec![],
                        salt: vec![1],
                    }),
                    0,
                    &mut guards,
                )
                .unwrap();
            let target_copy = store
                .write_object(
                    &source_repo,
                    Object::CopyHistory(tinytree_model::CopyHistory {
                        current_path: "public/new.txt".into(),
                        parents: vec![tinytree_model::CopyId(source_copy.key.id.0)],
                        salt: vec![2],
                    }),
                    0,
                    &mut guards,
                )
                .unwrap();
            let value = |copy| TreeValue::File {
                id: FileId(file.key.id.0),
                executable: false,
                size_bytes: file.size_bytes,
                copy_id: Some(tinytree_model::CopyId(copy)),
            };
            let before_files =
                BTreeMap::from([("public/old.txt".into(), value(source_copy.key.id.0))]);
            let before_tree = store
                .write_tree(&source_repo, &before_files, 0, &mut guards)
                .unwrap();
            let mut commit = Commit {
                parents: vec![CommitId::ROOT],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![before_tree],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([11; 16]),
                description: "Lineage fixture".into(),
                author: person(),
                committer: person(),
                secure_signature: None,
            };
            let before = store
                .write_object(&source_repo, Object::Commit(commit.clone()), 0, &mut guards)
                .unwrap();
            let before_view = view(&store, &source, &actor, CommitId(before.key.id.0), "public");
            let mut after_files =
                BTreeMap::from([("public/new.txt".into(), value(target_copy.key.id.0))]);
            if operation == ForgeLineageOperation::Copy {
                after_files.extend(before_files);
            }
            let after_tree = store
                .write_tree(&source_repo, &after_files, 0, &mut guards)
                .unwrap();
            commit.parents = vec![CommitId(before.key.id.0)];
            commit.root_tree.terms = vec![after_tree];
            let after = store
                .write_object(&source_repo, Object::Commit(commit), 0, &mut guards)
                .unwrap();
            request.after = view(&store, &source, &actor, CommitId(after.key.id.0), "public");
            request.before = store
                .forge_resolve_view(
                    &source,
                    Some(&actor),
                    "workspace",
                    Some(before_view.version()),
                )
                .unwrap();
            let (dest, _) = snapshot(
                &store,
                &destination,
                &[("vendor/old.txt", bytes), ("local/keep.txt", b"local")],
            );
            request.destination = view(&store, &destination, &actor, dest, "");
            request.lineage = vec![ForgeLineageHint {
                operation: operation.clone(),
                source_path: "old.txt".into(),
                target_path: "new.txt".into(),
            }];
            let run = store
                .forge_prepare_transform(&actor, &request, &person(), 0)
                .unwrap();
            let edge = run
                .lineage
                .iter()
                .find(|edge| edge.operation == operation)
                .unwrap();
            assert_eq!(edge.source_path, "public/old.txt");
            assert_eq!(edge.destination_path, "vendor/new.txt");
            assert_eq!(edge.destination_revision, run.output_head);
            let output = store
                .read_commit(&destination.repository_id(), CommitId(run.output_head))
                .unwrap();
            let files = store
                .flatten_tree(&destination.repository_id(), output.root_tree.terms[0])
                .unwrap();
            assert!(files.contains_key("local/keep.txt"));
            assert!(files.contains_key("vendor/new.txt"));
            assert_eq!(
                files.contains_key("vendor/old.txt"),
                operation == ForgeLineageOperation::Copy
            );
            assert!(
                store
                    .descriptor(&destination.repository_id(), source_copy.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .descriptor(&destination.repository_id(), target_copy.key)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn retained_pin_obeys_current_destination_path_policy() {
        let Fixture {
            directory: _directory,
            store,
            destination,
            actor,
            request,
            ..
        } = fixture();
        let run = store
            .forge_prepare_transform(&actor, &request, &person(), 0)
            .unwrap();
        let epoch = store.forge_get_account(&destination, &actor).unwrap().epoch;
        store
            .forge_publish_view(
                &destination,
                &actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec!["vendor".into()],
                    visibility: ForgeViewVisibility::Public,
                    revision: request.destination.revision().0,
                    export_allowed: true,
                },
                epoch,
                true,
            )
            .unwrap();
        assert!(matches!(
            store.forge_resolve_pin(&destination, &actor, run.id),
            Err(Error::ForgeAccessDenied)
        ));
        assert!(matches!(
            store.forge_pin_files(&destination, &actor, run.id, None, 10),
            Err(Error::ForgeAccessDenied)
        ));
        assert!(matches!(
            store.forge_transform_run(&destination, &actor, run.id),
            Err(Error::ForgeAccessDenied)
        ));
    }

    #[test]
    fn transform_output_enters_real_review_and_rejects_forged_provenance() {
        let Fixture {
            directory: _directory,
            store,
            destination,
            actor,
            request,
            ..
        } = fixture();
        let run = store
            .forge_prepare_transform(&actor, &request, &person(), 0)
            .unwrap();
        let mut push = crate::ForgeReviewPushRequest {
            request_id: format!("transform-{}", hex(&run.id)),
            source_base: CommitId(run.destination_base),
            source_head: CommitId(run.output_head),
            view_version: run.destination_view,
            expected_patchset: None,
            predecessor: None,
            provenance: Some(run.id),
            lineage: vec![],
            check_profile: LandingCheckProfile {
                name: "checks".into(),
                version: "v1".into(),
            },
        };
        let review = store
            .forge_push_review(&destination, &actor, &push, &person(), 0)
            .unwrap();
        assert_eq!(review.change_id, request.change_id.0);
        assert_eq!(review.provenance, Some(run.id));
        let replay = store
            .forge_push_review(&destination, &actor, &push, &person(), 50)
            .unwrap();
        assert_eq!(review.patchset_id, replay.patchset_id);
        let main = store
            .references(&destination.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap();
        assert_eq!(main.key.id.0, run.destination_base);
        push.request_id = "forged-origin".into();
        push.provenance = Some([231; 32]);
        assert!(matches!(
            store.forge_push_review(&destination, &actor, &push, &person(), 0),
            Err(Error::ForgeAccessDenied)
        ));
    }
}
