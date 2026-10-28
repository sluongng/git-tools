//! Finite discovery windows with explicitly protected physical exceptions.
use super::*;

pub(super) struct VersionProtection {
    readers: bool,
    pins: Vec<CloudVersionSelector>,
}
impl VersionProtection {
    pub(super) fn held(&self) -> bool {
        self.readers || !self.pins.is_empty()
    }
    pub(super) fn verify(&self, record: &VersionRecord, selected: BlobDigest) -> Result<()> {
        if self.pins.iter().any(|pin| {
            pin.workspace != record.workspace
                || pin.generation != record.generation
                || pin.manifest.0 != record.manifest
                || pin.selected != selected
        }) {
            return Err(Error::Corrupt("cloud protected version selection"));
        }
        Ok(())
    }
}

pub(super) struct Cleanup<'a> {
    pub(super) changes: Vec<Change<'a>>,
    pub(super) delta: Delta,
}

impl Inner {
    pub(super) fn version_protection(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        gate: &Runtime,
        scan: &mut Scan,
    ) -> Result<VersionProtection> {
        Ok(VersionProtection {
            readers: gate
                .cloud_version_readers
                .get(&(repo.to_owned(), owner.principal, workspace, generation))
                .is_some_and(|count| *count > 0),
            pins: self
                .active_version_pin_selectors(repo, owner, workspace, scan)?
                .into_iter()
                .filter(|pin| pin.generation == generation)
                .collect(),
        })
    }

    fn logical_cloud_heads(
        &self,
        repo: &str,
        owner: CloudOwner,
        plan: &RetainedPlan,
        scan: &mut Scan,
        incoming: Option<(u64, &RecoveryManifest)>,
    ) -> Result<HashMap<[u8; 32], u64>> {
        let prefix = scope(repo, owner);
        let limits = self.v3_limits(
            self.config.cloud.manifest_limits,
            self.config.cloud.max_version_decode_bytes,
        )?;
        let view = self.db.read_tx();
        let mut ordered = plan
            .versions
            .iter()
            .filter(|(key, record)| key.starts_with(&prefix) && record.format == 3)
            .collect::<Vec<_>>();
        ordered.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut heads = HashMap::new();
        let mut workspace = None;
        let mut head = latest::LogicalCaptureHead::default();
        let mut incoming_seen = false;
        // Keep one decoded winner, not one potentially large model per workspace.
        let mut finish = |workspace, mut head: latest::LogicalCaptureHead| -> Result<()> {
            if let Some((generation, RecoveryManifest::V3(candidate))) = incoming {
                if candidate.history.recovery.workspace == workspace {
                    head.consider(generation, candidate.clone(), limits)?;
                    incoming_seen = true;
                }
            }
            let generation = head
                .finish()?
                .ok_or(Error::Corrupt("cloud logical head missing"))?;
            heads.insert(workspace, generation);
            Ok(())
        };
        for (_, record) in ordered {
            if workspace != Some(record.workspace) {
                if let Some(previous) = workspace {
                    finish(previous, std::mem::take(&mut head))?;
                }
                workspace = Some(record.workspace);
            }
            let id = ManifestId(record.manifest);
            let key = manifest_key(repo, owner, id);
            let bytes = view
                .get(&self.spaces.cloud.manifests, &key)?
                .ok_or(Error::Corrupt("cloud logical head manifest missing"))?;
            scan.visit(self, &key, &bytes)?;
            let RecoveryManifest::V3(candidate) =
                self.decode_recovery(&bytes, id, self.config.cloud.manifest_limits)?
            else {
                return Err(Error::Corrupt("cloud logical head format"));
            };
            head.consider(record.generation, candidate, limits)?;
        }
        if let Some(workspace) = workspace {
            finish(workspace, head)?;
        }
        drop(finish);
        if !incoming_seen {
            if let Some((generation, RecoveryManifest::V3(candidate))) = incoming {
                heads.insert(candidate.history.recovery.workspace, generation);
            }
        }
        Ok(heads)
    }

    /// Plan first, including every receipt binding; no writes can hide malformed
    /// packages. The caller commits cleanup with its publication or GC prelude.
    fn exception_cleanup<'a>(
        &'a self,
        repo: &str,
        owner: CloudOwner,
        plan: &RetainedPlan,
        gate: &Runtime,
        scan: &mut Scan,
        advance: Option<([u8; 32], u64)>,
        incoming: Option<(ManifestId, u64, &RecoveryManifest)>,
    ) -> Result<Cleanup<'a>> {
        // One logical winner may outlive the discovery window. Ambiguous
        // histories cannot be safely reduced to a singleton: rolling admission
        // fails closed rather than silently discarding conflicting evidence.
        let heads = self.logical_cloud_heads(
            repo,
            owner,
            plan,
            scan,
            incoming.map(|(_, generation, manifest)| (generation, manifest)),
        )?;
        let prefix = scope(repo, owner);
        let mut retiring = HashSet::new();
        let mut retiring_ids = HashSet::new();
        let mut kept_ids = HashSet::new();
        if let Some((id, _, _)) = incoming {
            kept_ids.insert(id.0);
        }
        let mut floors = HashMap::new();
        for (key, record) in &plan.versions {
            if !key.starts_with(&prefix) {
                continue;
            }
            let floor = if let Some(floor) = floors.get(&record.workspace) {
                *floor
            } else {
                let floor = advance
                    .filter(|(workspace, _)| *workspace == record.workspace)
                    .map(|(_, floor)| floor)
                    .unwrap_or(
                        self.workspace_lifecycle(repo, owner, record.workspace)?
                            .retired_through,
                    );
                floors.insert(record.workspace, floor);
                floor
            };
            let protected = if record.generation <= floor {
                let protection = self.version_protection(
                    repo,
                    owner,
                    record.workspace,
                    record.generation,
                    gate,
                    scan,
                )?;
                if protection.held() {
                    let descriptor = *plan
                        .manifest_descriptors
                        .get(&manifest_key(repo, owner, ManifestId(record.manifest)))
                        .ok_or(Error::Corrupt("cloud protected manifest missing"))?;
                    protection.verify(record, descriptor)?;
                }
                protection.held() || heads.get(&record.workspace) == Some(&record.generation)
            } else {
                true
            };
            if protected {
                kept_ids.insert(record.manifest);
            } else {
                retiring.insert(key.clone());
                retiring_ids.insert(record.manifest);
            }
        }
        let mut cleanup = Cleanup {
            changes: Vec::new(),
            delta: Delta::default(),
        };
        for key in &retiring {
            cleanup
                .changes
                .push((&self.spaces.cloud.versions, key.clone(), None));
            cleanup
                .changes
                .push((&self.spaces.cloud.roots, key.clone(), None));
            cleanup
                .changes
                .push((&self.spaces.cloud.captures, key.clone(), None));
            cleanup.delta.versions -= 1;
            cleanup.delta.version_payload_bytes -= *plan
                .payload_work
                .get(key)
                .ok_or(Error::Corrupt("cloud rolling payload accounting"))?
                as i128;
            cleanup.delta.version_metadata_bytes -= *plan
                .metadata_work
                .get(key)
                .ok_or(Error::Corrupt("cloud rolling metadata accounting"))?
                as i128;
            cleanup.delta.version_records -= *plan
                .record_work
                .get(key)
                .ok_or(Error::Corrupt("cloud rolling record accounting"))?
                as i128;
        }
        let view = self.db.read_tx();
        let mut receipts = HashSet::new();
        for item in view.prefix(&self.spaces.cloud.receipts, &prefix) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (_, _, suffix) = parse_scope(&key)?;
            let target = self.retained_receipt_target(repo, owner, suffix, &bytes, plan)?;
            if !receipts.insert(target.clone()) {
                return Err(Error::Corrupt("cloud receipt replay history"));
            }
            if retiring.contains(&target) {
                cleanup
                    .changes
                    .push((&self.spaces.cloud.receipts, key.to_vec(), None));
                cleanup.delta.receipts -= 1;
            }
        }
        if receipts.len()
            != plan
                .versions
                .keys()
                .filter(|key| key.starts_with(&prefix))
                .count()
            || cleanup.delta.receipts != cleanup.delta.versions
        {
            return Err(Error::Corrupt("cloud version receipt missing"));
        }
        for id in retiring_ids.difference(&kept_ids) {
            cleanup.changes.push((
                &self.spaces.cloud.manifests,
                manifest_key(repo, owner, ManifestId(*id)),
                None,
            ));
        }
        Ok(cleanup)
    }

    pub(super) fn rolling_publication<'a>(
        &'a self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        generation: u64,
        incoming: ManifestId,
        incoming_manifest: &RecoveryManifest,
        state: &mut WorkspaceRecord,
        gate: &Runtime,
    ) -> Result<Cleanup<'a>> {
        let floor = state
            .retired_through
            .max(generation.saturating_sub(self.config.cloud.max_versions_per_workspace));
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let plan = self.retained_cloud_plan(Some(repo), Some(owner), true, &mut scan)?;
        let mut cleanup = self.exception_cleanup(
            repo,
            owner,
            &plan,
            gate,
            &mut scan,
            Some((workspace, floor)),
            Some((incoming, generation, incoming_manifest)),
        )?;
        if floor > state.retired_through {
            let boundary = plan
                .versions
                .get(&version_key(repo, owner, workspace, floor))
                .ok_or(Error::Corrupt("cloud rolling boundary missing"))?
                .manifest;
            state.retired_through = floor;
            state.boundary_manifest = Some(boundary);
            state.retention_revision = state
                .retention_revision
                .checked_add(1)
                .ok_or(Error::Limit("cloud retention revision"))?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"tinytree-cloud-rolling-retention-v1\0");
            hash.update(&workspace);
            hash.update(&generation.to_be_bytes());
            hash.update(&incoming.0);
            state.last_retirement = Some(*hash.finalize().as_bytes());
            let (key, value) = retirement::replay_close_record(repo, owner)?;
            cleanup
                .changes
                .push((&self.spaces.cloud.lifecycle, key, Some(value)));
        }
        Ok(cleanup)
    }

    /// The caller owns the Store gate. Never run from a guard destructor.
    pub(crate) fn reap_cloud_version_exceptions(
        &self,
        repo: Option<&str>,
        gate: &Runtime,
    ) -> Result<()> {
        let view = self.db.read_tx();
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let prefix = repo.map(repo_prefix).unwrap_or_default();
        let mut owners = HashSet::new();
        for item in view.prefix(&self.spaces.cloud.workspaces, prefix) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (actual, owner, suffix) = parse_scope(&key)?;
            if suffix.len() != 32 {
                return Err(Error::Corrupt("cloud workspace key"));
            }
            let state: WorkspaceRecord = decode(&bytes)?;
            state.validate_lifecycle()?;
            if state.retired_through == 0 {
                continue;
            }
            let workspace: [u8; 32] = suffix.try_into().unwrap();
            if view
                .range(
                    &self.spaces.cloud.versions,
                    version_key(actual, owner, workspace, 1)
                        ..=version_key(actual, owner, workspace, state.retired_through),
                )
                .next()
                .map(|item| item.into_inner())
                .transpose()?
                .is_some()
            {
                owners.insert((actual.to_owned(), owner.principal));
            }
        }
        if owners.is_empty() {
            return Ok(());
        }
        let plan = self.retained_cloud_plan(repo, None, true, &mut scan)?;
        let mut cleanups = Vec::new();
        for (actual, principal) in owners {
            let owner = CloudOwner { principal };
            let cleanup =
                self.exception_cleanup(&actual, owner, &plan, gate, &mut scan, None, None)?;
            if !cleanup.changes.is_empty() {
                cleanups.push((actual, owner, cleanup));
            }
        }
        if cleanups.is_empty() {
            return Ok(());
        }
        let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
        for (actual, owner, cleanup) in cleanups {
            self.cloud_changes(&mut tx, &actual, owner, cleanup.changes, cleanup.delta)?;
        }
        self.commit(tx)
    }
}
