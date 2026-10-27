//! Logical capture selection is independent of publication/CAS order.
use super::*;
use std::{ops::Bound, time::Instant};

fn within(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Limit("latest capture deadline"))
    } else {
        Ok(())
    }
}

impl Store {
    /// Selects the greatest capture sequence in one authenticated workspace.
    /// Mixed exporters or conflicting greatest sequences require an explicit
    /// generation. Publication head and idempotency receipts are unchanged.
    pub fn read_latest_captured_cloud_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        limits: ManifestLimits,
        deadline: Instant,
    ) -> Result<Option<CloudVersionReadGuard>> {
        within(deadline)?;
        let generation = {
            let _gate = self.inner.lock()?;
            within(deadline)?;
            self.inner.repository(repo)?;
            let view = self.inner.db.read_tx();
            let mut scan = Scan {
                records: 0,
                bytes: 0,
                payload: 0,
            };
            let key = workspace_key(repo, owner, workspace);
            let Some(bytes) = view.get(&self.inner.spaces.cloud.workspaces, &key)? else {
                return Ok(None);
            };
            scan.visit(&self.inner, &key, &bytes)?;
            if bytes.len() > 1024 {
                return Err(Error::Corrupt("cloud workspace row"));
            }
            let current: WorkspaceRecord = decode(&bytes)?;
            if current.generation == 0 {
                return Err(Error::Corrupt("cloud workspace generation"));
            }
            if current.generation > self.inner.config.cloud.max_versions_per_workspace as u64 {
                return Err(Error::Limit("latest capture version count"));
            }
            let lower = Bound::Excluded(version_key(repo, owner, workspace, 0));
            let upper = Bound::Included(version_key(repo, owner, workspace, current.generation));
            let mut count = 0u64;
            let mut previous = None;
            // At most one retained model and one decoder coexist. Each codec
            // clamps its complete model/scratch budget to manifest_limits;
            // the admitted version profile reserves twice that decode bound.
            let mut winner: Option<(u64, tinytree_cloud::ManifestV3)> = None;
            let mut ambiguous = false;
            for item in view.range(&self.inner.spaces.cloud.versions, (lower, upper)) {
                within(deadline)?;
                let (key, bytes) = item.into_inner()?;
                scan.visit(&self.inner, &key, &bytes)?;
                if bytes.len() > 1024 {
                    return Err(Error::Corrupt("cloud version row"));
                }
                let record: VersionRecord = decode(&bytes)?;
                count = count
                    .checked_add(1)
                    .ok_or(Error::Limit("latest capture version count"))?;
                if ![1, 2, 3].contains(&record.format)
                    || record.workspace != workspace
                    || record.generation != count
                    || record.previous != previous
                    || key.as_ref() != version_key(repo, owner, workspace, count).as_slice()
                {
                    return Err(Error::Corrupt("cloud version selection identity"));
                }
                previous = Some(record.manifest);
                let id = ManifestId(record.manifest);
                let key = manifest_key(repo, owner, id);
                let bytes = view
                    .get(&self.inner.spaces.cloud.manifests, &key)?
                    .ok_or(Error::Corrupt("cloud version manifest missing"))?;
                scan.visit(&self.inner, &key, &bytes)?;
                within(deadline)?;
                let manifest = self.inner.decode_recovery(&bytes, id, limits)?;
                within(deadline)?;
                if manifest.format() != record.format {
                    return Err(Error::Corrupt("cloud version manifest format"));
                }
                if manifest.workspace != workspace {
                    return Err(Error::Corrupt("cloud version workspace"));
                }
                let RecoveryManifest::V3(mut candidate) = manifest else {
                    continue;
                };
                // Keep the complete canonical binding except publication parents.
                candidate.history.recovery.parents.clear();
                match winner.as_mut() {
                    None => winner = Some((count, candidate)),
                    Some((generation, selected)) => {
                        if candidate.history.recovery.exporter != selected.history.recovery.exporter
                        {
                            return Err(Error::Invalid("ambiguous latest capture exporter"));
                        }
                        match candidate
                            .history
                            .recovery
                            .sequence
                            .cmp(&selected.history.recovery.sequence)
                        {
                            std::cmp::Ordering::Greater => {
                                *generation = count;
                                *selected = candidate;
                                ambiguous = false;
                            }
                            std::cmp::Ordering::Equal => {
                                if candidate != *selected {
                                    ambiguous = true;
                                } else {
                                    *generation = count;
                                }
                            }
                            std::cmp::Ordering::Less => {}
                        }
                    }
                }
            }
            if count != current.generation || previous != Some(current.manifest) {
                return Err(Error::Corrupt("cloud version selection gap"));
            }
            if ambiguous {
                return Err(Error::Invalid("ambiguous latest capture sequence"));
            }
            winner.map(|(generation, _)| generation)
        };
        within(deadline)?;
        // The scan holds no decoded candidates or catalog gate during the one
        // full retained proof. Immutable generations remain stable after unlock.
        let result = generation
            .map(|generation| self.read_cloud_version(repo, owner, workspace, generation, limits))
            .transpose()?;
        within(deadline)?;
        Ok(result)
    }
}
