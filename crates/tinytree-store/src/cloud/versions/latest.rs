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

/// Shared bounded logical ordering. Publication ancestry is not capture identity.
#[derive(Default)]
pub(super) struct LogicalCaptureHead {
    winner: Option<(u64, u64, [u8; 16], [u8; 32])>,
    ambiguous: bool,
}
impl LogicalCaptureHead {
    pub(super) fn consider(
        &mut self,
        generation: u64,
        mut candidate: tinytree_cloud::ManifestV3,
        limits: tinytree_cloud::ManifestV3Limits,
    ) -> Result<()> {
        candidate.history.recovery.parents.clear();
        // Canonical normalized bytes preserve the same complete logical binding
        // as latest selection, without retaining a second large decoded model.
        let bytes =
            tinytree_cloud::encode_manifest_v3(&candidate, limits).map_err(
                |error| match error {
                    tinytree_cloud::Error::Limit(_) => Error::Limit("cloud logical head binding"),
                    _ => Error::Corrupt("cloud logical head binding"),
                },
            )?;
        let binding = *blake3::hash(&bytes).as_bytes();
        let sequence = candidate.history.recovery.sequence;
        let exporter = candidate.history.recovery.exporter;
        match self.winner.as_mut() {
            None => self.winner = Some((generation, sequence, exporter, binding)),
            Some((selected_generation, selected_sequence, selected_exporter, selected_binding)) => {
                if exporter != *selected_exporter {
                    return Err(Error::Invalid("ambiguous latest capture exporter"));
                }
                match sequence.cmp(selected_sequence) {
                    std::cmp::Ordering::Greater => {
                        *selected_generation = generation;
                        *selected_sequence = sequence;
                        *selected_binding = binding;
                        self.ambiguous = false;
                    }
                    std::cmp::Ordering::Equal => {
                        if binding != *selected_binding {
                            self.ambiguous = true;
                        } else {
                            *selected_generation = (*selected_generation).max(generation);
                        }
                    }
                    std::cmp::Ordering::Less => {}
                }
            }
        }
        Ok(())
    }
    pub(super) fn finish(self) -> Result<Option<u64>> {
        if self.ambiguous {
            return Err(Error::Invalid("ambiguous latest capture sequence"));
        }
        Ok(self.winner.map(|(generation, _, _, _)| generation))
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
        let selected = {
            let mut gate = self.inner.lock()?;
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
            current.validate_lifecycle()?;
            if current.generation == 0 {
                return Err(Error::Corrupt("cloud workspace generation"));
            }
            if !self.inner.config.cloud.rolling_retention_enabled
                && current.generation - current.retired_through
                    > self.inner.config.cloud.max_versions_per_workspace
            {
                return Err(Error::Limit("latest capture version count"));
            }
            // Hidden logical-head exceptions participate in logical selection,
            // but remain absent from generation discovery and direct reads.
            let lower = Bound::Excluded(version_key(repo, owner, workspace, 0));
            let upper = Bound::Included(version_key(repo, owner, workspace, current.generation));
            let mut count = 0u64;
            let mut previous = current.boundary_manifest;
            // At most one retained model and one decoder coexist. Each codec
            // clamps its complete model/scratch budget to manifest_limits;
            // the admitted version profile reserves twice that decode bound.
            let mut head = LogicalCaptureHead::default();
            for item in view.range(&self.inner.spaces.cloud.versions, (lower, upper)) {
                within(deadline)?;
                let (key, bytes) = item.into_inner()?;
                scan.visit(&self.inner, &key, &bytes)?;
                if bytes.len() > 1024 {
                    return Err(Error::Corrupt("cloud version row"));
                }
                let record: VersionRecord = decode(&bytes)?;
                let generation = record.generation;
                if ![1, 2, 3].contains(&record.format)
                    || record.workspace != workspace
                    || generation == 0
                    || generation > current.generation
                    || key.as_ref() != version_key(repo, owner, workspace, generation).as_slice()
                {
                    return Err(Error::Corrupt("cloud version selection identity"));
                }
                if generation > current.retired_through {
                    count = count
                        .checked_add(1)
                        .ok_or(Error::Limit("latest capture version count"))?;
                    if generation != current.retired_through + count || record.previous != previous
                    {
                        return Err(Error::Corrupt("cloud version selection identity"));
                    }
                    previous = Some(record.manifest);
                }
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
                if let RecoveryManifest::V3(candidate) = manifest {
                    head.consider(
                        generation,
                        candidate,
                        self.inner
                            .v3_limits(limits, self.inner.config.cloud.max_version_decode_bytes)?,
                    )?;
                }
            }
            if count != current.generation - current.retired_through
                || previous != Some(current.manifest)
            {
                return Err(Error::Corrupt("cloud version selection gap"));
            }
            if let Some(generation) = head.finish()? {
                // Close the selection-to-read gap without retaining a decoded
                // model or a second catalog arena. GC/publication treat this
                // temporary physical reader exactly like an in-flight proof.
                let key = (repo.to_owned(), owner.principal, workspace, generation);
                if gate.cloud_version_readers.get(&key) == Some(&usize::MAX) {
                    return Err(Error::Limit("cloud version readers"));
                }
                let missing = usize::from(!gate.cloud_version_readers.contains_key(&key));
                gate.reserve_pins(&self.inner, 3, missing)?;
                *gate.cloud_version_readers.entry(key).or_default() += 1;
                Some((
                    generation,
                    ValidationGuard {
                        native: Vec::new(),
                        cloud: Vec::new(),
                        artifacts: Vec::new(),
                        version: Some((workspace, generation)),
                        repo: repo.to_owned(),
                        owner,
                        inner: self.inner.clone(),
                        _resident: None,
                    },
                ))
            } else {
                None
            }
        };
        within(deadline)?;
        let result = selected
            .map(|(generation, _selection)| {
                self.read_cloud_version(repo, owner, workspace, generation, limits)
            })
            .transpose()?;
        within(deadline)?;
        Ok(result)
    }
}
