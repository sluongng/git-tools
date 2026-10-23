//! Client-only unreachable collection. This is not a workspace-root registry or
//! permission to offload reachable bytes. The caller must fence the COMPLETE
//! workspace/operation/in-flight root registry for the entire synchronous call.
use super::*;

/// Snapshot of this process's store instance, repository and durable catalog.
/// A workspace registry fence is still required: a catalog epoch cannot detect
/// a workspace resurrecting an old, already resident object.
#[derive(Clone)]
pub struct ClientGcEpoch {
    owner: Weak<Inner>,
    repository: String,
    revision: u64,
}
impl fmt::Debug for ClientGcEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientGcEpoch")
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct ClientGcLimits {
    pub max_roots: usize,
    pub max_promises: usize,
    /// Includes object, reference, staging and pending-deletion records scanned.
    pub max_catalog_records: usize,
    pub max_marked_objects: usize,
    /// Per strict/transient traversal (at most two traversals per collection).
    pub max_edges: usize,
    pub max_metadata_bytes: u64,
    pub max_path_contexts: usize,
    pub max_path_context_bytes: usize,
    /// Includes pending crash-recovery deletions, not only new victims.
    pub max_deleted_objects: usize,
    pub max_deleted_bytes: u64,
}
impl Default for ClientGcLimits {
    fn default() -> Self {
        Self {
            max_roots: 10_000,
            max_promises: 100_000,
            max_catalog_records: 200_000,
            max_marked_objects: 100_000,
            max_edges: 1_000_000,
            max_metadata_bytes: 128 * 1024 * 1024,
            max_path_contexts: 100_000,
            max_path_context_bytes: 32 * 1024 * 1024,
            max_deleted_objects: 10_000,
            max_deleted_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientGcRequest {
    pub epoch: ClientGcEpoch,
    /// Complete canonical commit/tree/file roots, including retained jj history.
    pub complete_roots: Vec<ObjectKey>,
    /// In-flight writes may have incomplete closures. Present descendants stay.
    pub transient_roots: Vec<ObjectKey>,
    /// Exact FILE descriptors derived from admitted canonical metadata and the
    /// caller's current remote-retention proof. Never creates resident indexes.
    pub promised_files: Vec<Descriptor>,
    pub now: i64,
    pub grace_seconds: u64,
    pub limits: ClientGcLimits,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientGcReport {
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
    pub unlinked_payloads: u64,
    pub deferred_deletions: u64,
    pub stamped_unknown_objects: u64,
    pub expired_leases: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Age {
    touched_at: i64,
    revision: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Deletion {
    object: StoredDescriptor,
    revision: u64,
}
fn count(scanned: &mut usize, maximum: usize) -> Result<()> {
    *scanned = scanned
        .checked_add(1)
        .ok_or(Error::Limit("client GC catalog"))?;
    if *scanned > maximum {
        return Err(Error::Limit("client GC catalog"));
    }
    Ok(())
}
impl Inner {
    fn client_revision(&self, repo: &str) -> Result<u64> {
        self.db
            .read_tx()
            .get(&self.spaces.client_revisions, repo)?
            .map(|b| decode(&b))
            .transpose()
            .map(|v| v.unwrap_or(0))
    }
    pub(super) fn advance_client_revision(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        repo: &str,
    ) -> Result<u64> {
        let previous: u64 = tx
            .get(&self.spaces.client_revisions, repo)?
            .map(|b| decode(&b))
            .transpose()?
            .unwrap_or(0);
        let next = previous
            .checked_add(1)
            .ok_or(Error::Limit("client catalog revision"))?;
        tx.insert(&self.spaces.client_revisions, repo, encode(&next)?);
        Ok(next)
    }
    pub(super) fn touch_client_object(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        repo: &str,
        object: ObjectKey,
        now: i64,
    ) -> Result<()> {
        let k = object_key(repo, object);
        let previous: Option<Age> = tx
            .get(&self.spaces.client_ages, &k)?
            .map(|b| decode(&b))
            .transpose()?;
        let revision = self.advance_client_revision(tx, repo)?;
        let touched_at = previous.map_or(now, |old| old.touched_at.max(now));
        tx.insert(
            &self.spaces.client_ages,
            k,
            encode(&Age {
                touched_at,
                revision,
            })?,
        );
        // A reinstallation supersedes an earlier deletion. The publication gate
        // prevents recovery unlink from racing this index installation.
        tx.remove(&self.spaces.client_deletions, object_key(repo, object));
        Ok(())
    }
}
impl Store {
    pub fn client_gc_epoch(&self, repo: &str) -> Result<ClientGcEpoch> {
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        Ok(ClientGcEpoch {
            owner: Arc::downgrade(&self.inner),
            repository: repo.to_owned(),
            revision: self.inner.client_revision(repo)?,
        })
    }

    /// Unreachable resident sweep only. Caller must hold its complete root
    /// registry/collector guard until this returns, including error cleanup.
    /// This synchronous operation must be drained, not timeout-detached while
    /// releasing the caller's root fence. No network I/O occurs under the gate.
    /// Limits bound logical records/graph work, not Fjall's physical disk usage.
    pub fn client_gc(&self, repo: &str, request: &ClientGcRequest) -> Result<ClientGcReport> {
        let gate = self.inner.lock()?;
        let mut repository = self.inner.repository(repo)?;
        if self.inner.has_cloud_state(repo)? {
            return Err(Error::Invalid(
                "cloud state requires authoritative server GC",
            ));
        }
        if !Weak::ptr_eq(&request.epoch.owner, &Arc::downgrade(&self.inner))
            || request.epoch.repository != repo
            || request.epoch.revision != self.inner.client_revision(repo)?
        {
            return Err(Error::Invalid("stale client GC epoch"));
        }
        let l = &request.limits;
        if l.max_roots == 0
            || l.max_catalog_records == 0
            || l.max_marked_objects == 0
            || l.max_edges == 0
            || l.max_metadata_bytes == 0
            || l.max_path_contexts == 0
            || l.max_path_context_bytes == 0
            || l.max_deleted_objects == 0
        {
            return Err(Error::Invalid("client GC limits"));
        }
        if request
            .complete_roots
            .len()
            .checked_add(request.transient_roots.len())
            .is_none_or(|n| n > l.max_roots)
            || request.promised_files.len() > l.max_promises
        {
            return Err(Error::Limit("client GC roots or promises"));
        }
        let mut promises = HashMap::new();
        for d in &request.promised_files {
            if d.key.kind != Kind::File
                || d.format_version != tinytree_model::FORMAT_VERSION
                || d.size_bytes > self.inner.config.max_file_bytes
            {
                return Err(Error::Invalid("client GC file promise"));
            }
            if promises.insert(d.key, *d).is_some_and(|old| old != *d) {
                return Err(Error::Invalid("conflicting client GC file promises"));
            }
            // A promise is never an excuse for an indexed corrupt payload.
            if self
                .inner
                .descriptor(repo, d.key)?
                .is_some_and(|actual| actual != *d)
            {
                return Err(Error::Invalid("client GC file promise length"));
            }
        }
        let mut config = self.inner.config.clone();
        config.max_closure_objects = config.max_closure_objects.min(l.max_marked_objects);
        config.max_closure_edges = config.max_closure_edges.min(l.max_edges);
        config.max_path_contexts = config.max_path_contexts.min(l.max_path_contexts);
        config.max_path_context_bytes = config.max_path_context_bytes.min(l.max_path_context_bytes);
        let prefix = key(repo, b"");
        let view = self.inner.db.read_tx();
        let mut scanned = 0;
        let mut roots = request.complete_roots.clone();
        roots.push(tinytree_model::empty_tree().descriptor.key);
        for item in view.prefix(&self.inner.spaces.refs, &prefix) {
            count(&mut scanned, l.max_catalog_records)?;
            let (_, bytes) = item.into_inner()?;
            if let Some(target) = decode::<StoredReference>(&bytes)?.commit {
                let d = target.descriptor()?;
                if d.key.kind != Kind::Commit {
                    return Err(Error::Corrupt("reference target type"));
                }
                self.inner.require_descriptor(repo, d)?;
                roots.push(d.key);
                if roots.len() > l.max_roots {
                    return Err(Error::Limit("client GC roots"));
                }
            }
        }
        let mut transient = request.transient_roots.clone();
        let mut expired_staging = Vec::new();
        for item in view.prefix(&self.inner.spaces.staging, &prefix) {
            count(&mut scanned, l.max_catalog_records)?;
            let (stage_key, bytes) = item.into_inner()?;
            let stage: StageRecord = decode(&bytes)?;
            if stage.expires_at > request.now {
                let d = stage.object.descriptor()?;
                self.inner.require_descriptor(repo, d)?;
                transient.push(d.key);
            } else {
                expired_staging.push(stage_key);
            }
            if transient.len() > l.max_marked_objects {
                return Err(Error::Limit("client GC transient roots"));
            }
        }
        for ((r, k), _) in &gate.readers {
            if r == repo {
                transient.push(*k);
            }
            if transient.len() > l.max_marked_objects {
                return Err(Error::Limit("client GC transient roots"));
            }
        }
        let mut catalog = Vec::new();
        let mut unknown = Vec::new();
        for item in view.prefix(&self.inner.spaces.objects, &prefix) {
            count(&mut scanned, l.max_catalog_records)?;
            let (k, bytes) = item.into_inner()?;
            let d = decode::<StoredDescriptor>(&bytes)?.descriptor()?;
            if &*k != object_key(repo, d.key).as_slice() {
                return Err(Error::Corrupt("object index key"));
            }
            self.inner.require_descriptor(repo, d)?;
            let age: Option<Age> = view
                .get(&self.inner.spaces.client_ages, &k)?
                .map(|b| decode(&b))
                .transpose()?;
            let recent = match age {
                None => {
                    unknown.push(d.key);
                    true
                }
                Some(age) => {
                    if age.revision > request.epoch.revision {
                        return Err(Error::Corrupt("client object age revision"));
                    }
                    request
                        .now
                        .checked_sub(age.touched_at)
                        .is_none_or(|n| n < 0 || (n as u64) < request.grace_seconds)
                }
            };
            if recent {
                transient.push(d.key);
            }
            if transient.len() > l.max_marked_objects {
                return Err(Error::Limit("client GC transient roots"));
            }
            catalog.push(d);
        }
        let mut retained = self.inner.closure_bounded(
            repo,
            roots,
            true,
            false,
            &promises,
            &config,
            l.max_metadata_bytes,
        )?;
        retained.extend(self.inner.closure_bounded(
            repo,
            transient,
            false,
            false,
            &promises,
            &config,
            l.max_metadata_bytes,
        )?);
        if retained.len() > l.max_marked_objects {
            return Err(Error::Limit("client GC marked objects"));
        }
        let victims: Vec<_> = catalog
            .into_iter()
            .filter(|d| !retained.contains(&d.key))
            .collect();
        let mut pending = Vec::new();
        let mut delete_bytes = 0u64;
        for item in view.prefix(&self.inner.spaces.client_deletions, &prefix) {
            count(&mut scanned, l.max_catalog_records)?;
            let (k, bytes) = item.into_inner()?;
            let deletion: Deletion = decode(&bytes)?;
            let d = deletion.object.descriptor()?;
            if &*k != object_key(repo, d.key).as_slice()
                || deletion.revision > request.epoch.revision
            {
                return Err(Error::Corrupt("client deletion record"));
            }
            delete_bytes = delete_bytes
                .checked_add(d.size_bytes)
                .ok_or(Error::Limit("client GC deletion bytes"))?;
            pending.push(d);
        }
        for d in &victims {
            delete_bytes = delete_bytes
                .checked_add(d.size_bytes)
                .ok_or(Error::Limit("client GC deletion bytes"))?;
        }
        if pending
            .len()
            .checked_add(victims.len())
            .is_none_or(|n| n > l.max_deleted_objects)
            || delete_bytes > l.max_deleted_bytes
        {
            return Err(Error::Limit("client GC deletions"));
        }
        let mut report = ClientGcReport {
            stamped_unknown_objects: unknown.len() as u64,
            expired_leases: expired_staging.len() as u64,
            ..Default::default()
        };
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let revision = self.inner.advance_client_revision(&mut tx, repo)?;
        for k in expired_staging {
            tx.remove(&self.inner.spaces.staging, k);
        }
        for k in unknown {
            tx.insert(
                &self.inner.spaces.client_ages,
                object_key(repo, k),
                encode(&Age {
                    touched_at: request.now,
                    revision,
                })?,
            );
        }
        for d in &victims {
            repository.bytes = repository
                .bytes
                .checked_sub(d.size_bytes)
                .ok_or(Error::Corrupt("repository byte count"))?;
            repository.objects = repository
                .objects
                .checked_sub(1)
                .ok_or(Error::Corrupt("repository object count"))?;
            let k = object_key(repo, d.key);
            self.inner.remove_location(&mut tx, repo, d.key)?;
            tx.remove(&self.inner.spaces.objects, &k);
            tx.remove(&self.inner.spaces.client_ages, &k);
            tx.insert(
                &self.inner.spaces.client_deletions,
                &k,
                encode(&Deletion {
                    object: (*d).into(),
                    revision,
                })?,
            );
            report.deleted_objects += 1;
            report.deleted_bytes += d.size_bytes;
        }
        tx.insert(&self.inner.spaces.repos, repo, encode(&repository)?);
        self.inner.commit(tx)?;
        pending.extend(victims);
        let mut completed = Vec::new();
        for d in pending {
            let k = object_key(repo, d.key);
            // A restored index wins even if it has the same immutable ID. Do not
            // unlink a replacement or an unindexed candidate held by a writer.
            if self
                .inner
                .db
                .read_tx()
                .get(&self.inner.spaces.objects, &k)?
                .is_some()
            {
                completed.push(k);
                continue;
            }
            if gate.readers.contains_key(&(repo.to_owned(), d.key)) {
                report.deferred_deletions += 1;
                continue;
            }
            let path = self.inner.path(repo, d.key);
            match fs::remove_file(&path) {
                Ok(()) => report.unlinked_payloads += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            sync_dir(path.parent().ok_or(Error::Corrupt("object path"))?)?;
            completed.push(k);
        }
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        for k in completed {
            tx.remove(&self.inner.spaces.client_deletions, k);
        }
        self.inner.commit(tx)?;
        self.inner.reclaim_empty_packs(repo)?;
        Ok(report)
    }
}
