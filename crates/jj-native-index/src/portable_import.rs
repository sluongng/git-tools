//! Fresh exact index staging. This is not workspace restore or retention authority.
use super::*;
use tinytree_cloud as cloud;

/// Immutable version selection pins. Authorization comes only from the matching
/// configured provider's explicit cloud query API, never from these values.
#[derive(Clone, Debug)]
pub struct PortableImportVersion {
    pub provider_identity: [u8; 32],
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: [u8; 32],
    pub manifest_blake3: [u8; 32],
    pub manifest_size_bytes: u64,
}
impl PortableImportVersion {
    fn source(
        &self,
        root: cloud::IndexRootId,
        anchors: Vec<m::CommitId>,
    ) -> native_broker::CloudQuerySource {
        native_broker::CloudQuerySource {
            provider_identity: self.provider_identity,
            workspace_id: self.workspace_id,
            generation: self.generation,
            manifest_id: self.manifest_id,
            manifest_blake3: self.manifest_blake3,
            manifest_size_bytes: self.manifest_size_bytes,
            index_root_ids: vec![root.0],
            expected_anchors: anchors,
        }
    }
}
#[derive(Clone, Debug)]
pub struct PortableImportLimits {
    pub max_operations: usize,
    /// Importer allocations plus codec working reservations, excluding the
    /// caller's already-owned immutable artifact input and provider pools.
    pub max_working_bytes: usize,
    /// Aggregate logical bytes/records/parent work, not measured CPU or RSS.
    /// Includes the configured artifact-verifier byte/step ceiling reservation.
    pub max_work_units: u64,
    pub manifest: cloud::ManifestV2Limits,
}
impl Default for PortableImportLimits {
    fn default() -> Self {
        Self {
            max_operations: 1024,
            max_working_bytes: 512 * 1024 * 1024,
            max_work_units: 8 * 1024 * 1024 * 1024,
            manifest: Default::default(),
        }
    }
}
/// Exact source-bound local index staging only. No OP/VIEW installation, Ready,
/// backup completion, mount, Clean transition or sole-copy offload is implied.
#[derive(Debug)]
pub struct PortableImportReceipt {
    pub store: NativeIndexStore,
    pub operation_indexes: Vec<cloud::OperationIndex>,
    pub unique_snapshots: usize,
    /// Includes the conservative artifact verification ceiling reservation.
    pub logical_work_units: u64,
}
struct ScopeLease {
    scope: native_broker::QueryScope,
    owner: QueryOwner,
    cleanup: cleanup::Queue,
    timeout: Duration,
    closed: bool,
}
impl Drop for ScopeLease {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.cleanup
            .send(self.scope.handle, true, self.owner.clone(), self.timeout);
    }
}
fn cloud_error(error: cloud::Error) -> Error {
    match error {
        cloud::Error::Limit(_) => Error::Limit,
        _ => Error::Corrupt,
    }
}
fn charge(used: &mut u64, n: u64, maximum: u64) -> Result<(), Error> {
    *used = used
        .checked_add(n)
        .filter(|n| *n <= maximum)
        .ok_or(Error::Limit)?;
    Ok(())
}
fn snapshot_work(snapshot: &cloud::PortableIndexSnapshot) -> Result<u64, Error> {
    let mut bytes = snapshot.roots.len().checked_mul(32).ok_or(Error::Limit)?;
    for draft in &snapshot.drafts {
        bytes = bytes
            .checked_add(512)
            .and_then(|n| n.checked_add(draft.parents.len().checked_mul(64)?))
            .ok_or(Error::Limit)?;
    }
    Ok(bytes as u64)
}
impl NativeIndexStore {
    /// Import into an empty, restore-journal-owned private staging directory.
    /// All distinct snapshot sources are explicitly authorized and checked for
    /// baseline membership BEFORE filesystem creation. Unsupported providers and
    /// anchor-only implementations fail closed; ordinary root open is never used.
    ///
    /// Canonical baseline bytes are not hydrated: matching authenticated server
    /// snapshot validation is the canonical-summary authority. Actual canonical
    /// reads remain lazy through that source. Artifact shape alone is insufficient.
    /// Source/build/fsync work is synchronous; hosts must run this operation on an
    /// admitted owned worker. Providers must retain owner through physical jobs.
    /// An error after publication starts may leave partial OR fully loadable
    /// exact staging (including an uncertain final fsync or context-start error).
    /// Preserve it for explicit journal-bound reconciliation; an error is not
    /// rollback evidence or authority to adopt, repair, or delete the directory.
    pub async fn import_fresh_operation_indexes(
        path: &Path,
        config: Config,
        limits: Limits,
        query: Arc<dyn NativeQueryIo>,
        objects: Arc<dyn NativeObjectIo>,
        version: PortableImportVersion,
        manifest_bytes: &[u8],
        graph: &cloud::VerifiedIndexGraph<'_>,
        policy: PortableImportLimits,
        owner: QueryOwner,
    ) -> Result<PortableImportReceipt, Error> {
        persistence::require_durable_writer()?;
        config.validate()?;
        limits.validate()?;
        policy.manifest.validate().map_err(cloud_error)?;
        if policy.max_operations == 0
            || policy.max_operations > 65_537
            || policy.max_working_bytes == 0
            || policy.max_working_bytes > 1024 * 1024 * 1024
            || policy.max_work_units == 0
            || policy.max_work_units > (1u64 << 48)
        {
            return Err(Error::Invalid);
        }
        if manifest_bytes.len() > policy.manifest.recovery.max_manifest_bytes
            || manifest_bytes.len() > policy.max_working_bytes / 2
            || manifest_bytes.len() as u64 > policy.max_work_units
        {
            return Err(Error::Limit);
        }
        if version.provider_identity != config.provider_fingerprint {
            return Err(Error::Foreign);
        }
        if version.generation == 0
            || manifest_bytes.len() as u64 != version.manifest_size_bytes
            || blake3::hash(manifest_bytes).as_bytes() != &version.manifest_blake3
        {
            return Err(Error::Corrupt);
        }
        let mut codec = policy.manifest;
        codec.max_operation_indexes = codec.max_operation_indexes.min(policy.max_operations);
        codec.max_index_roots = codec.max_index_roots.min(policy.max_operations);
        codec.recovery.max_decode_bytes = codec
            .recovery
            .max_decode_bytes
            .min(policy.max_working_bytes / 2);
        codec.artifacts.max_decode_bytes = codec
            .artifacts
            .max_decode_bytes
            .min(policy.max_working_bytes / 2);
        codec.artifacts.snapshot.max_roots =
            codec.artifacts.snapshot.max_roots.min(limits.max_roots);
        codec.artifacts.snapshot.max_drafts =
            codec.artifacts.snapshot.max_drafts.min(limits.max_drafts);
        codec.artifacts.snapshot.max_parents =
            codec.artifacts.snapshot.max_parents.min(limits.max_parents);
        codec.artifacts.snapshot.max_metadata_bytes = codec
            .artifacts
            .snapshot
            .max_metadata_bytes
            .min(limits.max_metadata_bytes as u64);
        // V3 adds a bounded attachment descriptor; index staging validates its
        // framing/identity, not capture DATA or physical frontend quiescence.
        // Only the configured provider can authorize the selected history below.
        let manifest = match manifest_bytes.get(..10) {
            Some(header) if &header[..8] == b"TTCLOUD\0" && header[8..] == 2u16.to_le_bytes() => {
                cloud::decode_manifest_v2(
                    manifest_bytes,
                    cloud::ManifestId(version.manifest_id),
                    codec,
                )
                .map_err(cloud_error)?
            }
            Some(header) if &header[..8] == b"TTCLOUD\0" && header[8..] == 3u16.to_le_bytes() => {
                cloud::decode_manifest_v3(
                    manifest_bytes,
                    cloud::ManifestId(version.manifest_id),
                    cloud::ManifestV3Limits {
                        history: codec,
                        ..Default::default()
                    },
                )
                .map_err(cloud_error)?
                .history
            }
            _ => return Err(Error::Corrupt),
        };
        if manifest.recovery.workspace != version.workspace_id
            || manifest.index_roots.as_slice() != graph.roots()
        {
            return Err(Error::Corrupt);
        }
        let proof_work = codec
            .artifacts
            .max_total_artifact_bytes
            .checked_add(codec.artifacts.max_graph_steps as u64)
            .ok_or(Error::Limit)?;
        let mut work = 0;
        charge(&mut work, proof_work, policy.max_work_units)?;
        graph
            .validate_for_limits(codec.artifacts)
            .map_err(cloud_error)?;
        // Reject foreign/partial staging without adopting it. The durable writer
        // repeats this validation under its filesystem lock after source checks.
        match std::fs::symlink_metadata(path) {
            Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                return Err(Error::Corrupt);
            }
            Ok(_) if std::fs::read_dir(path)?.next().is_some() => return Err(Error::Corrupt),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let cleanup = cleanup::Queue::start(query.clone(), &limits)?;
        charge(
            &mut work,
            manifest_bytes.len() as u64,
            policy.max_work_units,
        )?;
        // Verify every exact frozen baseline, including hidden and promoted
        // records. Only bounded IDs cross the client; no canonical history fetch.
        for root in &manifest.index_roots {
            let snapshot = graph.snapshot(root.id).map_err(cloud_error)?;
            let memory = snapshot_work(&snapshot)?;
            if memory > (policy.max_working_bytes / 2) as u64 {
                return Err(Error::Limit);
            }
            charge(&mut work, memory, policy.max_work_units)?;
            let source = version.source(root.id, snapshot.roots);
            let reply = query
                .open_cloud_scope(
                    native_broker::OpenCloudQueryScope {
                        source: source.clone(),
                        overlay: vec![],
                        replay_key: random(),
                    },
                    owner.clone(),
                )
                .await
                .map_err(status)?;
            let mut lease = ScopeLease {
                scope: reply.value,
                owner: reply.owner,
                cleanup: cleanup.clone(),
                closed: false,
                timeout: Duration::from_millis(limits.cleanup_timeout_ms),
            };
            if lease.scope.query_identity_version != 2 {
                return Err(Error::Unsupported);
            }
            if lease.scope.cloud_source_digest != Some(source.digest())
                || lease.scope.expires_at_unix_seconds <= now()?
            {
                return Err(Error::Corrupt);
            }
            for batch in snapshot
                .drafts
                .chunks((limits.page_size as usize).min(64).max(1))
            {
                let reply = query
                    .facts(
                        lease.scope.handle,
                        batch
                            .iter()
                            .map(|draft| q::GraphFact {
                                fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                                    commit: Some(q::CommitId {
                                        value: draft.commit().0.to_vec(),
                                    }),
                                })),
                            })
                            .collect(),
                        Arc::new((owner.clone(), lease.owner.clone())),
                    )
                    .await
                    .map_err(status)?;
                if reply.value.answers.len() != batch.len()
                    || reply.value.answers.iter().any(|answer| {
                        !matches!(
                            answer.answer,
                            Some(q::graph_fact_answer::Answer::HasId(true))
                        )
                    })
                {
                    return Err(Error::Corrupt);
                }
            }
            // Queue owns only the independent persistent reply owner, never the
            // command's physical admission. Cancellation also schedules close.
            query
                .close_scope(
                    lease.scope.handle,
                    Arc::new((owner.clone(), lease.owner.clone())),
                )
                .await
                .map_err(status)?;
            lease.closed = true;
            drop(lease);
        }
        // No caller/worker cancellation can interrupt a synchronous file write;
        // owner remains live through every fsync and exact readback below.
        persistence::prepare_fresh_directory(path)?;
        let storage = Storage::init_import(path, config)?;
        let mut installed = BTreeMap::new();
        let mut retained = 0usize;
        for root in &manifest.index_roots {
            let snapshot = graph.snapshot(root.id).map_err(cloud_error)?;
            let snapshot_memory = snapshot_work(&snapshot)?;
            if snapshot_memory
                .checked_add(retained as u64)
                .is_none_or(|n| n > (policy.max_working_bytes / 2) as u64)
            {
                return Err(Error::Limit);
            }
            charge(&mut work, snapshot_memory, policy.max_work_units)?;
            let drafts = snapshot
                .drafts
                .into_iter()
                .map(|draft| {
                    let record = Arc::new(Draft {
                        order: draft.order,
                        descriptor: draft.descriptor,
                        change: draft.change,
                        parents: draft.parents,
                    });
                    (record.id(), record)
                })
                .collect();
            let drafts = persistence::order_drafts(drafts, &limits)?;
            let tips = storage.persist_exact_snapshot(&snapshot.roots, &drafts, &limits)?;
            let (actual_roots, actual_drafts) = storage.load(&tips, &limits)?;
            if actual_roots != snapshot.roots || actual_drafts != drafts {
                return Err(Error::Corrupt);
            }
            let source = CheckpointSource::CloudSnapshot {
                pins: version.source(root.id, snapshot.roots),
                baseline_tips: tips.clone(),
            };
            let source_bytes = source.encode(&limits)?.len();
            retained = retained
                .checked_add(source_bytes)
                .and_then(|n| n.checked_add(tips.len().checked_mul(64)?))
                .and_then(|n| n.checked_add(256))
                .ok_or(Error::Limit)?;
            if retained > policy.max_working_bytes / 2 {
                return Err(Error::Limit);
            }
            installed.insert(root.id, (tips, source));
        }
        for association in &manifest.operation_indexes {
            let (tips, source) = installed.get(&association.root).ok_or(Error::Corrupt)?;
            charge(
                &mut work,
                source.encode(&limits)?.len() as u64 + (tips.len() as u64 * 32) + 128,
                policy.max_work_units,
            )?;
            storage.associate_source(&association.operation, tips, source, &limits)?;
            let (actual, actual_source) = storage.association(&association.operation, &limits)?;
            if &actual != tips || &actual_source != source {
                return Err(Error::Corrupt);
            }
        }
        // Final marker is the only normal-open gate. It is not a workspace Ready
        // transaction and does not install OP/VIEW or change their semantic IDs.
        storage.finish_import(version.manifest_id, &limits)?;
        let store = Self::with_storage(storage, limits, query, objects)?;
        Ok(PortableImportReceipt {
            store,
            unique_snapshots: manifest.index_roots.len(),
            operation_indexes: manifest.operation_indexes,
            logical_work_units: work,
        })
    }
}
