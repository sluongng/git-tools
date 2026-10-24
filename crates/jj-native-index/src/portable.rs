//! Exact, bounded operation-index export. Artifacts are not retention or fetch grants.
use super::*;
use std::sync::Mutex;
use tinytree_cloud as cloud;

#[derive(Clone, Debug)]
pub struct PortableExportLimits {
    pub max_operations: usize,
    /// Aggregate association and segment reads, including repeated checkpoints.
    pub max_source_reads: usize,
    pub max_source_bytes: usize,
    /// Conservative logical byte/record/radix work, not elapsed CPU or RSS.
    pub max_work_units: u64,
    /// Shared retained quota: source/build peaks, output tables and escaped artifacts.
    pub max_working_bytes: usize,
    pub artifacts: cloud::IndexArtifactLimits,
}
impl Default for PortableExportLimits {
    fn default() -> Self {
        Self {
            max_operations: 1024,
            max_source_reads: 65_536,
            max_source_bytes: 256 * 1024 * 1024,
            max_work_units: 8 * 1024 * 1024 * 1024,
            max_working_bytes: 512 * 1024 * 1024,
            artifacts: Default::default(),
        }
    }
}
impl PortableExportLimits {
    fn validate(&self) -> Result<(), Error> {
        self.artifacts.validate().map_err(cloud_error)?;
        if self.max_operations == 0
            || self.max_operations > 65_537
            || self.max_source_reads == 0
            || self.max_source_reads > 10_000_000
            || self.max_source_bytes == 0
            || self.max_source_bytes > 1024 * 1024 * 1024
            || self.max_work_units == 0
            || self.max_work_units > (1u64 << 48)
            || self.max_working_bytes == 0
            || self.max_working_bytes > 1024 * 1024 * 1024
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PortableExportWork {
    pub source_reads: usize,
    pub source_bytes: usize,
    pub logical_work_units: u64,
    pub unique_artifact_bytes: u64,
    pub unique_artifacts: usize,
}
struct Quota {
    used: Mutex<usize>,
    maximum: usize,
}
struct Reservation {
    quota: Arc<Quota>,
    bytes: usize,
}
impl Quota {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation, Error> {
        let mut used = self.used.lock().unwrap();
        *used = used
            .checked_add(bytes)
            .filter(|n| *n <= self.maximum)
            .ok_or(Error::Limit)?;
        Ok(Reservation {
            quota: self.clone(),
            bytes,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        *self.quota.used.lock().unwrap() -= self.bytes;
    }
}
struct ExportOwner {
    // Captured source/provider owners are destroyed before releasing admission.
    _caller: QueryOwner,
    _source: QueryOwner,
    _reservation: Reservation,
}
/// One immutable encoded artifact. Payload access is borrowed so retained SDK
/// allocations cannot escape their quota/source owners. Sinks may retain this
/// value, but doing so can make later export work fail fast with `Error::Limit`.
pub struct ExportArtifact {
    descriptor: cloud::IndexArtifactDescriptor,
    bytes: Vec<u8>,
    _owner: Arc<ExportOwner>,
}
impl ExportArtifact {
    pub fn descriptor(&self) -> cloud::IndexArtifactDescriptor {
        self.descriptor
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
/// A sink owns each emitted artifact until its write physically completes.
/// Returning success need not retain it; any retained artifact remains charged.
/// Cancellation must retain accepted physical jobs' entire artifact, not merely
/// a cloned byte slice or its source guard. This method never publishes a version.
#[async_trait]
pub trait IndexArtifactSink: Send {
    async fn write(&mut self, artifact: ExportArtifact) -> Result<(), Error>;
}
/// Complete successful export metadata. Partial sink writes on error/cancellation
/// are staging data only; no complete result or retained version is implied.
pub struct PortableIndexExport {
    operations: Vec<cloud::OperationIndex>,
    roots: Vec<cloud::IndexRootDescriptor>,
    artifacts: Vec<cloud::IndexArtifactDescriptor>,
    work: PortableExportWork,
    _owner: ExportOwner,
}
impl PortableIndexExport {
    pub fn operation_indexes(&self) -> &[cloud::OperationIndex] {
        &self.operations
    }
    pub fn index_roots(&self) -> &[cloud::IndexRootDescriptor] {
        &self.roots
    }
    pub fn artifacts(&self) -> &[cloud::IndexArtifactDescriptor] {
        &self.artifacts
    }
    pub fn work(&self) -> PortableExportWork {
        self.work
    }
}
fn cloud_error(error: cloud::Error) -> Error {
    match error {
        cloud::Error::Limit(_) => Error::Limit,
        _ => Error::Corrupt,
    }
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b).ok_or(Error::Limit)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b).ok_or(Error::Limit)
}
fn charge(
    work: &mut PortableExportWork,
    units: u64,
    limits: &PortableExportLimits,
) -> Result<(), Error> {
    work.logical_work_units = work
        .logical_work_units
        .checked_add(units)
        .filter(|n| *n <= limits.max_work_units)
        .ok_or(Error::Limit)?;
    Ok(())
}
impl NativeIndexStore {
    /// Export ONLY the supplied frozen, strictly sorted unique operation set.
    /// Operation zero must be explicitly present and have a real association.
    /// No current refs, canonical commit/TREE/FILE fetches or query authority are
    /// inferred. Repeated checkpoints are rebuilt and charged, not CPU-cached.
    ///
    /// Source reads and bounded encoding are synchronous portions of this async
    /// operation; hosts should run bulk export on their admitted blocking worker.
    /// No storage lock is held while awaiting the sink.
    pub async fn export_operation_indexes(
        &self,
        operation_ids: &[[u8; 64]],
        limits: PortableExportLimits,
        sink: &mut dyn IndexArtifactSink,
        owner: QueryOwner,
    ) -> Result<PortableIndexExport, Error> {
        limits.validate()?;
        if operation_ids.is_empty() || operation_ids.len() > limits.max_operations {
            return Err(Error::Limit);
        }
        if operation_ids[0] != [0; 64] || !operation_ids.windows(2).all(|p| p[0] < p[1]) {
            return Err(Error::Invalid);
        }
        let quota = Arc::new(Quota {
            used: Mutex::new(0),
            maximum: limits.max_working_bytes,
        });
        // Maps, result Vec capacity, keys and allocator slack coexist at return.
        let table_bytes = add(
            4096,
            add(
                mul(operation_ids.len(), 256)?,
                add(
                    mul(limits.artifacts.max_artifacts, 512)?,
                    mul(limits.artifacts.max_snapshots, 512)?,
                )?,
            )?,
        )?;
        let output_owner = ExportOwner {
            _caller: owner.clone(),
            _source: self.core.owner(),
            _reservation: quota.reserve(table_bytes)?,
        };
        let mut operations = Vec::with_capacity(operation_ids.len());
        let mut roots = BTreeMap::<cloud::IndexRootId, cloud::IndexRootDescriptor>::new();
        let mut artifacts =
            BTreeMap::<cloud::IndexArtifactId, cloud::IndexArtifactDescriptor>::new();
        let mut reads = persistence::ExportReadBudget {
            bytes: 0,
            reads: 0,
            max_bytes: limits.max_source_bytes,
            max_reads: limits.max_source_reads,
        };
        let mut work = PortableExportWork::default();
        for operation in operation_ids {
            // Includes local decoded records, loader maps, portable parent-copy
            // vectors and sorting scratch before any checkpoint is allocated.
            let load_peak = add(
                4096,
                add(
                    mul(self.core.limits.max_checkpoint_bytes, 8)?,
                    add(
                        mul(self.core.limits.max_drafts, 1024)?,
                        add(
                            mul(self.core.limits.max_segments, 512)?,
                            mul(self.core.limits.max_roots, 256)?,
                        )?,
                    )?,
                )?,
            )?;
            let load_owner = quota.reserve(load_peak)?;
            let before = reads.bytes;
            // Reserve verification/decoding work before each physical read.
            let source_work_bytes =
                usize::try_from((limits.max_work_units - work.logical_work_units) / 128)
                    .map_err(|_| Error::Limit)?;
            reads.max_bytes = limits
                .max_source_bytes
                .min(add(reads.bytes, source_work_bytes)?);
            let (anchors, drafts) =
                self.core
                    .storage
                    .export_operation(operation, &self.core.limits, &mut reads)?;
            charge(&mut work, (reads.bytes - before) as u64 * 128, &limits)?;
            let parent_count = drafts
                .iter()
                .try_fold(0usize, |n, d| add(n, d.parents.len()))?;
            let record_bytes = add(
                22,
                add(
                    mul(anchors.len(), 32)?,
                    add(mul(drafts.len(), 70)?, mul(parent_count, 32)?)?,
                )?,
            )?;
            // Radix depth is at most64 nibbles. This charge also covers model
            // construction, canonical validation, hashing and bounded sorting.
            let sort_work = mul(
                drafts.len(),
                (usize::BITS - drafts.len().max(1).leading_zeros()) as usize,
            )?;
            charge(
                &mut work,
                add(mul(record_bytes, 128)?, mul(sort_work, 64)?)? as u64,
                &limits,
            )?;
            let mut snapshot = cloud::PortableIndexSnapshot {
                roots: anchors,
                drafts: drafts
                    .iter()
                    .map(|draft| cloud::IndexDraft {
                        descriptor: draft.descriptor,
                        change: draft.change,
                        parents: draft.parents.clone(),
                        order: draft.order,
                    })
                    .collect(),
            };
            snapshot.drafts.sort_by_key(cloud::IndexDraft::commit);
            let build_owner = Arc::new(ExportOwner {
                _caller: owner.clone(),
                _source: self.core.owner(),
                _reservation: quota.reserve(limits.artifacts.max_decode_bytes)?,
            });
            let built =
                cloud::build_index_artifacts(&snapshot, limits.artifacts).map_err(cloud_error)?;
            // No canonical payloads or model records escape the synchronous
            // builder. Emitted artifact Vecs are moved, never cloned as a bundle.
            drop(snapshot);
            drop(drafts);
            drop(load_owner);
            if let Some(existing) = roots.get(&built.root.id) {
                if existing != &built.root {
                    return Err(Error::Corrupt);
                }
            } else {
                if roots.len() >= limits.artifacts.max_snapshots {
                    return Err(Error::Limit);
                }
                roots.insert(built.root.id, built.root);
            }
            operations.push(cloud::OperationIndex {
                operation: *operation,
                root: built.root.id,
            });
            for artifact in built.artifacts {
                charge(&mut work, artifact.bytes.len() as u64, &limits)?;
                if let Some(existing) = artifacts.get(&artifact.descriptor.id) {
                    if existing != &artifact.descriptor {
                        return Err(Error::Corrupt);
                    }
                    continue;
                }
                if artifacts.len() >= limits.artifacts.max_artifacts {
                    return Err(Error::Limit);
                }
                work.unique_artifact_bytes = work
                    .unique_artifact_bytes
                    .checked_add(artifact.bytes.len() as u64)
                    .filter(|n| *n <= limits.artifacts.max_total_artifact_bytes)
                    .ok_or(Error::Limit)?;
                artifacts.insert(artifact.descriptor.id, artifact.descriptor);
                sink.write(ExportArtifact {
                    descriptor: artifact.descriptor,
                    bytes: artifact.bytes,
                    _owner: build_owner.clone(),
                })
                .await?;
            }
        }
        work.source_reads = reads.reads;
        work.source_bytes = reads.bytes;
        work.unique_artifacts = artifacts.len();
        Ok(PortableIndexExport {
            operations,
            roots: roots.into_values().collect(),
            artifacts: artifacts.into_values().collect(),
            work,
            _owner: output_owner,
        })
    }
}
