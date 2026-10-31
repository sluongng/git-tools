//! Bounded successor coverage from complete verified private files only.
//! This proves semantic retention, not current remote authority or unlink rights.
use anyhow::{Context, Result, ensure};
use native_retention::{
    MAX_CAPTURE_COVERAGE_BYTES, MAX_CAPTURE_COVERAGE_EDGES, MAX_CAPTURE_COVERAGE_OBJECTS,
    OwnedCaptureCoverage,
};
use native_workspace_admin::private_bundle::VerifiedPrivateBundle;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};
use tinytree_cloud as c;
use tinytree_model as m;
type Owner = Arc<dyn std::any::Any + Send + Sync>;
const MAX_BUNDLE_ENTRIES: usize = 100_000;
fn typed(key: m::ObjectKey) -> (u8, [u8; 32]) {
    (key.kind as u8, key.id.0)
}
fn operation_id(bytes: &[u8]) -> Result<[u8; 64]> {
    Ok(bytes
        .try_into()
        .context("private coverage operation width")?)
}
/// Caller supplies bounded admission, NOT a registry CaptureReadGuard. Drop all
/// registry read guards before giving this result to maintenance. File readers
/// and admission remain owned through generic proof verification/commit.
pub(crate) async fn prove_successor(
    old: &VerifiedPrivateBundle,
    new: &VerifiedPrivateBundle,
    owner: Owner,
) -> Result<OwnedCaptureCoverage> {
    ensure!(
        old.manifest().workspace_id == new.manifest().workspace_id
            && old.manifest().exporter == new.manifest().exporter
            && old.manifest().generation < new.manifest().generation,
        "private successor logical identity/generation differs"
    );
    ensure!(
        old.operations().len() <= MAX_BUNDLE_ENTRIES
            && new.operations().len() <= MAX_BUNDLE_ENTRIES
            && new.native_descriptors().len() <= MAX_BUNDLE_ENTRIES,
        "private coverage inventory bound"
    );
    let members = new
        .operations()
        .iter()
        .map(c::VerifiedObject::id)
        .collect::<BTreeSet<_>>();
    for object in old.operations() {
        ensure!(
            members.contains(&object.id()),
            "private successor omits retained OP/VIEW identity"
        );
    }
    let old_checkpoint = crate::native_private_retention::checkpoint(old)?;
    let new_checkpoint = crate::native_private_retention::checkpoint(new)?;
    for operation in old_checkpoint
        .op_heads
        .iter()
        .chain(old_checkpoint.working_copy_operation.iter())
    {
        let id = operation_id(operation)?;
        ensure!(
            id == [0; 64] || members.contains(&c::ObjectId::Operation(id)),
            "private successor omits retained head/WC operation"
        );
    }
    ensure!(
        old_checkpoint.roots.len() <= MAX_BUNDLE_ENTRIES
            && new_checkpoint.roots.len() <= MAX_BUNDLE_ENTRIES,
        "private coverage root bound"
    );
    let old_reader = old.native_reader();
    let new_reader = new.native_reader();
    let owner: Owner = Arc::new((owner, old_reader, new_reader.clone()));
    let mut reachable = new_checkpoint
        .roots
        .iter()
        .copied()
        .map(typed)
        .collect::<BTreeSet<_>>();
    let mut missing = old_checkpoint
        .roots
        .iter()
        .copied()
        .map(typed)
        .filter(|key| !reachable.contains(key))
        .collect::<BTreeSet<_>>();
    if missing.is_empty() {
        return Ok(OwnedCaptureCoverage::new(vec![], owner));
    }
    let catalog = new
        .native_descriptors()
        .iter()
        .map(|d| (typed(d.key), *d))
        .collect::<BTreeMap<_, _>>();
    let mut pending = VecDeque::new();
    let mut queued = BTreeSet::new();
    // The previous WC COMMIT often remains an explicit index/OP root while its
    // TREE stops being the direct WC root. Prefer that exact anchored edge.
    for id in [
        old.working_copy().snapshot_commit,
        old.review_base().map(|base| base.base).unwrap_or([0; 32]),
        new.working_copy().snapshot_commit,
    ] {
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(id),
        };
        if id != [0; 32] && reachable.contains(&typed(key)) && queued.insert(typed(key)) {
            pending.push_back(key);
        }
    }
    for key in &new_checkpoint.roots {
        if queued.insert(typed(*key)) {
            pending.push_back(*key);
        }
    }
    let mut read = BTreeSet::new();
    let mut objects = vec![];
    let (mut bytes, mut edges) = (0usize, 0usize);
    while !missing.is_empty() {
        let key = pending
            .pop_front()
            .context("private successor does not cover retained typed roots")?;
        if key.kind == m::Kind::File
            || key.kind == m::Kind::Symlink
            || (key.kind == m::Kind::Commit && key.id.0 == [0; 32])
            || !read.insert(typed(key))
        {
            continue;
        }
        ensure!(
            objects.len() < MAX_CAPTURE_COVERAGE_OBJECTS,
            "private coverage object bound"
        );
        let descriptor = *catalog
            .get(&typed(key))
            .context("private successor metadata is not in verified bundle")?;
        ensure!(
            descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
            "private coverage descriptor differs"
        );
        let size =
            usize::try_from(descriptor.size_bytes).context("private coverage metadata size")?;
        let next_bytes = bytes
            .checked_add(size)
            .filter(|n| *n <= MAX_CAPTURE_COVERAGE_BYTES)
            .context("private coverage byte bound")?;
        // Reader has no network/cache source and rehashes its held file bytes.
        let metadata = new_reader
            .read_metadata_with_owner(key, owner.clone())
            .await?;
        ensure!(
            metadata.descriptor() == descriptor && metadata.bytes().len() == size,
            "private coverage source changed"
        );
        let limits = m::Limits {
            max_object_bytes: MAX_CAPTURE_COVERAGE_BYTES,
            ..m::Limits::default()
        };
        m::verify(&descriptor, metadata.bytes(), limits)?;
        let value = m::decode(metadata.bytes(), limits)?;
        let encoded = m::encode(&value, limits)?;
        ensure!(
            encoded.descriptor == descriptor && encoded.bytes.as_slice() == metadata.bytes(),
            "private coverage metadata is not canonical"
        );
        let references = value.references();
        edges = edges
            .checked_add(references.len())
            .filter(|n| *n <= MAX_CAPTURE_COVERAGE_EDGES)
            .context("private coverage edge bound")?;
        for reference in references {
            let id = typed(reference);
            missing.remove(&id);
            if reachable.insert(id) && queued.insert(id) {
                pending.push_front(reference);
            }
        }
        bytes = next_bytes;
        objects.push(encoded);
    }
    Ok(OwnedCaptureCoverage::new(objects, owner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::object_id::ObjectId as _;
    use native_workspace_admin::private_bundle::{
        BundleLimits, LegacyCaptureState, LegacyPrivateManifest,
    };
    use std::{
        fs::File,
        io::{Seek, SeekFrom, Write},
    };
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    fn frame(file: &mut File, kind: u8, id: &[u8], bytes: &[u8]) -> Result<()> {
        file.write_all(&[kind, id.len() as u8])?;
        file.write_all(id)?;
        file.write_all(&(bytes.len() as u64).to_le_bytes())?;
        file.write_all(blake3::hash(bytes).as_bytes())?;
        file.write_all(bytes)?;
        Ok(())
    }
    /// Real versioned private-file bytes enter the public semantic verifier;
    /// this fixture creates no remote authority or registry retirement ticket.
    fn bundle(
        mut native: Vec<m::EncodedObject>,
        mut roots: Vec<m::ObjectKey>,
        cloud: Vec<c::VerifiedObject>,
        generation: u64,
    ) -> Result<VerifiedPrivateBundle> {
        let empty = m::empty_tree();
        if !native
            .iter()
            .any(|e| e.descriptor.key == empty.descriptor.key)
        {
            native.push(empty.clone());
        }
        roots.push(empty.descriptor.key);
        roots.sort_by_key(|k| typed(*k));
        roots.dedup();
        let mut records = vec![c::OperationRecord {
            operation: "00".repeat(64),
            view: "00".repeat(64),
            parents: vec![],
        }];
        for object in &cloud {
            if let Some(op) = object.operation() {
                records.push(c::OperationRecord {
                    operation: hex(object.id().bytes()),
                    view: op.view_id.hex(),
                    parents: op.parents.iter().map(|id| id.hex()).collect(),
                });
            }
        }
        records.sort_by(|a, b| a.operation.cmp(&b.operation));
        let heads = vec![records.last().unwrap().operation.clone()];
        let manifest = LegacyPrivateManifest {
            schema_version: 1,
            codec: c::CODEC.into(),
            workspace_id: [1; 32],
            source_workspace_id: [2; 32],
            exporter: [3; 16],
            generation,
            capture: LegacyCaptureState {
                review_base: None,
                operation_heads: heads,
                working_copy: c::PortableWorkingCopy {
                    workspace: "default".into(),
                    wc_operation: "00".repeat(64),
                    snapshot_operation: "00".repeat(64),
                    snapshot_commit: [0; 32],
                    tree: c::CaptureTree {
                        terms: vec![empty.descriptor.key.id.0],
                        labels: vec![],
                    },
                },
                operations: records,
                roots: roots
                    .into_iter()
                    .map(|k| c::CapturedRoot {
                        kind: k.kind as u8,
                        id: k.id.0,
                    })
                    .collect(),
                remote: None,
            },
        };
        let mut file = tempfile::tempfile()?;
        file.write_all(b"TTPRIV1\0")?;
        for object in cloud {
            let kind = match object.id() {
                c::ObjectId::Operation(_) => 1,
                c::ObjectId::View(_) => 2,
            };
            frame(
                &mut file,
                kind,
                object.id().bytes(),
                object.payload().context("fixture intrinsic payload")?,
            )?;
        }
        for object in native {
            frame(
                &mut file,
                16 + object.descriptor.key.kind as u8,
                &object.descriptor.key.id.0,
                &object.bytes,
            )?;
        }
        frame(&mut file, 6, &[], &serde_json::to_vec(&manifest)?)?;
        file.write_all(&[255])?;
        file.sync_all()?;
        file.seek(SeekFrom::Start(0))?;
        let mut hash = blake3::Hasher::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = std::io::Read::read(&mut file, &mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        native_workspace_admin::private_bundle::verify_file(
            file,
            *hash.finalize().as_bytes(),
            BundleLimits::default(),
        )
    }
    fn leaf(bytes: &[u8]) -> m::EncodedObject {
        m::EncodedObject {
            descriptor: m::describe_file(bytes),
            bytes: bytes.to_vec(),
        }
    }
    fn tree(file: &m::EncodedObject, name: &str) -> Result<m::EncodedObject> {
        Ok(m::encode(
            &m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: name.into(),
                    value: m::TreeValue::File {
                        id: m::FileId(file.descriptor.key.id.0),
                        executable: false,
                        size_bytes: file.descriptor.size_bytes,
                        copy_id: None,
                    },
                }],
            }),
            m::Limits::default(),
        )?)
    }
    #[tokio::test]
    async fn private_coverage_accepts_direct_or_canonically_anchored_typed_root() -> Result<()> {
        let file = leaf(b"retained private bytes");
        let old = bundle(vec![file.clone()], vec![file.descriptor.key], vec![], 1)?;
        let direct = bundle(vec![file.clone()], vec![file.descriptor.key], vec![], 2)?;
        prove_successor(&old, &direct, Arc::new(())).await?;
        let parent = tree(&file, "retained.txt")?;
        let indirect = bundle(
            vec![file, parent.clone()],
            vec![parent.descriptor.key],
            vec![],
            3,
        )?;
        prove_successor(&old, &indirect, Arc::new(())).await?;
        Ok(())
    }
    #[tokio::test]
    async fn private_coverage_refuses_missing_root_without_old_file_or_network_fallback()
    -> Result<()> {
        let old_file = leaf(b"old unique bytes");
        let next_file = leaf(b"different successor bytes");
        let old = bundle(
            vec![old_file.clone()],
            vec![old_file.descriptor.key],
            vec![],
            1,
        )?;
        let next = bundle(
            vec![next_file.clone()],
            vec![next_file.descriptor.key],
            vec![],
            2,
        )?;
        let error = prove_successor(&old, &next, Arc::new(()))
            .await
            .err()
            .context("missing old root must refuse")?;
        assert!(
            error
                .to_string()
                .contains("does not cover retained typed roots")
        );
        Ok(())
    }
    #[tokio::test]
    async fn private_coverage_refuses_lost_operation_and_view_even_when_native_roots_match()
    -> Result<()> {
        use jj_lib::{
            backend::CommitId,
            op_store::{OpStore, Operation, OperationId, RootOperationData, View},
            simple_op_store::SimpleOpStore,
        };
        let directory = tempfile::tempdir()?;
        let store = SimpleOpStore::init(
            directory.path(),
            RootOperationData {
                root_commit_id: CommitId::from_bytes(&[0; 32]),
            },
        )?;
        let mut view = View::make_root(CommitId::from_bytes(&[0; 32]));
        view.wc_commit_ids
            .insert("retained-agent".into(), CommitId::from_bytes(&[0; 32]));
        let view_id = store.write_view(&view).await?;
        let mut operation = Operation::make_root(view_id.clone());
        operation.parents = vec![OperationId::from_bytes(&[0; 64])];
        operation.metadata.description = "retained undo operation".into();
        let operation_id = store.write_operation(&operation).await?;
        let objects = vec![
            c::export_operation(&store, &operation_id, c::Limits::default()).await?,
            c::export_view(&store, &view_id, c::Limits::default()).await?,
        ];
        let old = bundle(vec![], vec![], objects, 1)?;
        let next = bundle(vec![], vec![], vec![], 2)?;
        let error = prove_successor(&old, &next, Arc::new(()))
            .await
            .err()
            .context("missing OP/VIEW must refuse")?;
        assert!(
            error
                .to_string()
                .contains("omits retained OP/VIEW identity")
        );
        Ok(())
    }
    #[tokio::test]
    async fn private_coverage_exhaustion_refuses_instead_of_returning_partial_proof() -> Result<()>
    {
        let old_file = leaf(b"old root unavailable in successor");
        let next_file = leaf(b"successor shared leaf");
        let old = bundle(
            vec![old_file.clone()],
            vec![old_file.descriptor.key],
            vec![],
            1,
        )?;
        let mut objects = vec![next_file.clone()];
        let mut roots = vec![];
        for i in 0..=MAX_CAPTURE_COVERAGE_OBJECTS {
            let value = tree(&next_file, &format!("entry-{i}"))?;
            roots.push(value.descriptor.key);
            objects.push(value);
        }
        let next = bundle(objects, roots, vec![], 2)?;
        let error = prove_successor(&old, &next, Arc::new(()))
            .await
            .err()
            .context("bounded proof must refuse")?;
        assert!(error.to_string().contains("coverage object bound"));
        Ok(())
    }
}
