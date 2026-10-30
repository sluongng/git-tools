//! A verified root-selected inventory, not an object catalog or retention lease.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    path::Path,
    time::Duration,
};
use tinytree_client::{MetadataOutcome, NativeClient};
use tinytree_model::{Descriptor, Kind, Object, ObjectId, ObjectKey, TreeValue};
use tokio::time::Instant;

/// Create disposable download staging with owner-only Unix permissions from
/// creation, independently of the caller's umask. SDK trust checks still apply.
pub fn temporary_directory() -> std::io::Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("tinytree-inventory-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    builder.tempdir()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    pub max_objects: usize,
    pub max_bytes: u64,
    pub max_queue: usize,
    pub max_edges: u64,
    pub timeout_seconds: u64,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            max_objects: 100_000,
            max_bytes: 16 * 1024 * 1024 * 1024,
            max_queue: 100_000,
            max_edges: 1_000_000,
            timeout_seconds: 3600,
        }
    }
}
impl Budgets {
    pub fn valid(&self) -> bool {
        (1..=1_000_000).contains(&self.max_objects)
            && (1..=1024 * 1024 * 1024 * 1024).contains(&self.max_bytes)
            && (1..=1_000_000).contains(&self.max_queue)
            && (1..=10_000_000).contains(&self.max_edges)
            && (1..=86400).contains(&self.timeout_seconds)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectedRef {
    pub name: String,
    pub generation: u64,
    pub head: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Largest {
    pub id: String,
    pub size_bytes: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KindTotals {
    pub objects: u64,
    pub bytes: u64,
    pub largest: Option<Largest>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub scope: String,
    pub complete: bool,
    pub error: Option<String>,
    pub budgets: Budgets,
    pub initial_refs: Vec<SelectedRef>,
    pub final_refs: Vec<SelectedRef>,
    pub by_kind: BTreeMap<String, KindTotals>,
    pub unique_objects: u64,
    pub unique_bytes: u64,
    pub reference_edges: u64,
    pub commit_parent_edges: u64,
    pub commit_predecessor_edges: u64,
    pub max_metadata_bytes: u64,
}
impl Report {
    pub fn failed(budgets: Budgets, error: &str) -> Self {
        Self {
            scope: "explicit-reference-reachable-closure".into(),
            complete: false,
            error: Some(error.into()),
            budgets,
            initial_refs: vec![],
            final_refs: vec![],
            by_kind: BTreeMap::new(),
            unique_objects: 0,
            unique_bytes: 0,
            reference_edges: 0,
            commit_parent_edges: 0,
            commit_predecessor_edges: 0,
            max_metadata_bytes: 0,
        }
    }
}
fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn kind(k: Kind) -> &'static str {
    match k {
        Kind::File => "file",
        Kind::Tree => "tree",
        Kind::Commit => "commit",
        Kind::Symlink => "symlink",
        Kind::CopyHistory => "copy_history",
    }
}
fn intrinsic(key: ObjectKey) -> bool {
    key.kind == Kind::Commit && key.id.0 == tinytree_model::CommitId::ROOT.0
}
fn rpc(code: tinytree_client::protocol::item_error::Code) -> String {
    format!("metadata_item_{code:?}")
}
fn refs(view: &tinytree_client::protocol::RepositoryView) -> Vec<SelectedRef> {
    let mut refs = view
        .references
        .iter()
        .map(|r| SelectedRef {
            name: r.name.clone(),
            generation: r.generation,
            head: r
                .commit
                .as_ref()
                .and_then(|c| c.key.as_ref())
                .map(|k| hex(&k.native_id)),
        })
        .collect::<Vec<_>>();
    refs.sort_by(|a, b| a.name.cmp(&b.name));
    refs
}
fn commit_key(d: &tinytree_client::protocol::ObjectDescriptor) -> Result<ObjectKey, String> {
    let key = d.key.as_ref().ok_or("invalid_head_descriptor")?;
    if key.kind != Kind::Commit as i32 {
        return Err("invalid_head_kind".into());
    }
    Ok(ObjectKey {
        kind: Kind::Commit,
        id: ObjectId::from_slice(&key.native_id).map_err(|_| "invalid_head_id")?,
    })
}
/// SDK-verified original bytes. The sink must finish handling this object
/// before the walker records its successful delivery in a checkpoint.
pub enum VerifiedObject {
    Metadata(tinytree_client::VerifiedMetadata),
    File(tinytree_client::VerifiedFile),
}
pub trait InventorySink {
    fn object(
        &mut self,
        value: VerifiedObject,
    ) -> impl std::future::Future<Output = Result<(), String>>;
    fn checkpoint(
        &mut self,
        value: Checkpoint,
    ) -> impl std::future::Future<Output = Result<(), String>>;
}
struct Discard;
impl InventorySink for Discard {
    async fn object(&mut self, _: VerifiedObject) -> Result<(), String> {
        Ok(())
    }
    async fn checkpoint(&mut self, _: Checkpoint) -> Result<(), String> {
        Ok(())
    }
}
/// Compact owner-local traversal progress, not a remote proof or retention lease.
/// Hex-packed keys are33bytes(kind+id); length records append8big-endian bytes.
/// No source object bytes or credentials are persisted here.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    version: u32,
    report: Report,
    discovered: String,
    frontier: String,
    expected: String,
    completed: String,
}
fn key_bytes(key: ObjectKey) -> Vec<u8> {
    let mut bytes = vec![key.kind as u8];
    bytes.extend_from_slice(&key.id.0);
    bytes
}
fn packed_keys(keys: impl Iterator<Item = ObjectKey>) -> String {
    keys.map(|key| hex(&key_bytes(key))).collect()
}
fn packed_lengths(values: &HashMap<ObjectKey, u64>) -> String {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| key_bytes(**key));
    entries
        .into_iter()
        .map(|(key, size)| {
            let mut bytes = key_bytes(*key);
            bytes.extend_from_slice(&size.to_be_bytes());
            hex(&bytes)
        })
        .collect()
}
fn unpack(value: &str, lengths: bool, maximum: usize) -> Result<Vec<(ObjectKey, u64)>, String> {
    let width = if lengths { 82 } else { 66 };
    if value.len() % width != 0
        || value.len() / width > maximum
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid_checkpoint".into());
    }
    value
        .as_bytes()
        .chunks_exact(width)
        .map(|row| {
            let bytes = row
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let key = ObjectKey {
                kind: Kind::try_from(bytes[0]).map_err(|_| "invalid_checkpoint")?,
                id: ObjectId::from_slice(&bytes[1..33]).map_err(|_| "invalid_checkpoint")?,
            };
            if intrinsic(key) {
                return Err("invalid_checkpoint".into());
            }
            Ok((
                key,
                if lengths {
                    u64::from_be_bytes(bytes[33..41].try_into().unwrap())
                } else {
                    0
                },
            ))
        })
        .collect()
}
struct Walk {
    discovered: HashSet<ObjectKey>,
    queue: VecDeque<ObjectKey>,
    expected: HashMap<ObjectKey, u64>,
    logical: HashMap<ObjectKey, u64>,
}
impl Walk {
    fn checkpoint(&self, report: &Report) -> Checkpoint {
        let mut discovered = self.discovered.iter().copied().collect::<Vec<_>>();
        discovered.sort_by_key(|key| key_bytes(*key));
        Checkpoint {
            version: 1,
            report: report.clone(),
            discovered: packed_keys(discovered.into_iter()),
            frontier: packed_keys(self.queue.iter().copied()),
            expected: packed_lengths(&self.expected),
            completed: packed_lengths(&self.logical),
        }
    }
    fn restore(checkpoint: Checkpoint, report: &Report) -> Result<Self, String> {
        if checkpoint.version != 1
            || checkpoint.report.budgets != report.budgets
            || checkpoint.report.initial_refs != report.initial_refs
            || checkpoint.report.unique_bytes > report.budgets.max_bytes
            || checkpoint.report.reference_edges > report.budgets.max_edges
            || checkpoint.report.complete
        {
            return Err("invalid_checkpoint".into());
        }
        let discovered = unpack(&checkpoint.discovered, false, report.budgets.max_objects)?;
        let queue = unpack(&checkpoint.frontier, false, report.budgets.max_queue)?;
        let expected = unpack(&checkpoint.expected, true, report.budgets.max_objects)?;
        let logical = unpack(&checkpoint.completed, true, report.budgets.max_objects)?;
        let walk = Self {
            discovered: discovered.iter().map(|(k, _)| *k).collect(),
            queue: queue.iter().map(|(k, _)| *k).collect(),
            expected: expected.iter().copied().collect(),
            logical: logical.iter().copied().collect(),
        };
        let queued = queue.iter().map(|(k, _)| *k).collect::<HashSet<_>>();
        if walk.discovered.len() != discovered.len()
            || queued.len() != queue.len()
            || walk.expected.len() != expected.len()
            || walk.logical.len() != logical.len()
            || walk.logical.len() as u64 != checkpoint.report.unique_objects
            || walk.discovered.len() != queued.len() + walk.logical.len()
            || queued
                .iter()
                .any(|key| !walk.discovered.contains(key) || walk.logical.contains_key(key))
            || walk
                .logical
                .keys()
                .chain(walk.expected.keys())
                .any(|key| !walk.discovered.contains(key))
            || walk
                .expected
                .iter()
                .any(|(key, size)| walk.logical.get(key).is_some_and(|actual| actual != size))
        {
            return Err("invalid_checkpoint".into());
        }
        Ok(walk)
    }
    fn enqueue(&mut self, key: ObjectKey, b: &Budgets) -> Result<(), String> {
        if intrinsic(key) || self.discovered.contains(&key) {
            return Ok(());
        }
        if self.discovered.len() >= b.max_objects {
            return Err("object_budget".into());
        }
        if self.queue.len() >= b.max_queue {
            return Err("queue_budget".into());
        }
        self.discovered.insert(key);
        self.queue.push_back(key);
        Ok(())
    }
    fn expect(&mut self, key: ObjectKey, size: u64, max_objects: usize) -> Result<(), String> {
        if !self.expected.contains_key(&key) && self.expected.len() >= max_objects {
            return Err("object_budget".into());
        }
        if self
            .expected
            .insert(key, size)
            .is_some_and(|old| old != size)
            || self.logical.get(&key).is_some_and(|actual| *actual != size)
        {
            return Err("tree_child_length_mismatch".into());
        }
        Ok(())
    }
    fn count(&mut self, d: Descriptor, logical: u64, r: &mut Report) -> Result<(), String> {
        if self
            .expected
            .get(&d.key)
            .is_some_and(|size| *size != logical)
        {
            return Err("tree_child_length_mismatch".into());
        }
        let bytes = r
            .unique_bytes
            .checked_add(d.size_bytes)
            .ok_or("byte_budget")?;
        if bytes > r.budgets.max_bytes {
            return Err("byte_budget".into());
        }
        r.unique_bytes = bytes;
        r.unique_objects += 1;
        let totals = r.by_kind.entry(kind(d.key.kind).into()).or_default();
        totals.objects += 1;
        totals.bytes += d.size_bytes;
        if totals
            .largest
            .as_ref()
            .is_none_or(|old| old.size_bytes < d.size_bytes)
        {
            totals.largest = Some(Largest {
                id: hex(&d.key.id.0),
                size_bytes: d.size_bytes,
            });
        }
        self.logical.insert(d.key, logical);
        Ok(())
    }
}
/// All object bytes are verified by the SDK. `complete` also requires the
/// explicitly selected refs to have unchanged generations/heads at the end.
/// No remote lease is acquired: disappearance or concurrent movement fails closed.
pub async fn inventory(
    client: &NativeClient,
    names: Vec<String>,
    temporary: &Path,
    budgets: Budgets,
) -> Report {
    inventory_with_sink(client, names, temporary, budgets, None, &mut Discard).await
}
/// Deliver verified objects through the same inventory traversal, with batched
/// resumable progress. A resumed checkpoint never chooses a newer source head.
pub async fn inventory_with_sink<S: InventorySink>(
    client: &NativeClient,
    names: Vec<String>,
    temporary: &Path,
    budgets: Budgets,
    checkpoint: Option<Checkpoint>,
    sink: &mut S,
) -> Report {
    let mut report = Report::failed(budgets, "not_started");
    if !report.budgets.valid() {
        report.error = Some("invalid_budgets".into());
        return report;
    }
    if names.is_empty()
        || names.len() > 256
        || names.iter().any(|n| n.is_empty() || n.len() > 4096)
        || names.iter().collect::<HashSet<_>>().len() != names.len()
    {
        report.error = Some("invalid_reference_selection".into());
        return report;
    }
    let deadline = Instant::now() + Duration::from_secs(report.budgets.timeout_seconds);
    let client = client.clone().with_operation_deadline(deadline);
    let result = tokio::time::timeout_at(
        deadline,
        run(
            &client,
            names,
            temporary,
            &mut report,
            deadline,
            checkpoint,
            sink,
        ),
    )
    .await;
    match result {
        Ok(Ok(())) => {
            report.complete = true;
            report.error = None;
        }
        Ok(Err(error)) => report.error = Some(error),
        Err(_) => report.error = Some("deadline_exceeded".into()),
    }
    report
}
async fn run<S: InventorySink>(
    client: &NativeClient,
    names: Vec<String>,
    temporary: &Path,
    report: &mut Report,
    deadline: Instant,
    checkpoint: Option<Checkpoint>,
    sink: &mut S,
) -> Result<(), String> {
    let initial = client
        .open(names.clone())
        .await
        .map_err(|e| format!("open_{:?}", e.code()))?;
    report.initial_refs = refs(&initial);
    let mut walk = Walk {
        discovered: HashSet::new(),
        queue: VecDeque::new(),
        expected: HashMap::new(),
        logical: HashMap::new(),
    };
    let mut heads = HashMap::new();
    for reference in &initial.references {
        let d = reference
            .commit
            .as_ref()
            .ok_or("selected_reference_absent")?;
        let key = commit_key(d)?;
        heads.insert(key, d.size_bytes);
        walk.enqueue(key, &report.budgets)?;
    }
    if let Some(checkpoint) = checkpoint {
        walk = Walk::restore(checkpoint.clone(), report)?;
        *report = checkpoint.report;
        report.error = None;
    }
    let mut delivered = 0usize;
    let mut last_checkpoint = Instant::now();
    while let Some(key) = walk.queue.pop_front() {
        if Instant::now() >= deadline {
            return Err("deadline_exceeded".into());
        }
        if key.kind == Kind::File {
            // The SDK enforces its configured finite per-file/disk caps before
            // streaming the file, and verifies exact length and native identity.
            if walk
                .expected
                .get(&key)
                .is_some_and(|n| *n > report.budgets.max_bytes - report.unique_bytes)
            {
                return Err("byte_budget".into());
            }
            let file = client
                .download_file(key.id, temporary)
                .await
                .map_err(|e| format!("file_download_{:?}", e.code()))?;
            let d = file.descriptor();
            if d.key != key {
                return Err("file_identity_mismatch".into());
            }
            sink.object(VerifiedObject::File(file)).await?;
            walk.count(d, d.size_bytes, report)?;
            delivered += 1;
            if delivered % 128 == 0 || last_checkpoint.elapsed() >= Duration::from_secs(5) {
                sink.checkpoint(walk.checkpoint(report)).await?;
                last_checkpoint = Instant::now();
            }
            continue;
        }
        let mut outcomes = client
            .read_objects(vec![key])
            .await
            .map_err(|e| format!("metadata_read_{:?}", e.code()))?;
        if outcomes.len() != 1 {
            return Err("metadata_outcome_count".into());
        }
        let value = match outcomes.pop().unwrap() {
            MetadataOutcome::Object(value) => value,
            MetadataOutcome::Error { code } => return Err(rpc(code)),
        };
        let d = value.descriptor();
        if d.key != key {
            return Err("metadata_identity_mismatch".into());
        }
        if heads.get(&key).is_some_and(|size| *size != d.size_bytes) {
            return Err("head_descriptor_mismatch".into());
        }
        report.max_metadata_bytes = report.max_metadata_bytes.max(d.size_bytes);
        let logical = match value.object() {
            Object::Symlink(target) => target.len() as u64,
            _ => d.size_bytes,
        };
        walk.count(d, logical, report)?;
        if let Object::Tree(tree) = value.object() {
            for entry in &tree.entries {
                match entry.value {
                    TreeValue::File { id, size_bytes, .. } => walk.expect(
                        ObjectKey {
                            kind: Kind::File,
                            id: ObjectId(id.0),
                        },
                        size_bytes,
                        report.budgets.max_objects,
                    )?,
                    TreeValue::Symlink { id, target_bytes } => walk.expect(
                        ObjectKey {
                            kind: Kind::Symlink,
                            id: ObjectId(id.0),
                        },
                        target_bytes,
                        report.budgets.max_objects,
                    )?,
                    _ => {}
                }
            }
        }
        if let Object::Commit(commit) = value.object() {
            report.commit_parent_edges += commit.parents.len() as u64;
            report.commit_predecessor_edges += commit.predecessors.len() as u64;
        }
        let children = value.object().references();
        let count = children.len() as u64;
        report.reference_edges = report
            .reference_edges
            .checked_add(count)
            .ok_or("edge_budget")?;
        if report.reference_edges > report.budgets.max_edges {
            return Err("edge_budget".into());
        }
        for child in children {
            walk.enqueue(child, &report.budgets)?;
        }
        sink.object(VerifiedObject::Metadata(value)).await?;
        delivered += 1;
        if delivered % 128 == 0 || last_checkpoint.elapsed() >= Duration::from_secs(5) {
            sink.checkpoint(walk.checkpoint(report)).await?;
            last_checkpoint = Instant::now();
        }
    }
    sink.checkpoint(walk.checkpoint(report)).await?;
    let final_view = client
        .open(names)
        .await
        .map_err(|e| format!("final_open_{:?}", e.code()))?;
    report.final_refs = refs(&final_view);
    if report.initial_refs != report.final_refs {
        return Err("references_changed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinytree_model::{
        ChangeId, CommitId, CopyHistory, CopyId, FileId, Tree, TreeEntry, TreeId,
    };
    use tinytree_store::{PublishRequest, Store};
    #[test]
    fn sdk_inventory_verifies_typed_closure_and_fails_incomplete_at_limits() {
        // This neutral fixture credential exists only inside this test process.
        unsafe {
            std::env::set_var(
                "TINYTREE_INVENTORY_FIXTURE_TOKEN",
                "inventory-fixture-token",
            );
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path(), Default::default()).unwrap();
            store.create_repository("example").unwrap();
            let mut leases = vec![];
            let mut expected = HashMap::new();
            let mut put = |d: Descriptor, bytes: &[u8]| {
                let receipt = store.put("example", d, bytes, 10).unwrap();
                leases.push(receipt.staging_lease);
                expected.insert(d.key, d.size_bytes);
            };
            let file = tinytree_model::describe_file(b"same file");
            put(file, b"same file");
            let oldfile = tinytree_model::describe_file(b"predecessor only");
            put(oldfile, b"predecessor only");
            let empty = tinytree_model::empty_tree();
            put(empty.descriptor, &empty.bytes);
            let prior_copy = tinytree_model::encode(
                &Object::CopyHistory(CopyHistory {
                    current_path: "origin".into(),
                    parents: vec![],
                    salt: vec![],
                }),
                Default::default(),
            )
            .unwrap();
            put(prior_copy.descriptor, &prior_copy.bytes);
            let copy = tinytree_model::encode(
                &Object::CopyHistory(CopyHistory {
                    current_path: "a".into(),
                    parents: vec![CopyId(prior_copy.descriptor.key.id.0)],
                    salt: vec![],
                }),
                Default::default(),
            )
            .unwrap();
            put(copy.descriptor, &copy.bytes);
            let link =
                tinytree_model::encode(&Object::Symlink("a".into()), Default::default()).unwrap();
            put(link.descriptor, &link.bytes);
            let tree = tinytree_model::encode(
                &Object::Tree(Tree {
                    entries: vec![
                        TreeEntry {
                            name: "a".into(),
                            value: TreeValue::File {
                                id: FileId(file.key.id.0),
                                size_bytes: file.size_bytes,
                                executable: false,
                                copy_id: Some(CopyId(copy.descriptor.key.id.0)),
                            },
                        },
                        TreeEntry {
                            name: "b".into(),
                            value: TreeValue::File {
                                id: FileId(file.key.id.0),
                                size_bytes: file.size_bytes,
                                executable: false,
                                copy_id: None,
                            },
                        },
                        TreeEntry {
                            name: "link".into(),
                            value: TreeValue::Symlink {
                                id: tinytree_model::SymlinkId(link.descriptor.key.id.0),
                                target_bytes: 1,
                            },
                        },
                    ],
                }),
                Default::default(),
            )
            .unwrap();
            put(tree.descriptor, &tree.bytes);
            let oldtree = tinytree_model::encode(
                &Object::Tree(Tree {
                    entries: vec![TreeEntry {
                        name: "previous".into(),
                        value: TreeValue::File {
                            id: FileId(oldfile.key.id.0),
                            size_bytes: oldfile.size_bytes,
                            executable: false,
                            copy_id: None,
                        },
                    }],
                }),
                Default::default(),
            )
            .unwrap();
            put(oldtree.descriptor, &oldtree.bytes);
            let commit = |id: u8, root: TreeId, parents, predecessors| {
                let mut c = tinytree_model::synthetic_root();
                c.parents = parents;
                c.predecessors = predecessors;
                c.change_id = ChangeId([id; 16]);
                c.root_tree.terms = if id == 3 {
                    vec![root, TreeId(empty.descriptor.key.id.0), root]
                } else {
                    vec![root]
                };
                c.root_tree.labels = vec![String::new(); c.root_tree.terms.len()];
                tinytree_model::encode(&Object::Commit(c), Default::default()).unwrap()
            };
            let base = commit(
                1,
                TreeId(empty.descriptor.key.id.0),
                vec![CommitId::ROOT],
                vec![],
            );
            put(base.descriptor, &base.bytes);
            let previous = commit(
                2,
                TreeId(oldtree.descriptor.key.id.0),
                vec![CommitId::ROOT],
                vec![],
            );
            put(previous.descriptor, &previous.bytes);
            let head = commit(
                3,
                TreeId(tree.descriptor.key.id.0),
                vec![CommitId(base.descriptor.key.id.0)],
                vec![CommitId(previous.descriptor.key.id.0)],
            );
            put(head.descriptor, &head.bytes);
            store
                .publish(
                    "example",
                    &PublishRequest {
                        idempotency_key: "fixture".into(),
                        reference_name: "main".into(),
                        expected_generation: 0,
                        new_commit: Some(CommitId(head.descriptor.key.id.0)),
                        staging_leases: leases,
                    },
                    10,
                )
                .unwrap();
            let service = tinytree_server::RepositoryService::new(
                store,
                Default::default(),
                vec!["example".into()],
                vec![
                    tinytree_server::Principal::new(
                        "fixture",
                        "inventory-fixture-token",
                        vec!["example".into()],
                        false,
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        tinytree_proto::v1::repository_server::RepositoryServer::new(service),
                    )
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = stopped.await;
                        },
                    )
                    .await
                    .unwrap();
            });
            let client = NativeClient::connect(tinytree_client::Config {
                endpoint: format!("http://{address}"),
                repository_id: "example".into(),
                token_env: Some("TINYTREE_INVENTORY_FIXTURE_TOKEN".into()),
                compression: false,
                ..Default::default()
            })
            .await
            .unwrap();
            let temp = temporary_directory().unwrap();
            let report = inventory(
                &client,
                vec!["main".into()],
                temp.path(),
                Budgets::default(),
            )
            .await;
            assert!(report.complete, "{:?}", report.error);
            assert_eq!(report.unique_objects, expected.len() as u64);
            assert_eq!(report.unique_bytes, expected.values().sum::<u64>());
            assert_eq!(report.by_kind["file"].objects, 2);
            assert_eq!(report.by_kind["commit"].objects, 3);
            assert_eq!(report.commit_predecessor_edges, 1);
            assert_eq!(report.reference_edges, 13); // Includes negative term, duplicate positive term and copy parent.
            assert_eq!(report.initial_refs, report.final_refs);
            assert_eq!(
                report.initial_refs[0].head,
                Some(hex(&head.descriptor.key.id.0))
            );
            // The sink ACK is distinct from a completed inventory report. A
            // durable EOF checkpoint can survive interruption before final ref
            // stability/readback, then resume without redelivering ACKed bytes.
            struct RecordingSink {
                delivered: HashSet<ObjectKey>,
                saved: Option<Checkpoint>,
                interrupt: bool,
            }
            impl InventorySink for RecordingSink {
                async fn object(&mut self, value: VerifiedObject) -> Result<(), String> {
                    let descriptor = match value {
                        VerifiedObject::Metadata(value) => value.descriptor(),
                        VerifiedObject::File(value) => value.descriptor(),
                    };
                    assert!(
                        self.delivered.insert(descriptor.key),
                        "one typed delivery per walk"
                    );
                    Ok(())
                }
                async fn checkpoint(&mut self, value: Checkpoint) -> Result<(), String> {
                    self.saved = Some(value);
                    if self.interrupt {
                        self.interrupt = false;
                        return Err("fixture_checkpoint_interrupted".into());
                    }
                    Ok(())
                }
            }
            let mut sink = RecordingSink {
                delivered: HashSet::new(),
                saved: None,
                interrupt: true,
            };
            let interrupted = inventory_with_sink(
                &client,
                vec!["main".into()],
                temp.path(),
                Budgets::default(),
                None,
                &mut sink,
            )
            .await;
            assert!(!interrupted.complete);
            assert_eq!(
                interrupted.error.as_deref(),
                Some("fixture_checkpoint_interrupted")
            );
            assert_eq!(sink.delivered, expected.keys().copied().collect());
            let durable = serde_json::to_vec(sink.saved.as_ref().unwrap()).unwrap();
            let checkpoint: Checkpoint = serde_json::from_slice(&durable).unwrap();
            let mut resumed_sink = RecordingSink {
                delivered: HashSet::new(),
                saved: None,
                interrupt: false,
            };
            let resumed = inventory_with_sink(
                &client,
                vec!["main".into()],
                temp.path(),
                Budgets::default(),
                Some(checkpoint),
                &mut resumed_sink,
            )
            .await;
            assert!(resumed.complete, "{:?}", resumed.error);
            assert!(
                resumed_sink.delivered.is_empty(),
                "ACKed checkpoint objects must not be redelivered"
            );
            assert_eq!(resumed.unique_objects, report.unique_objects);
            assert_eq!(resumed.unique_bytes, report.unique_bytes);
            assert_eq!(resumed.reference_edges, report.reference_edges);
            let mut corrupt: Checkpoint = serde_json::from_slice(&durable).unwrap();
            corrupt.frontier = "00".into();
            let rejected = inventory_with_sink(
                &client,
                vec!["main".into()],
                temp.path(),
                Budgets::default(),
                Some(corrupt),
                &mut resumed_sink,
            )
            .await;
            assert!(!rejected.complete);
            assert_eq!(rejected.error.as_deref(), Some("invalid_checkpoint"));
            for (objects, bytes, queue, error) in [
                (1, 1_000_000, 100, "object_budget"),
                (100, 1, 100, "byte_budget"),
                (100, 1_000_000, 1, "queue_budget"),
            ] {
                let report = inventory(
                    &client,
                    vec!["main".into()],
                    temp.path(),
                    Budgets {
                        max_objects: objects,
                        max_bytes: bytes,
                        max_queue: queue,
                        ..Default::default()
                    },
                )
                .await;
                assert!(!report.complete);
                assert_eq!(report.error.as_deref(), Some(error));
            }
            let absent = inventory(
                &client,
                vec!["absent".into()],
                temp.path(),
                Budgets::default(),
            )
            .await;
            assert!(!absent.complete);
            assert_eq!(absent.error.as_deref(), Some("selected_reference_absent"));
            stop.send(()).unwrap();
            task.await.unwrap();
        });
    }
}
