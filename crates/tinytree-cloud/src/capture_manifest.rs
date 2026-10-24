//! Exact portable capture/export DATA binding. A declared writer policy is not
//! physical frontend quiescence, authenticated retention, or restore authority.
use crate as c;
use c::{BlobDigest, Error, ManifestId, Result};
use jj_lib::object_id::ObjectId as _;
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model as m;
const HEADER: usize = 10;
const FOOTER: usize = 72;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CaptureDataId(pub [u8; 32]);
impl CaptureDataId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(c::data_digest(bytes))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureDescriptor {
    pub id: CaptureDataId,
    pub selected: BlobDigest,
}
impl CaptureDescriptor {
    /// Identity/descriptor only; this does not decode or prove a live capture.
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            id: CaptureDataId::of(bytes),
            selected: BlobDigest::of(bytes),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestV3 {
    pub history: c::ManifestV2,
    pub capture: CaptureDescriptor,
}
impl ManifestV3 {
    pub fn indexed(&self) -> &c::ManifestV2 {
        &self.history
    }
}
#[derive(Clone, Copy, Debug)]
pub struct CaptureBindingLimits {
    /// Total additional working charge, including capture decoding and validation.
    /// Callers subtract already-live objects/artifacts/proof from their one pool.
    pub max_bytes: usize,
    pub max_work: u64,
}
impl Default for CaptureBindingLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_work: 10_000_000,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ManifestV3Limits {
    pub history: c::ManifestV2Limits,
    pub capture: c::CaptureDataLimits,
    pub binding: CaptureBindingLimits,
}
impl Default for ManifestV3Limits {
    fn default() -> Self {
        Self {
            history: Default::default(),
            capture: Default::default(),
            binding: Default::default(),
        }
    }
}
fn invalid(why: &'static str) -> Error {
    Error::Invalid(why)
}
fn limit() -> Error {
    Error::Limit("capture manifest binding")
}
impl ManifestV3Limits {
    pub fn validate(self) -> Result<()> {
        self.history.validate()?;
        if self.history.recovery.max_manifest_bytes < HEADER + FOOTER
            || self.capture.max_bytes == 0
            || self.capture.max_bytes > 64 * 1024 * 1024
            || self.capture.max_decode_bytes == 0
            || self.capture.max_decode_bytes > 1024 * 1024 * 1024
            || self.capture.max_operations == 0
            || self.capture.max_parent_edges == 0
            || self.capture.max_roots == 0
            || self.capture.max_blobs == 0
            || self.binding.max_bytes == 0
            || self.binding.max_bytes > 1024 * 1024 * 1024
            || self.binding.max_work == 0
        {
            return Err(invalid("capture manifest limits"));
        }
        Ok(())
    }
    fn descriptor(self, capture: CaptureDescriptor) -> Result<()> {
        self.validate()?;
        if capture.selected.size_bytes == 0 {
            return Err(invalid("empty capture attachment"));
        }
        if capture.selected.size_bytes > self.capture.max_bytes as u64 {
            return Err(limit());
        }
        Ok(())
    }
    fn history_limit(self) -> c::ManifestV2Limits {
        let mut history = self.history;
        history.recovery.max_manifest_bytes -= FOOTER;
        history
    }
}
/// Version 3 preserves the V2 body layout and appends exactly one fixed capture
/// descriptor. Version-aware old readers reject it instead of ignoring attachment.
pub fn encode_manifest_v3(value: &ManifestV3, limits: ManifestV3Limits) -> Result<Vec<u8>> {
    limits.descriptor(value.capture)?;
    // Existing V2 encoding plus the final V3 output coexist briefly. Reserve the
    // configured upper bound before either encoding/copy can allocate.
    if limits
        .history
        .recovery
        .max_manifest_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(4096))
        .is_none_or(|n| n > limits.history.recovery.max_decode_bytes)
    {
        return Err(limit());
    }
    let prefix = c::encode_manifest_v2(&value.history, limits.history_limit())?;
    let mut bytes = Vec::with_capacity(prefix.len().checked_add(FOOTER).ok_or_else(limit)?);
    bytes.extend_from_slice(&prefix);
    bytes[8..10].copy_from_slice(&3u16.to_le_bytes());
    bytes.extend_from_slice(&value.capture.id.0);
    bytes.extend_from_slice(&value.capture.selected.blake3);
    bytes.extend_from_slice(&value.capture.selected.size_bytes.to_le_bytes());
    Ok(bytes)
}
pub fn decode_manifest_v3(
    bytes: &[u8],
    expected: ManifestId,
    limits: ManifestV3Limits,
) -> Result<ManifestV3> {
    limits.validate()?;
    if bytes.len() > limits.history.recovery.max_manifest_bytes {
        return Err(limit());
    }
    if bytes.len() < HEADER + FOOTER
        || &bytes[..8] != b"TTCLOUD\0"
        || bytes[8..10] != 3u16.to_le_bytes()
        || ManifestId::of(bytes) != expected
    {
        return Err(invalid("capture manifest identity or version"));
    }
    let split = bytes.len() - FOOTER;
    let capture = CaptureDescriptor {
        id: CaptureDataId(bytes[split..split + 32].try_into().expect("fixed footer")),
        selected: BlobDigest {
            blake3: bytes[split + 32..split + 64]
                .try_into()
                .expect("fixed footer"),
            size_bytes: u64::from_le_bytes(bytes[split + 64..].try_into().expect("fixed footer")),
        },
    };
    limits.descriptor(capture)?;
    let mut history_limits = limits.history_limit();
    history_limits.recovery.max_decode_bytes = history_limits
        .recovery
        .max_decode_bytes
        .checked_sub(split.checked_add(4096).ok_or_else(limit)?)
        .ok_or_else(limit)?;
    history_limits.validate()?;
    let mut prefix = bytes[..split].to_vec();
    prefix[8..10].copy_from_slice(&2u16.to_le_bytes());
    let history = c::decode_manifest_v2(&prefix, ManifestId::of(&prefix), history_limits)?;
    Ok(ManifestV3 { history, capture })
}
struct Budget {
    bytes: usize,
    work: u64,
    limits: CaptureBindingLimits,
}
impl Budget {
    fn bytes(&mut self, n: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(n)
            .filter(|n| *n <= self.limits.max_bytes)
            .ok_or_else(limit)?;
        Ok(())
    }
    fn work(&mut self, n: usize) -> Result<()> {
        self.work = self
            .work
            .checked_add(n as u64)
            .filter(|n| *n <= self.limits.max_work)
            .ok_or_else(limit)?;
        Ok(())
    }
}
fn hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2 {
        return Err(invalid("capture identity width"));
    }
    let nibble = |b: u8| match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => Err(invalid("capture identity encoding")),
    };
    let mut out = [0; N];
    for (slot, pair) in out.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *slot = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    Ok(out)
}
fn normalized_labels(labels: &[String]) -> &[String] {
    if labels.iter().all(String::is_empty) {
        &[]
    } else {
        labels
    }
}
fn remote_intents(
    data: &c::CaptureData,
    id: CaptureDataId,
    maximum: usize,
    budget: &mut Budget,
) -> Result<Vec<c::IntentRoots>> {
    let mut roots = BTreeSet::new();
    if let Some(remote) = &data.remote {
        if remote.observations.len() > 128 || remote.archived_pushes.len() > 128 {
            return Err(limit());
        }
        for head in remote
            .observations
            .iter()
            .map(|r| &r.head)
            .chain(remote.archived_pushes.iter().map(|r| &r.head))
        {
            budget.work(1)?;
            if let Some(head) = head {
                let id = hex::<32>(head)?;
                if id == [0; 32] {
                    return Err(invalid("intrinsic remote head"));
                }
                if !roots.contains(&id) {
                    if roots.len() >= maximum {
                        return Err(limit());
                    }
                    budget.bytes(128)?;
                    roots.insert(id);
                }
            }
        }
    }
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    budget.bytes(
        roots
            .len()
            .checked_mul(std::mem::size_of::<m::ObjectKey>())
            .ok_or_else(limit)?,
    )?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-capture-recovery-intent-v1\0");
    hash.update(&id.0);
    Ok(vec![c::IntentRoots {
        kind: c::IntentKind::Recovery,
        id: *hash.finalize().as_bytes(),
        roots: roots
            .into_iter()
            .map(|id| m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id),
            })
            .collect(),
    }])
}
/// Supplemental roots come only from actual portable remote observations and
/// archived heads; archived records are never restored as active retry intents.
pub fn capture_recovery_intents(
    data: &c::CaptureData,
    id: CaptureDataId,
    limits: CaptureBindingLimits,
) -> Result<Vec<c::IntentRoots>> {
    if limits.max_bytes == 0 || limits.max_work == 0 {
        return Err(limit());
    }
    let mut budget = Budget {
        bytes: 0,
        work: 0,
        limits,
    };
    remote_intents(data, id, 100_000, &mut budget)
}
/// Structural capture/export binding only. This type cannot create the opaque
/// host QuiescentCapture and does not prove native payload closure or retention.
pub struct VerifiedCaptureManifest<'a> {
    manifest: &'a ManifestV3,
    data: c::CaptureData,
    work: u64,
    _objects: &'a [c::VerifiedObject],
    _artifacts: &'a [c::VerifiedIndexArtifact],
}
impl VerifiedCaptureManifest<'_> {
    pub fn manifest(&self) -> &ManifestV3 {
        self.manifest
    }
    pub fn capture_data(&self) -> &c::CaptureData {
        &self.data
    }
    /// Deterministic validation work charged, including raw decode/hash bytes.
    pub fn binding_work(&self) -> u64 {
        self.work
    }
    pub fn native_roots(&self) -> &[m::ObjectKey] {
        &self.manifest.history.recovery.native_roots
    }
}
pub fn verify_capture_export_binding<'a>(
    manifest: &'a ManifestV3,
    bytes: &[u8],
    objects: &'a [c::VerifiedObject],
    artifacts: &'a [c::VerifiedIndexArtifact],
    graph: &c::VerifiedIndexGraph<'_>,
    limits: ManifestV3Limits,
) -> Result<VerifiedCaptureManifest<'a>> {
    limits.descriptor(manifest.capture)?;
    if bytes.len() > limits.capture.max_bytes
        || bytes
            .len()
            .checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .is_none_or(|n| n > limits.capture.max_decode_bytes)
    {
        return Err(limit());
    }
    let mut budget = Budget {
        bytes: 0,
        work: 0,
        limits: limits.binding,
    };
    // Charge before hashing, JSON decoding or canonical re-encoding. The
    // decoder's conservative envelope covers its simultaneously live trees.
    budget.work(bytes.len())?;
    let decoded = bytes
        .len()
        .checked_mul(128)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(limit)?;
    budget.bytes(decoded)?;
    validate_history_shape(&manifest.history, limits.history, &mut budget)?;
    if manifest.capture != CaptureDescriptor::of(bytes) {
        return Err(invalid("selected capture descriptor mismatch"));
    }
    let data = c::decode_data(bytes, manifest.capture.id.0, &limits.capture)
        .map_err(|_| invalid("canonical capture data mismatch"))?;
    if data.policy != c::CapturePolicy::HostQuiescentV1 {
        return Err(invalid("capture declared writer policy"));
    }
    let records = data
        .operations
        .len()
        .checked_add(data.blobs.len())
        .and_then(|n| n.checked_add(objects.len()))
        .and_then(|n| n.checked_add(artifacts.len()))
        .ok_or_else(limit)?;
    budget.bytes(records.checked_mul(4096).ok_or_else(limit)?)?;
    budget.work(records)?;
    let recovery = &manifest.history.recovery;
    if recovery.operation_heads.len() != data.operation_heads.len()
        || !recovery
            .operation_heads
            .iter()
            .zip(&data.operation_heads)
            .all(|(id, text)| hex::<64>(text).is_ok_and(|found| *id == found))
    {
        return Err(invalid("capture selected heads mismatch"));
    }
    let mut undo = recovery.undo_operations.iter();
    for op in data
        .operations
        .iter()
        .skip(1)
        .filter(|op| data.operation_heads.binary_search(&op.operation).is_err())
    {
        if undo.next() != Some(&hex::<64>(&op.operation)?) {
            return Err(invalid("capture retained undo inventory mismatch"));
        }
    }
    if undo.next().is_some() || !recovery.intent_operations.is_empty() {
        return Err(invalid("capture retained operation roots mismatch"));
    }
    let wc = &data.working_copy;
    if recovery.working_copy.workspace_name != wc.workspace
        || recovery.working_copy.wc_operation != hex::<64>(&wc.wc_operation)?
        || recovery.working_copy.snapshot_operation != hex::<64>(&wc.snapshot_operation)?
        || recovery.working_copy.snapshot_commit != wc.snapshot_commit
        || !recovery
            .working_copy
            .tree
            .terms
            .iter()
            .map(|id| id.0)
            .eq(wc.tree.terms.iter().copied())
        || normalized_labels(&recovery.working_copy.tree.labels)
            != normalized_labels(&wc.tree.labels)
    {
        return Err(invalid("capture working-copy checkpoint mismatch"));
    }
    let mut operations = BTreeMap::new();
    let mut views = BTreeMap::new();
    let mut selected = BTreeMap::new();
    let mut blobs = BTreeMap::<(u8, [u8; 64]), BlobDigest>::new();
    let mut edges = recovery
        .intent_roots
        .iter()
        .try_fold(wc.tree.terms.len(), |n, i| sum(n, i.roots.len()))?;
    for object in objects {
        let deps = object.dependencies();
        edges = sum(
            edges,
            sum(
                deps.operations.len(),
                sum(deps.views.len(), deps.native_commits.len())?,
            )?,
        )?;
        if edges > limits.history.recovery.max_edges {
            return Err(limit());
        }
        budget.work(sum(
            deps.operations.len(),
            sum(deps.views.len(), deps.native_commits.len())?,
        )?)?;
        budget.work(object.payload().map_or(0, |p| p.len()))?;
        selected.insert(
            object.id(),
            object
                .digest()
                .ok_or_else(|| invalid("intrinsic selected object"))?,
        );
        let (kind, id) = match object.id() {
            c::ObjectId::Operation(id) => (1, id),
            c::ObjectId::View(id) => (2, id),
        };
        if blobs
            .insert(
                (kind, id),
                object
                    .digest()
                    .ok_or_else(|| invalid("capture selected object descriptor"))?,
            )
            .is_some()
        {
            return Err(invalid("duplicate capture object"));
        }
        if let Some(view) = object.view() {
            views.insert(id, view);
        }
        if let Some(operation) = object.operation() {
            if operations.insert(id, operation).is_some() {
                return Err(invalid("duplicate capture operation"));
            }
        }
    }
    if recovery.members.len() != selected.len()
        || recovery
            .members
            .iter()
            .any(|member| selected.get(&member.id) != Some(&member.selected))
    {
        return Err(invalid("capture exact selected manifest members"));
    }
    let mut referenced_views = BTreeSet::new();
    for operation in operations.values() {
        let id: [u8; 64] = operation
            .view_id
            .as_bytes()
            .try_into()
            .map_err(|_| invalid("capture view width"))?;
        if id != [0; 64] {
            referenced_views.insert(id);
        }
    }
    if !views.keys().copied().eq(referenced_views) {
        return Err(invalid("capture exact view coverage"));
    }
    if operations.len().checked_add(1) != Some(data.operations.len()) {
        return Err(invalid("capture operation coverage"));
    }
    for record in data.operations.iter().skip(1) {
        let operation = operations
            .get(&hex::<64>(&record.operation)?)
            .ok_or_else(|| invalid("capture actual operation missing"))?;
        budget.work(operation.parents.len().checked_add(1).ok_or_else(limit)?)?;
        if operation.view_id.as_bytes() != hex::<64>(&record.view)?
            || operation.parents.len() != record.parents.len()
            || !operation
                .parents
                .iter()
                .zip(&record.parents)
                .all(|(actual, text)| hex::<64>(text).is_ok_and(|id| actual.as_bytes() == id))
        {
            return Err(invalid("capture actual view or ordered parents mismatch"));
        }
    }
    if recovery.working_copy.snapshot_operation == [0; 64] {
        if recovery.working_copy.wc_operation != [0; 64]
            || wc.snapshot_commit != [0; 32]
            || recovery.working_copy.tree != m::synthetic_root().root_tree
        {
            return Err(invalid("intrinsic working-copy checkpoint"));
        }
    } else {
        let op = operations
            .get(&recovery.working_copy.snapshot_operation)
            .ok_or_else(|| invalid("snapshot operation missing"))?;
        let view_id: [u8; 64] = op
            .view_id
            .as_bytes()
            .try_into()
            .map_err(|_| invalid("snapshot view width"))?;
        let view = views
            .get(&view_id)
            .ok_or_else(|| invalid("snapshot view missing"))?;
        if view
            .wc_commit_ids
            .iter()
            .find(|(name, _)| name.as_str() == wc.workspace)
            .map(|(_, id)| id.as_bytes())
            != Some(wc.snapshot_commit.as_slice())
        {
            return Err(invalid("selected workspace snapshot mismatch"));
        }
    }
    if manifest.history.operation_indexes.len() != data.indexes.len()
        || !manifest
            .history
            .operation_indexes
            .iter()
            .zip(&data.indexes)
            .all(|(actual, record)| {
                actual.root.0 == record.root
                    && hex::<64>(&record.operation).is_ok_and(|id| actual.operation == id)
            })
    {
        return Err(invalid("capture exact operation index association"));
    }
    for artifact in artifacts {
        let descriptor = artifact.descriptor();
        let (kind, id) = match descriptor.id {
            c::IndexArtifactId::Root(id) => (3, id.0),
            c::IndexArtifactId::Node(id) => (4, id.0),
        };
        let mut key = [0; 64];
        key[..32].copy_from_slice(&id);
        if blobs.insert((kind, key), descriptor.selected).is_some() {
            return Err(invalid("duplicate capture artifact"));
        }
    }
    if blobs.len() != data.blobs.len() {
        return Err(invalid("capture raw blob inventory count"));
    }
    for record in &data.blobs {
        let mut id = [0; 64];
        if record.kind <= 2 {
            id = hex::<64>(&record.id)?;
        } else {
            id[..32].copy_from_slice(&hex::<32>(&record.id)?);
        }
        if blobs.remove(&(record.kind, id))
            != Some(BlobDigest {
                blake3: record.blake3,
                size_bytes: record.size,
            })
        {
            return Err(invalid("capture exact raw blob descriptor"));
        }
    }
    let intents = remote_intents(
        &data,
        manifest.capture.id,
        limits.history.recovery.max_native_roots,
        &mut budget,
    )?;
    if recovery.intent_roots != intents {
        return Err(invalid("capture deterministic recovery roots mismatch"));
    }
    let mut roots = BTreeSet::<(u8, [u8; 32])>::new();
    for object in objects {
        for id in &object.dependencies().native_commits {
            add_root(&mut roots, m::Kind::Commit, *id, limits, &mut budget)?;
        }
    }
    add_root(
        &mut roots,
        m::Kind::Commit,
        wc.snapshot_commit,
        limits,
        &mut budget,
    )?;
    for tree in &wc.tree.terms {
        add_root(&mut roots, m::Kind::Tree, *tree, limits, &mut budget)?;
    }
    // Immutable content-addressed roots bind this exact supplied closure to the
    // borrowed proof. Reapply the caller's current policy without rebuilding or
    // cloning every snapshot merely to collect its IDs.
    validate_graph_inventory(
        &manifest.history,
        artifacts,
        graph,
        &mut budget,
        &mut roots,
        limits,
    )?;
    for intent in intents {
        for root in intent.roots {
            add_root(&mut roots, root.kind, root.id.0, limits, &mut budget)?;
        }
    }
    if !data
        .roots
        .iter()
        .map(|root| (root.kind, root.id))
        .eq(roots.iter().copied())
        || !recovery
            .native_roots
            .iter()
            .map(|root| (root.kind as u8, root.id.0))
            .eq(roots.iter().copied())
    {
        return Err(invalid("independent capture native root union mismatch"));
    }
    Ok(VerifiedCaptureManifest {
        manifest,
        data,
        work: budget.work,
        _objects: objects,
        _artifacts: artifacts,
    })
}

fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(limit)
}
fn sum(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(limit)
}
/// Shape validation allocates only its bounded association maps, not a second
/// encoded manifest. Reserve its conservative wire/model envelope first.
fn validate_history_shape(
    value: &c::ManifestV2,
    limits: c::ManifestV2Limits,
    budget: &mut Budget,
) -> Result<()> {
    let r = &value.recovery;
    let records = [
        r.operation_heads.len(),
        r.undo_operations.len(),
        r.intent_operations.len(),
        r.members.len(),
        r.native_roots.len(),
        r.parents.len(),
        value.operation_indexes.len(),
        value.index_roots.len(),
        r.working_copy.tree.terms.len(),
    ]
    .into_iter()
    .try_fold(0usize, sum)?;
    let mut upper = sum(4096, mul(records, 256)?)?;
    upper = sum(upper, r.working_copy.workspace_name.len())?;
    for label in &r.working_copy.tree.labels {
        upper = sum(upper, sum(8, label.len())?)?;
    }
    for intent in &r.intent_roots {
        upper = sum(upper, sum(64, mul(intent.roots.len(), 40)?)?)?;
    }
    budget.work(upper)?;
    // The V2 shape checker bounds sixteen bytes of model/scratch per wire byte
    // plus 512 per indexed association/member; this upper estimate dominates it.
    let peak = sum(mul(upper, 16)?, mul(records, 512)?)?;
    if sum(budget.bytes, peak)? > budget.limits.max_bytes {
        return Err(limit());
    }
    c::manifest::shape_v2(value, limits)
}
fn add_root(
    roots: &mut BTreeSet<(u8, [u8; 32])>,
    kind: m::Kind,
    id: [u8; 32],
    limits: ManifestV3Limits,
    budget: &mut Budget,
) -> Result<()> {
    budget.work(1)?;
    if kind == m::Kind::Commit && id == [0; 32] {
        return Ok(());
    }
    if !roots.contains(&(kind as u8, id)) {
        if roots.len() >= limits.history.recovery.max_native_roots {
            return Err(limit());
        }
        // Conservative B-tree entry/node admission, not an RSS assertion.
        budget.bytes(1024)?;
        roots.insert((kind as u8, id));
    }
    Ok(())
}
struct GraphWalk<'a, 'b> {
    table: &'a BTreeMap<c::IndexArtifactId, &'a c::VerifiedIndexArtifact>,
    visited: &'b mut BTreeSet<c::IndexArtifactId>,
    roots: &'b mut BTreeSet<(u8, [u8; 32])>,
    budget: &'b mut Budget,
    limits: ManifestV3Limits,
    steps: &'b mut usize,
    encoded: usize,
    memory: usize,
    edges: usize,
}
impl GraphWalk<'_, '_> {
    fn tick(&mut self, n: usize) -> Result<()> {
        *self.steps = sum(*self.steps, n)?;
        if *self.steps > self.limits.history.artifacts.max_graph_steps {
            return Err(limit());
        }
        self.budget.work(n)
    }
    fn node(&mut self, id: c::IndexNodeId, depth: usize) -> Result<()> {
        if depth > 64 {
            return Err(invalid("capture index depth"));
        }
        self.tick(1)?;
        let key = c::IndexArtifactId::Node(id);
        let artifact = *self
            .table
            .get(&key)
            .ok_or_else(|| invalid("capture index child missing"))?;
        self.visited.insert(key);
        let c::IndexArtifact::Node(node) = artifact.artifact() else {
            return Err(invalid("capture index node kind"));
        };
        match &node.payload {
            c::IndexNodePayload::Branch(children) => {
                for child in children {
                    self.node(child.node, depth + 1)?;
                }
            }
            c::IndexNodePayload::Anchors(ids) => {
                self.tick(ids.len())?;
                self.encoded = sum(self.encoded, mul(ids.len(), 32)?)?;
                for id in ids {
                    add_root(self.roots, m::Kind::Commit, id.0, self.limits, self.budget)?;
                }
            }
            c::IndexNodePayload::Drafts(drafts) => {
                self.tick(drafts.len())?;
                for draft in drafts {
                    let s = self.limits.history.artifacts.snapshot;
                    if draft.parents.len() > s.max_parents
                        || draft.descriptor.size_bytes > s.max_metadata_bytes
                    {
                        return Err(limit());
                    }
                    self.tick(draft.parents.len())?;
                    self.edges = sum(self.edges, draft.parents.len())?;
                    self.encoded = sum(self.encoded, sum(70, mul(draft.parents.len(), 32)?)?)?;
                    self.memory = sum(self.memory, mul(draft.parents.len(), 32)?)?;
                    if self.edges > s.max_edges {
                        return Err(limit());
                    }
                    add_root(
                        self.roots,
                        m::Kind::Commit,
                        draft.commit().0,
                        self.limits,
                        self.budget,
                    )?;
                }
            }
        }
        let s = self.limits.history.artifacts.snapshot;
        if self.encoded > s.max_encoded_bytes || self.memory > s.max_decode_bytes {
            return Err(limit());
        }
        Ok(())
    }
}
fn validate_graph_inventory(
    value: &c::ManifestV2,
    artifacts: &[c::VerifiedIndexArtifact],
    graph: &c::VerifiedIndexGraph<'_>,
    budget: &mut Budget,
    roots: &mut BTreeSet<(u8, [u8; 32])>,
    limits: ManifestV3Limits,
) -> Result<()> {
    let policy = limits.history.artifacts;
    if graph.roots() != value.index_roots {
        return Err(invalid("capture graph selected roots mismatch"));
    }
    if artifacts.len() > policy.max_artifacts || value.index_roots.len() > policy.max_snapshots {
        return Err(limit());
    }
    let mut table = BTreeMap::new();
    let mut raw = 0u64;
    let mut retained = sum(
        4096,
        sum(mul(artifacts.len(), 512)?, policy.snapshot.max_decode_bytes)?,
    )?;
    for artifact in artifacts {
        let d = artifact.descriptor();
        if d.selected.size_bytes > policy.max_artifact_bytes as u64 {
            return Err(limit());
        }
        raw = raw
            .checked_add(d.selected.size_bytes)
            .filter(|n| *n <= policy.max_total_artifact_bytes)
            .ok_or_else(limit)?;
        retained = sum(retained, artifact.retained_size_upper_bound()?)?;
        if retained > policy.max_decode_bytes {
            return Err(limit());
        }
        budget.work(usize::try_from(d.selected.size_bytes).map_err(|_| limit())?)?;
        if table.insert(d.id, artifact).is_some() {
            return Err(invalid("duplicate capture graph artifact"));
        }
    }
    // Root IDs commit canonical child IDs recursively. Verified artifacts are
    // immutable and content-addressed, so an exact root+raw match to the proof
    // and exact supplied closure cannot substitute a different logical graph.
    let mut visited = BTreeSet::new();
    let mut steps = 0;
    for descriptor in &value.index_roots {
        let key = c::IndexArtifactId::Root(descriptor.id);
        let a = *table
            .get(&key)
            .ok_or_else(|| invalid("capture graph root missing"))?;
        if a.descriptor().selected != descriptor.selected {
            return Err(invalid("capture graph root raw descriptor mismatch"));
        }
        let c::IndexArtifact::Root(root) = a.artifact() else {
            return Err(invalid("capture graph root kind"));
        };
        let s = policy.snapshot;
        if root.anchor_count == 0
            || root.anchor_count as usize > s.max_roots
            || root.draft_count as usize > s.max_drafts
        {
            return Err(limit());
        }
        let memory = sum(
            4096 + std::mem::size_of::<c::PortableIndexSnapshot>(),
            sum(
                mul(root.anchor_count as usize, 32)?,
                mul(
                    root.draft_count as usize,
                    std::mem::size_of::<c::IndexDraft>() + 512,
                )?,
            )?,
        )?;
        if memory > s.max_decode_bytes {
            return Err(limit());
        }
        visited.insert(key);
        let mut walk = GraphWalk {
            table: &table,
            visited: &mut visited,
            roots,
            budget,
            limits,
            steps: &mut steps,
            encoded: 22,
            memory,
            edges: 0,
        };
        walk.tick(1)?;
        walk.node(root.anchors, 0)?;
        if let Some(id) = root.drafts {
            walk.node(id, 0)?;
        }
    }
    if visited.len() != table.len() {
        return Err(invalid("capture graph exact closure mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jj_lib::{
        backend::{MillisSinceEpoch, Timestamp},
        op_store::{
            OpStore, Operation, OperationId, OperationMetadata, RootOperationData, TimestampRange,
            View,
        },
        simple_op_store::SimpleOpStore,
    };
    fn text(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    fn fixture(
        objects: &[c::VerifiedObject],
        heads: Vec<[u8; 64]>,
        wc: c::WorkingCopy,
        remote: bool,
    ) -> (ManifestV3, c::CaptureData, Vec<c::VerifiedIndexArtifact>) {
        let l = ManifestV3Limits::default();
        let mut draft = m::synthetic_root();
        draft.parents = vec![m::CommitId([9; 32]), m::CommitId::ROOT];
        draft.description = "retained hidden draft".into();
        let encoded = m::encode(&m::Object::Commit(draft.clone()), Default::default()).unwrap();
        let model = c::PortableIndexSnapshot {
            roots: vec![m::CommitId([9; 32])],
            drafts: vec![c::IndexDraft {
                descriptor: encoded.descriptor,
                change: draft.change_id,
                parents: draft.parents,
                order: 7,
            }],
        };
        let built = c::build_index_artifacts(&model, l.history.artifacts).unwrap();
        let artifacts: Vec<_> = built
            .artifacts
            .into_iter()
            .map(|a| c::decode_index_artifact(&a.bytes, a.descriptor, l.history.artifacts).unwrap())
            .collect();
        let mut operations = vec![c::OperationRecord {
            operation: text(&[0; 64]),
            view: text(&[0; 64]),
            parents: vec![],
        }];
        let mut blobs = vec![];
        let mut members = vec![];
        for object in objects {
            let (kind, id) = match object.id() {
                c::ObjectId::Operation(id) => (1, id),
                c::ObjectId::View(id) => (2, id),
            };
            let d = object.digest().unwrap();
            blobs.push(c::BlobRecord {
                kind,
                id: text(&id),
                blake3: d.blake3,
                size: d.size_bytes,
            });
            members.push(c::Member {
                id: object.id(),
                selected: d,
            });
            if let Some(op) = object.operation() {
                operations.push(c::OperationRecord {
                    operation: text(&id),
                    view: text(op.view_id.as_bytes()),
                    parents: op.parents.iter().map(|id| text(id.as_bytes())).collect(),
                });
            }
        }
        for a in &artifacts {
            let d = a.descriptor();
            let (kind, id) = match d.id {
                c::IndexArtifactId::Root(id) => (3, id.0),
                c::IndexArtifactId::Node(id) => (4, id.0),
            };
            blobs.push(c::BlobRecord {
                kind,
                id: text(&id),
                blake3: d.selected.blake3,
                size: d.selected.size_bytes,
            });
        }
        operations.sort_by(|a, b| a.operation.cmp(&b.operation));
        blobs.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
        members.sort_by_key(|m| m.id);
        let mut roots: BTreeSet<_> = objects
            .iter()
            .flat_map(|o| {
                o.dependencies()
                    .native_commits
                    .iter()
                    .map(|id| (m::Kind::Commit as u8, *id))
            })
            .filter(|(_, id)| *id != [0; 32])
            .collect();
        roots.insert((m::Kind::Commit as u8, [9; 32]));
        roots.insert((m::Kind::Commit as u8, encoded.descriptor.key.id.0));
        if wc.snapshot_commit != [0; 32] {
            roots.insert((m::Kind::Commit as u8, wc.snapshot_commit));
        }
        for id in &wc.tree.terms {
            roots.insert((m::Kind::Tree as u8, id.0));
        }
        if remote {
            roots.insert((m::Kind::Commit as u8, [11; 32]));
            roots.insert((m::Kind::Commit as u8, [12; 32]));
        }
        let mut data = c::CaptureData {
            version: 1,
            policy: c::CapturePolicy::HostQuiescentV1,
            codec: c::CODEC.into(),
            operation_heads: heads.iter().map(|id| text(id)).collect(),
            working_copy: c::PortableWorkingCopy {
                workspace: wc.workspace_name.clone(),
                wc_operation: text(&wc.wc_operation),
                snapshot_operation: text(&wc.snapshot_operation),
                snapshot_commit: wc.snapshot_commit,
                tree: c::CaptureTree {
                    terms: wc.tree.terms.iter().map(|id| id.0).collect(),
                    labels: vec![],
                },
            },
            indexes: operations
                .iter()
                .map(|op| c::IndexRecord {
                    operation: op.operation.clone(),
                    root: built.root.id.0,
                })
                .collect(),
            operations,
            blobs,
            roots: roots
                .iter()
                .map(|(kind, id)| c::CapturedRoot {
                    kind: *kind,
                    id: *id,
                })
                .collect(),
            remote: remote.then(|| c::PortableRemoteState {
                observations: vec![c::RemoteObservation {
                    reference: "refs/heads/main".into(),
                    bookmark: "main".into(),
                    generation: 1,
                    head: Some(text(&[11; 32])),
                }],
                archived_pushes: vec![c::ArchivedIntent {
                    reference: "refs/heads/main".into(),
                    bookmark: "main".into(),
                    generation: 1,
                    head: Some(text(&[12; 32])),
                    request_id: text(&[3; 16]),
                    base_operation: text(&[0; 64]),
                    confirmed_generation: Some(2),
                }],
            }),
        };
        // Nonempty labels are semantic, anonymous labels use the existing empty
        // representation on the portable capsule side.
        if !wc.tree.labels.iter().all(String::is_empty) {
            data.working_copy.tree.labels = wc.tree.labels.clone();
        }
        let bytes = c::encode_capture_data(&data, l.capture).unwrap();
        let capture = CaptureDescriptor::of(&bytes);
        let indexes = data
            .indexes
            .iter()
            .map(|i| c::OperationIndex {
                operation: hex(&i.operation).unwrap(),
                root: c::IndexRootId(i.root),
            })
            .collect();
        let undo = data
            .operations
            .iter()
            .skip(1)
            .filter(|op| data.operation_heads.binary_search(&op.operation).is_err())
            .map(|op| hex(&op.operation).unwrap())
            .collect();
        let recovery = c::Manifest {
            workspace: [1; 32],
            exporter: [2; 16],
            sequence: 1,
            operation_heads: heads,
            undo_operations: undo,
            intent_operations: vec![],
            working_copy: wc,
            parents: vec![],
            members,
            intent_roots: capture_recovery_intents(&data, capture.id, l.binding).unwrap(),
            native_roots: roots
                .into_iter()
                .map(|(kind, id)| m::ObjectKey {
                    kind: m::Kind::try_from(kind).unwrap(),
                    id: m::ObjectId(id),
                })
                .collect(),
            coverage: c::Coverage::FullRequired,
            detached: vec![],
        };
        (
            ManifestV3 {
                history: c::ManifestV2 {
                    recovery,
                    operation_indexes: indexes,
                    index_roots: vec![built.root],
                },
                capture,
            },
            data,
            artifacts,
        )
    }
    fn intrinsic() -> c::WorkingCopy {
        c::WorkingCopy {
            workspace_name: "default".into(),
            wc_operation: [0; 64],
            snapshot_operation: [0; 64],
            snapshot_commit: [0; 32],
            tree: m::synthetic_root().root_tree,
        }
    }
    fn assert_binding(
        value: &ManifestV3,
        data: &c::CaptureData,
        objects: &[c::VerifiedObject],
        artifacts: &[c::VerifiedIndexArtifact],
        limits: ManifestV3Limits,
    ) -> Result<u64> {
        let bytes = c::encode_capture_data(data, limits.capture).unwrap();
        let graph = c::verify_index_artifact_graph(
            &value.history.index_roots,
            artifacts,
            Default::default(),
        )
        .unwrap();
        verify_capture_export_binding(value, &bytes, objects, artifacts, &graph, limits)
            .map(|checked| checked.binding_work())
    }
    #[test]
    fn v3_codec_and_exact_intrinsic_binding_reject_legacy_attachments_and_unselected_roots() {
        let limits = ManifestV3Limits::default();
        let (value, data, artifacts) = fixture(&[], vec![[0; 64]], intrinsic(), false);
        assert!(assert_binding(&value, &data, &[], &artifacts, limits).unwrap() > 0);
        let bytes = encode_manifest_v3(&value, limits).unwrap();
        assert_eq!(
            decode_manifest_v3(&bytes, ManifestId::of(&bytes), limits).unwrap(),
            value
        );
        assert!(c::decode_manifest_v2(&bytes, ManifestId::of(&bytes), limits.history).is_err());
        let mut old = bytes.clone();
        old[8..10].copy_from_slice(&2u16.to_le_bytes());
        assert!(decode_manifest_v3(&old, ManifestId::of(&old), limits).is_err());
        for malformed in [
            &bytes[..bytes.len() - 1],
            &[bytes.as_slice(), &[0]].concat(),
        ] {
            assert!(decode_manifest_v3(malformed, ManifestId::of(malformed), limits).is_err());
        }
        for domain in [true, false] {
            let mut wrong = value.clone();
            if domain {
                wrong.capture.id.0[0] ^= 1;
            } else {
                wrong.capture.selected.blake3[0] ^= 1;
            }
            assert!(assert_binding(&wrong, &data, &[], &artifacts, limits).is_err());
        }
        let mut wrong = value.clone();
        let mut invented = data.clone();
        invented.roots.push(c::CapturedRoot {
            kind: m::Kind::Commit as u8,
            id: [99; 32],
        });
        invented.roots.sort();
        let raw = c::encode_capture_data(&invented, limits.capture).unwrap();
        wrong.capture = CaptureDescriptor::of(&raw);
        wrong.history.recovery.native_roots.push(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId([99; 32]),
        });
        wrong
            .history
            .recovery
            .native_roots
            .sort_by_key(|r| (r.kind as u8, r.id.0));
        wrong.history.recovery.intent_roots = vec![c::IntentRoots {
            kind: c::IntentKind::Recovery,
            id: [4; 32],
            roots: vec![m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId([99; 32]),
            }],
        }];
        assert!(assert_binding(&wrong, &invented, &[], &artifacts, limits).is_err());
        // Even agreement between both root lists cannot invent native authority.
        // Clear the unrelated intent mismatch so this reaches the union check.
        wrong.history.recovery.intent_roots.clear();
        assert!(matches!(
            assert_binding(&wrong, &invented, &[], &artifacts, limits),
            Err(Error::Invalid(
                "independent capture native root union mismatch"
            ))
        ));
        let mut candidate = data.clone();
        candidate.policy = c::CapturePolicy::TreeFreezeCandidateV1;
        let raw = c::encode_capture_data(&candidate, limits.capture).unwrap();
        let mut wrong = value.clone();
        wrong.capture = CaptureDescriptor::of(&raw);
        assert!(assert_binding(&wrong, &candidate, &[], &artifacts, limits).is_err());
        for scarce in [
            CaptureBindingLimits {
                max_work: 1,
                ..limits.binding
            },
            CaptureBindingLimits {
                max_bytes: 1,
                ..limits.binding
            },
        ] {
            assert!(matches!(
                assert_binding(
                    &value,
                    &data,
                    &[],
                    &artifacts,
                    ManifestV3Limits {
                        binding: scarce,
                        ..limits
                    }
                ),
                Err(Error::Limit(_))
            ));
        }
        // A previously broad-decoded proof must not bypass tighter current caps.
        let mut tight = limits;
        tight.history.artifacts.snapshot.max_parents = 1;
        assert!(matches!(
            assert_binding(&value, &data, &[], &artifacts, tight),
            Err(Error::Limit(_))
        ));
        let mut tight = limits;
        tight.history.artifacts.max_graph_steps = 1;
        assert!(matches!(
            assert_binding(&value, &data, &[], &artifacts, tight),
            Err(Error::Limit(_))
        ));
        let mut wrong = value.clone();
        wrong.history.index_roots[0].selected.blake3[0] ^= 1;
        let graph = c::verify_index_artifact_graph(
            &value.history.index_roots,
            &artifacts,
            Default::default(),
        )
        .unwrap();
        let raw = c::encode_capture_data(&data, limits.capture).unwrap();
        assert!(
            verify_capture_export_binding(&wrong, &raw, &[], &artifacts, &graph, limits).is_err()
        );
    }
    fn op(view: jj_lib::op_store::ViewId, parents: Vec<OperationId>, label: &str) -> Operation {
        let t = Timestamp {
            timestamp: MillisSinceEpoch(1),
            tz_offset: 0,
        };
        Operation {
            view_id: view,
            parents,
            metadata: OperationMetadata {
                time: TimestampRange {
                    start: t.clone(),
                    end: t,
                },
                description: label.into(),
                hostname: "agent".into(),
                username: "agent".into(),
                is_snapshot: true,
                workspace_name: Some("default".into()),
                attributes: BTreeMap::new(),
            },
            commit_predecessors: None,
        }
    }
    #[tokio::test]
    async fn exact_actual_operation_views_ordered_merge_parents_undo_and_remote_inventory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("op");
        std::fs::create_dir(&path).unwrap();
        let store = SimpleOpStore::init(
            &path,
            RootOperationData {
                root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
            },
        )
        .unwrap();
        let mut view = View::make_root(jj_lib::backend::CommitId::from_bytes(&[9; 32]));
        view.wc_commit_ids.insert(
            "default".into(),
            jj_lib::backend::CommitId::from_bytes(&[9; 32]),
        );
        let v = store.write_view(&view).await.unwrap();
        let a = store
            .write_operation(&op(
                v.clone(),
                vec![OperationId::from_bytes(&[0; 64])],
                "retained ancestor",
            ))
            .await
            .unwrap();
        let b = store
            .write_operation(&op(v.clone(), vec![a.clone()], "non-head undo"))
            .await
            .unwrap();
        let branch = store
            .write_operation(&op(
                v.clone(),
                vec![OperationId::from_bytes(&[0; 64])],
                "divergent undo",
            ))
            .await
            .unwrap();
        let head = store
            .write_operation(&op(
                v.clone(),
                vec![b.clone(), branch.clone()],
                "ordered merge",
            ))
            .await
            .unwrap();
        let mut objects = vec![
            c::export_view(&store, &v, Default::default())
                .await
                .unwrap(),
        ];
        for id in [&a, &b, &branch, &head] {
            objects.push(
                c::export_operation(&store, id, Default::default())
                    .await
                    .unwrap(),
            );
        }
        let wc = c::WorkingCopy {
            workspace_name: "default".into(),
            wc_operation: a.as_bytes().try_into().unwrap(),
            snapshot_operation: head.as_bytes().try_into().unwrap(),
            snapshot_commit: [9; 32],
            tree: m::synthetic_root().root_tree,
        };
        let (value, data, artifacts) = fixture(
            &objects,
            vec![head.as_bytes().try_into().unwrap()],
            wc,
            true,
        );
        let limits = ManifestV3Limits::default();
        assert!(assert_binding(&value, &data, &objects, &artifacts, limits).is_ok());
        let mut wrong = value.clone();
        wrong.history.recovery.undo_operations.pop();
        assert!(assert_binding(&wrong, &data, &objects, &artifacts, limits).is_err());
        let mut reordered = data.clone();
        reordered
            .operations
            .iter_mut()
            .find(|o| o.operation == text(head.as_bytes()))
            .unwrap()
            .parents
            .reverse();
        let mut wrong = value.clone();
        wrong.capture =
            CaptureDescriptor::of(&c::encode_capture_data(&reordered, limits.capture).unwrap());
        assert!(assert_binding(&wrong, &reordered, &objects, &artifacts, limits).is_err());
        let mut wrong = value.clone();
        wrong.history.recovery.members[0].selected.blake3[0] ^= 1;
        assert!(assert_binding(&wrong, &data, &objects, &artifacts, limits).is_err());
        let mut wrong = value.clone();
        wrong.history.recovery.intent_operations = vec![head.as_bytes().try_into().unwrap()];
        assert!(assert_binding(&wrong, &data, &objects, &artifacts, limits).is_err());
        let mut wrong = value.clone();
        wrong.history.recovery.intent_roots[0].roots.pop();
        assert!(assert_binding(&wrong, &data, &objects, &artifacts, limits).is_err());
        let mut wrong = value.clone();
        wrong.history.recovery.working_copy.snapshot_commit = [8; 32];
        assert!(assert_binding(&wrong, &data, &objects, &artifacts, limits).is_err());
        // Capsule+manifest agreement still cannot substitute a false actual WC.
        let mut false_wc = data.clone();
        false_wc.working_copy.snapshot_commit = [8; 32];
        let mut wrong = value.clone();
        wrong.history.recovery.working_copy.snapshot_commit = [8; 32];
        wrong.capture =
            CaptureDescriptor::of(&c::encode_capture_data(&false_wc, limits.capture).unwrap());
        assert!(assert_binding(&wrong, &false_wc, &objects, &artifacts, limits).is_err());
    }
}
