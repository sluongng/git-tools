//! Deterministic recovery manifests. Coverage is relative to declared capture
//! roots; it is not proof of client fencing or an authorization/retention lease.
use crate::{BlobDigest, CODEC, Error, ObjectId, Result, VerifiedObject};
use jj_lib::object_id::ObjectId as _;
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model as m;

const MAGIC: &[u8; 8] = b"TTCLOUD\0";
const VERSION: u16 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestId(pub [u8; 32]);
impl ManifestId {
    /// Hash exact canonical bytes in a domain distinct from raw cloud payloads.
    pub fn of(bytes: &[u8]) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-cloud-manifest-v1\0");
        hash.update(bytes);
        Self(*hash.finalize().as_bytes())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: ObjectId,
    pub selected: BlobDigest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coverage {
    /// Every operation/view reachable from the declared required roots exists.
    /// This does not claim that an exporter declared all its local undo history.
    FullRequired,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum IntentKind {
    Publication = 1,
    Recovery = 2,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentRoots {
    pub kind: IntentKind,
    pub id: [u8; 32],
    pub roots: Vec<m::ObjectKey>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingParentEdge {
    pub from_operation: [u8; 64],
    pub to_parent: [u8; 64],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DetachedComponent {
    pub roots: Vec<[u8; 64]>,
    pub missing_parent_edges: Vec<MissingParentEdge>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct WorkingCopy {
    pub workspace_name: String,
    pub wc_operation: [u8; 64],
    pub snapshot_operation: [u8; 64],
    pub snapshot_commit: [u8; 32],
    /// Exact add/remove/add order and labels, including unresolved terms.
    pub tree: m::TreeMerge,
}
impl std::fmt::Debug for WorkingCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkingCopy")
            .field("tree_terms", &self.tree.terms.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, PartialEq, Eq)]
pub struct Manifest {
    pub workspace: [u8; 32],
    pub exporter: [u8; 16],
    pub sequence: u64,
    pub operation_heads: Vec<[u8; 64]>,
    pub undo_operations: Vec<[u8; 64]>,
    pub intent_operations: Vec<[u8; 64]>,
    pub working_copy: WorkingCopy,
    pub parents: Vec<ManifestId>,
    pub members: Vec<Member>,
    pub intent_roots: Vec<IntentRoots>,
    pub native_roots: Vec<m::ObjectKey>,
    pub coverage: Coverage,
    /// Version one explicitly rejects nonempty detached components.
    pub detached: Vec<DetachedComponent>,
}
impl std::fmt::Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manifest")
            .field("members", &self.members.len())
            .field("native_roots", &self.native_roots.len())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ManifestLimits {
    pub max_manifest_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_members: usize,
    pub max_operation_roots: usize,
    pub max_native_roots: usize,
    pub max_intents: usize,
    pub max_tree_terms: usize,
    pub max_name_bytes: usize,
    pub max_label_bytes: usize,
    pub max_edges: usize,
    pub max_total_payload_bytes: u64,
}
impl Default for ManifestLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 8 * 1024 * 1024,
            max_decode_bytes: 64 * 1024 * 1024,
            max_members: 65_536,
            max_operation_roots: 4096,
            max_native_roots: 100_000,
            max_intents: 1024,
            max_tree_terms: 4095,
            max_name_bytes: 1024,
            max_label_bytes: 64 * 1024,
            max_edges: 1_000_000,
            max_total_payload_bytes: 256 * 1024 * 1024,
        }
    }
}
impl ManifestLimits {
    pub fn validate(self) -> Result<()> {
        if !(1..=64 * 1024 * 1024).contains(&self.max_manifest_bytes)
            || self.max_decode_bytes == 0
            || self.max_decode_bytes > 1024 * 1024 * 1024
            || !(1..=1_000_000).contains(&self.max_members)
            || !(1..=100_000).contains(&self.max_operation_roots)
            || !(1..=1_000_000).contains(&self.max_native_roots)
            || !(1..=100_000).contains(&self.max_intents)
            || !(1..=100_001).contains(&self.max_tree_terms)
            || !(1..=64 * 1024).contains(&self.max_name_bytes)
            || self.max_label_bytes > 1024 * 1024
            || !(1..=10_000_000).contains(&self.max_edges)
            || self.max_total_payload_bytes > 1024 * 1024 * 1024 * 1024
        {
            return Err(Error::Invalid("manifest limits"));
        }
        Ok(())
    }
}
fn invalid(reason: &'static str) -> Error {
    Error::Invalid(reason)
}
fn limit() -> Error {
    Error::Limit("manifest resource budget")
}
fn root_key(root: &m::ObjectKey) -> (u8, [u8; 32]) {
    (root.kind as u8, root.id.0)
}
fn sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|p| p[0] < p[1])
}
fn root_set(roots: &[m::ObjectKey], maximum: usize) -> Result<()> {
    if roots.len() > maximum {
        return Err(limit());
    }
    if roots
        .iter()
        .any(|r| r.kind == m::Kind::Commit && r.id.0 == [0; 32])
        || !roots.windows(2).all(|p| root_key(&p[0]) < root_key(&p[1]))
    {
        return Err(invalid("native root ordering or intrinsic root"));
    }
    Ok(())
}
fn shape(manifest: &Manifest, limits: ManifestLimits) -> Result<()> {
    limits.validate()?;
    if !manifest.detached.is_empty() {
        return Err(Error::Unsupported("detached cloud components"));
    }
    if manifest.operation_heads.is_empty() {
        return Err(invalid("missing current operation heads"));
    }
    for roots in [
        &manifest.operation_heads,
        &manifest.undo_operations,
        &manifest.intent_operations,
    ] {
        if roots.len() > limits.max_operation_roots {
            return Err(limit());
        }
        if !sorted(roots) {
            return Err(invalid("operation root ordering"));
        }
    }
    let name = &manifest.working_copy.workspace_name;
    if name.is_empty()
        || name.len() > limits.max_name_bytes
        || name == "."
        || name == ".."
        || name
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(invalid("portable workspace name"));
    }
    let tree = &manifest.working_copy.tree;
    if tree.terms.is_empty()
        || tree.terms.len() % 2 != 1
        || tree.terms.len() != tree.labels.len()
        || (tree.terms.len() == 1 && !tree.labels[0].is_empty())
    {
        return Err(invalid("working-copy tree merge shape"));
    }
    if tree.terms.len() > limits.max_tree_terms
        || tree.labels.iter().any(|s| s.len() > limits.max_label_bytes)
    {
        return Err(limit());
    }
    if manifest.parents.len() > limits.max_operation_roots {
        return Err(limit());
    }
    if !sorted(&manifest.parents) {
        return Err(invalid("cloud parent ordering"));
    }
    if manifest.members.len() > limits.max_members {
        return Err(limit());
    }
    if manifest.members.iter().any(|m| m.id.is_intrinsic())
        || !manifest.members.windows(2).all(|p| p[0].id < p[1].id)
    {
        return Err(invalid("cloud member ordering or intrinsic member"));
    }
    manifest.members.iter().try_fold(0u64, |total, member| {
        total
            .checked_add(member.selected.size_bytes)
            .filter(|n| *n <= limits.max_total_payload_bytes)
            .ok_or_else(limit)
    })?;
    if manifest.intent_roots.len() > limits.max_intents {
        return Err(limit());
    }
    if !manifest
        .intent_roots
        .windows(2)
        .all(|p| (p[0].kind, p[0].id) < (p[1].kind, p[1].id))
    {
        return Err(invalid("intent ordering"));
    }
    let supplemental = manifest.intent_roots.iter().try_fold(
        manifest.working_copy.tree.terms.len(),
        |n, intent| {
            n.checked_add(intent.roots.len())
                .filter(|n| *n <= limits.max_edges)
                .ok_or_else(limit)
        },
    )?;
    if supplemental > limits.max_edges {
        return Err(limit());
    }
    for intent in &manifest.intent_roots {
        root_set(&intent.roots, limits.max_native_roots)?;
    }
    root_set(&manifest.native_roots, limits.max_native_roots)?;
    Ok(())
}
struct Writer {
    bytes: Option<Vec<u8>>,
    used: usize,
    maximum: usize,
}
impl Writer {
    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes.len())
            .filter(|n| *n <= self.maximum)
            .ok_or_else(limit)?;
        if let Some(out) = &mut self.bytes {
            out.extend_from_slice(bytes);
        }
        Ok(())
    }
    fn u8(&mut self, n: u8) -> Result<()> {
        self.raw(&[n])
    }
    fn u16(&mut self, n: u16) -> Result<()> {
        self.raw(&n.to_le_bytes())
    }
    fn u32(&mut self, n: usize) -> Result<()> {
        self.raw(&u32::try_from(n).map_err(|_| limit())?.to_le_bytes())
    }
    fn u64(&mut self, n: u64) -> Result<()> {
        self.raw(&n.to_le_bytes())
    }
    fn string(&mut self, s: &str) -> Result<()> {
        self.u32(s.len())?;
        self.raw(s.as_bytes())
    }
    fn ops(&mut self, ids: &[[u8; 64]]) -> Result<()> {
        self.u32(ids.len())?;
        for id in ids {
            self.raw(id)?;
        }
        Ok(())
    }
    fn roots(&mut self, roots: &[m::ObjectKey]) -> Result<()> {
        self.u32(roots.len())?;
        for root in roots {
            self.u8(root.kind as u8)?;
            self.raw(&root.id.0)?;
        }
        Ok(())
    }
}
fn write(manifest: &Manifest, w: &mut Writer) -> Result<()> {
    w.raw(MAGIC)?;
    w.u16(VERSION)?;
    w.u16(m::FORMAT_VERSION)?;
    w.string(CODEC)?;
    w.raw(&manifest.workspace)?;
    w.raw(&manifest.exporter)?;
    w.u64(manifest.sequence)?;
    w.u8(match manifest.coverage {
        Coverage::FullRequired => 1,
    })?;
    w.ops(&manifest.operation_heads)?;
    w.ops(&manifest.undo_operations)?;
    w.ops(&manifest.intent_operations)?;
    let wc = &manifest.working_copy;
    w.string(&wc.workspace_name)?;
    w.raw(&wc.wc_operation)?;
    w.raw(&wc.snapshot_operation)?;
    w.raw(&wc.snapshot_commit)?;
    w.u32(wc.tree.terms.len())?;
    for term in &wc.tree.terms {
        w.raw(&term.0)?;
    }
    w.u32(wc.tree.labels.len())?;
    for label in &wc.tree.labels {
        w.string(label)?;
    }
    w.u32(manifest.parents.len())?;
    for parent in &manifest.parents {
        w.raw(&parent.0)?;
    }
    w.u32(manifest.members.len())?;
    for member in &manifest.members {
        w.u8(match member.id {
            ObjectId::Operation(_) => 1,
            ObjectId::View(_) => 2,
        })?;
        w.raw(member.id.bytes())?;
        w.raw(&member.selected.blake3)?;
        w.u64(member.selected.size_bytes)?;
    }
    w.u32(manifest.intent_roots.len())?;
    for intent in &manifest.intent_roots {
        w.u8(intent.kind as u8)?;
        w.raw(&intent.id)?;
        w.roots(&intent.roots)?;
    }
    w.roots(&manifest.native_roots)?;
    w.u32(0)?;
    Ok(())
}
/// Encode canonical order as supplied. Never normalize sets or merge terms.
pub fn encode_manifest(manifest: &Manifest, limits: ManifestLimits) -> Result<Vec<u8>> {
    shape(manifest, limits)?;
    let mut size = Writer {
        bytes: None,
        used: 0,
        maximum: limits.max_manifest_bytes,
    };
    write(manifest, &mut size)?;
    let mut out = Writer {
        bytes: Some(Vec::with_capacity(size.used)),
        used: 0,
        maximum: limits.max_manifest_bytes,
    };
    write(manifest, &mut out)?;
    Ok(out.bytes.expect("writer has a byte buffer"))
}
struct Memory {
    used: usize,
    maximum: usize,
}
impl Memory {
    fn charge(&mut self, n: usize) -> Result<()> {
        self.used = self
            .used
            .checked_add(n)
            .filter(|n| *n <= self.maximum)
            .ok_or_else(limit)?;
        Ok(())
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
    memory: Memory,
}
impl Reader<'_> {
    fn raw(&mut self, n: usize) -> Result<&[u8]> {
        let end = self.position.checked_add(n).ok_or_else(limit)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("truncated cloud manifest"))?;
        self.position = end;
        Ok(bytes)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.raw(N)?.try_into().expect("checked fixed-width field"))
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.array()?) as usize)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn count<T>(&mut self, maximum: usize, min_wire: usize) -> Result<usize> {
        let n = self.u32()?;
        if n > maximum
            || n.checked_mul(min_wire)
                .is_none_or(|bytes| bytes > self.bytes.len() - self.position)
        {
            return Err(limit());
        }
        self.memory
            .charge(n.checked_mul(std::mem::size_of::<T>()).ok_or_else(limit)?)?;
        Ok(n)
    }
    fn string(&mut self, maximum: usize) -> Result<String> {
        let n = self.u32()?;
        if n > maximum {
            return Err(limit());
        }
        self.memory.charge(n)?;
        let s = std::str::from_utf8(self.raw(n)?)
            .map_err(|_| invalid("manifest string is not UTF-8"))?;
        Ok(s.to_owned())
    }
    fn ops(&mut self, maximum: usize) -> Result<Vec<[u8; 64]>> {
        let n = self.count::<[u8; 64]>(maximum, 64)?;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(self.array()?);
        }
        Ok(v)
    }
    fn roots(&mut self, maximum: usize) -> Result<Vec<m::ObjectKey>> {
        let n = self.count::<m::ObjectKey>(maximum, 33)?;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let kind =
                m::Kind::try_from(self.u8()?).map_err(|_| invalid("manifest native root kind"))?;
            v.push(m::ObjectKey {
                kind,
                id: m::ObjectId(self.array()?),
            });
        }
        Ok(v)
    }
}
/// Verify an independently selected identity before bounded canonical decoding.
pub fn decode_manifest(
    bytes: &[u8],
    expected: ManifestId,
    limits: ManifestLimits,
) -> Result<Manifest> {
    limits.validate()?;
    if bytes.len() > limits.max_manifest_bytes {
        return Err(limit());
    }
    if ManifestId::of(bytes) != expected {
        return Err(invalid("manifest identity mismatch"));
    }
    let mut r = Reader {
        bytes,
        position: 0,
        memory: Memory {
            used: std::mem::size_of::<Manifest>(),
            maximum: limits.max_decode_bytes,
        },
    };
    if r.raw(MAGIC.len())? != MAGIC {
        return Err(invalid("manifest magic"));
    }
    if r.u16()? != VERSION || r.u16()? != m::FORMAT_VERSION {
        return Err(Error::Unsupported("manifest or native format version"));
    }
    let codec_len = r.u32()?;
    if codec_len != CODEC.len() || r.raw(codec_len)? != CODEC.as_bytes() {
        return Err(Error::Unsupported("manifest operation codec"));
    }
    let workspace = r.array()?;
    let exporter = r.array()?;
    let sequence = r.u64()?;
    let coverage = match r.u8()? {
        1 => Coverage::FullRequired,
        _ => return Err(Error::Unsupported("manifest coverage policy")),
    };
    let operation_heads = r.ops(limits.max_operation_roots)?;
    let undo_operations = r.ops(limits.max_operation_roots)?;
    let intent_operations = r.ops(limits.max_operation_roots)?;
    let workspace_name = r.string(limits.max_name_bytes)?;
    let wc_operation = r.array()?;
    let snapshot_operation = r.array()?;
    let snapshot_commit = r.array()?;
    let n = r.count::<m::TreeId>(limits.max_tree_terms, 32)?;
    let mut terms = Vec::with_capacity(n);
    for _ in 0..n {
        terms.push(m::TreeId(r.array()?));
    }
    let n = r.count::<String>(limits.max_tree_terms, 4)?;
    let mut labels = Vec::with_capacity(n);
    for _ in 0..n {
        labels.push(r.string(limits.max_label_bytes)?);
    }
    let working_copy = WorkingCopy {
        workspace_name,
        wc_operation,
        snapshot_operation,
        snapshot_commit,
        tree: m::TreeMerge { terms, labels },
    };
    let n = r.count::<ManifestId>(limits.max_operation_roots, 32)?;
    let mut parents = Vec::with_capacity(n);
    for _ in 0..n {
        parents.push(ManifestId(r.array()?));
    }
    let n = r.count::<Member>(limits.max_members, 105)?;
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = r.u8()?;
        let semantic = r.array()?;
        let id = match kind {
            1 => ObjectId::Operation(semantic),
            2 => ObjectId::View(semantic),
            _ => return Err(invalid("manifest cloud object kind")),
        };
        members.push(Member {
            id,
            selected: BlobDigest {
                blake3: r.array()?,
                size_bytes: r.u64()?,
            },
        });
    }
    let n = r.count::<IntentRoots>(limits.max_intents, 37)?;
    let mut intent_roots = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = match r.u8()? {
            1 => IntentKind::Publication,
            2 => IntentKind::Recovery,
            _ => return Err(Error::Unsupported("manifest intent kind")),
        };
        let id = r.array()?;
        let roots = r.roots(limits.max_native_roots)?;
        intent_roots.push(IntentRoots { kind, id, roots });
    }
    let native_roots = r.roots(limits.max_native_roots)?;
    if r.u32()? != 0 {
        return Err(Error::Unsupported("detached cloud components"));
    }
    if r.position != bytes.len() {
        return Err(invalid("trailing cloud manifest data"));
    }
    let manifest = Manifest {
        workspace,
        exporter,
        sequence,
        operation_heads,
        undo_operations,
        intent_operations,
        working_copy,
        parents,
        members,
        intent_roots,
        native_roots,
        coverage,
        detached: vec![],
    };
    shape(&manifest, limits)?;
    Ok(manifest)
}
/// Borrowed proof: selected object bytes and the manifest cannot be replaced
/// while its exact root set is used. Store callers still pin storage and verify
/// native closure plus snapshot commit/tree equality before durable publication.
pub struct ValidatedManifest<'a> {
    manifest: &'a Manifest,
    _objects: &'a [VerifiedObject],
}
impl ValidatedManifest<'_> {
    pub fn native_roots(&self) -> &[m::ObjectKey] {
        &self.manifest.native_roots
    }
    pub fn working_copy(&self) -> &WorkingCopy {
        &self.manifest.working_copy
    }
}
struct Frame {
    id: ObjectId,
    next: usize,
}
fn object<'a>(
    objects: &BTreeMap<ObjectId, &'a VerifiedObject>,
    id: ObjectId,
) -> Result<&'a VerifiedObject> {
    objects
        .get(&id)
        .copied()
        .ok_or_else(|| invalid("required cloud object missing"))
}
/// Validate complete OP/VIEW coverage and the exact derived native-root union.
/// Already verified objects are borrowed, not decoded/cloned again. The caller
/// separately budgets their codec allocations while constructing this slice.
pub fn verify_coverage<'a>(
    manifest: &'a Manifest,
    objects: &'a [VerifiedObject],
    limits: ManifestLimits,
) -> Result<ValidatedManifest<'a>> {
    shape(manifest, limits)?;
    if objects.len() != manifest.members.len() {
        return Err(invalid("cloud member coverage count"));
    }
    let mut memory = Memory {
        used: 0,
        maximum: limits.max_decode_bytes,
    };
    // Covers fixed validation scaffolding and the model's small intrinsic-root
    // construction before any table, frame or root-set allocation is allowed.
    memory.charge(4096)?;
    memory.charge(objects.len().checked_mul(256).ok_or_else(limit)?)?;
    let mut table = BTreeMap::new();
    let mut payload = 0u64;
    let mut edges = manifest
        .intent_roots
        .iter()
        .fold(manifest.working_copy.tree.terms.len(), |n, intent| {
            n.saturating_add(intent.roots.len())
        });
    for object in objects {
        if object.id().is_intrinsic() || table.insert(object.id(), object).is_some() {
            return Err(invalid("duplicate or intrinsic cloud payload"));
        }
        let digest = object
            .digest()
            .ok_or_else(|| invalid("missing selected cloud descriptor"))?;
        payload = payload
            .checked_add(digest.size_bytes)
            .filter(|n| *n <= limits.max_total_payload_bytes)
            .ok_or_else(limit)?;
        let deps = object.dependencies();
        edges = edges
            .checked_add(deps.operations.len())
            .and_then(|n| n.checked_add(deps.views.len()))
            .and_then(|n| n.checked_add(deps.native_commits.len()))
            .filter(|n| *n <= limits.max_edges)
            .ok_or_else(limit)?;
    }
    for member in &manifest.members {
        if object(&table, member.id)?.digest() != Some(member.selected) {
            return Err(invalid("selected cloud representation mismatch"));
        }
    }
    memory.charge(
        objects
            .len()
            .checked_mul(std::mem::size_of::<Frame>())
            .ok_or_else(limit)?,
    )?;
    let mut stack = Vec::<Frame>::with_capacity(objects.len());
    let mut colors = BTreeMap::<ObjectId, u8>::new();
    let wc = &manifest.working_copy;
    for root in manifest
        .operation_heads
        .iter()
        .chain(&manifest.undo_operations)
        .chain(&manifest.intent_operations)
        .chain([&wc.wc_operation, &wc.snapshot_operation])
    {
        let mut pending = Some(ObjectId::Operation(*root));
        loop {
            if let Some(id) = pending.take() {
                if !id.is_intrinsic() {
                    match colors.get(&id) {
                        Some(1) => return Err(invalid("cyclic cloud operation graph")),
                        Some(2) => {}
                        _ => {
                            object(&table, id)?;
                            memory.charge(256)?;
                            colors.insert(id, 1);
                            if stack.len() >= objects.len() {
                                return Err(limit());
                            }
                            stack.push(Frame { id, next: 0 });
                        }
                    }
                }
            }
            let Some(frame) = stack.last_mut() else {
                break;
            };
            let deps = object(&table, frame.id)?.dependencies();
            if frame.next < deps.operations.len() {
                pending = Some(ObjectId::Operation(deps.operations[frame.next]));
                frame.next += 1;
            } else if frame.next < deps.operations.len() + deps.views.len() {
                pending = Some(ObjectId::View(
                    deps.views[frame.next - deps.operations.len()],
                ));
                frame.next += 1;
            } else {
                let id = frame.id;
                stack.pop();
                colors.insert(id, 2);
            }
        }
    }
    if colors.len() != objects.len() || colors.values().any(|c| *c != 2) {
        return Err(invalid("unreachable cloud member"));
    }
    if wc.snapshot_operation == [0; 64] {
        if wc.wc_operation != [0; 64]
            || wc.snapshot_commit != [0; 32]
            || wc.tree != m::synthetic_root().root_tree
        {
            return Err(invalid("intrinsic working-copy checkpoint"));
        }
    } else {
        let op = object(&table, ObjectId::Operation(wc.snapshot_operation))?
            .operation()
            .ok_or_else(|| invalid("snapshot is not an operation"))?;
        let view_id: [u8; 64] = op
            .view_id
            .as_bytes()
            .try_into()
            .map_err(|_| invalid("snapshot view identity width"))?;
        let view = object(&table, ObjectId::View(view_id))?
            .view()
            .ok_or_else(|| invalid("snapshot view missing"))?;
        let snapshot = view
            .wc_commit_ids
            .iter()
            .find(|(name, _)| name.as_str() == wc.workspace_name)
            .map(|(_, id)| id.as_bytes());
        if snapshot != Some(wc.snapshot_commit.as_slice()) {
            return Err(invalid("selected workspace snapshot mismatch"));
        }
    }
    let mut native = BTreeSet::<(u8, [u8; 32])>::new();
    let mut add = |root: m::ObjectKey| -> Result<()> {
        if root.kind == m::Kind::Commit && root.id.0 == [0; 32] {
            return Ok(());
        }
        let key = root_key(&root);
        if !native.contains(&key) {
            if native.len() >= limits.max_native_roots {
                return Err(limit());
            }
            memory.charge(128)?;
            native.insert(key);
        }
        Ok(())
    };
    for object in objects {
        for id in &object.dependencies().native_commits {
            add(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(*id),
            })?;
        }
    }
    for term in &wc.tree.terms {
        add(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(term.0),
        })?;
    }
    for intent in &manifest.intent_roots {
        for root in &intent.roots {
            add(*root)?;
        }
    }
    if !manifest
        .native_roots
        .iter()
        .map(root_key)
        .eq(native.iter().copied())
    {
        return Err(invalid("native root union mismatch"));
    }
    Ok(ValidatedManifest {
        manifest,
        _objects: objects,
    })
}
