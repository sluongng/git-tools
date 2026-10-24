use super::*;
use fs2::FileExt as _;
use std::{fs::File, io::Write};
pub(super) type SegmentId = [u8; 32];
#[derive(Default)]
struct Layout {
    segments: usize,
    bytes: usize,
    linked_segments: usize,
    linked_bytes: usize,
}
/// Export-only shared source-read accounting, checked before every read.
pub(super) struct ExportReadBudget {
    pub bytes: usize,
    pub reads: usize,
    pub max_bytes: usize,
    pub max_reads: usize,
}
impl ExportReadBudget {
    fn cap(&mut self, maximum: usize) -> Result<usize, Error> {
        if self.reads >= self.max_reads || self.bytes >= self.max_bytes {
            return Err(Error::Limit);
        }
        self.reads += 1;
        Ok(maximum.min(self.max_bytes - self.bytes))
    }
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.max_bytes)
            .ok_or(Error::Limit)?;
        Ok(())
    }
}
fn encoded_size(parents: usize, roots: usize, drafts: &[Arc<Draft>]) -> Result<usize, Error> {
    let mut size = 57usize
        .checked_add(parents.checked_mul(32).ok_or(Error::Limit)?)
        .and_then(|n| n.checked_add(roots.checked_mul(32)?))
        .ok_or(Error::Limit)?;
    for draft in drafts {
        size = size
            .checked_add(70)
            .and_then(|n| n.checked_add(draft.parents.len().checked_mul(32)?))
            .ok_or(Error::Limit)?;
    }
    Ok(size)
}
const MAGIC: &[u8; 8] = b"JJNIDX01";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    identity: [u8; 32],
    config: Config,
}
pub(super) struct Storage {
    location: PathBuf,
    root: private_fs::PrivateDir,
    segments: private_fs::PrivateDir,
    operations: private_fs::PrivateDir,
    manifest: Manifest,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn envelope(kind: u8, key: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 48);
    out.extend_from_slice(MAGIC);
    out.push(kind);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    let mut hash = blake3::Hasher::new();
    hash.update(b"jj-native-index-record-v1");
    hash.update(key);
    hash.update(&out);
    out.extend_from_slice(hash.finalize().as_bytes());
    out
}
fn payload<'a>(kind: u8, key: &[u8], bytes: &'a [u8]) -> Result<&'a [u8], Error> {
    if bytes.len() < 45 || &bytes[..8] != MAGIC {
        return Err(Error::Unsupported);
    }
    if bytes[8] != kind {
        return Err(Error::Corrupt);
    }
    let len = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| Error::Corrupt)?) as usize;
    if len.checked_add(45) != Some(bytes.len()) {
        return Err(Error::Corrupt);
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"jj-native-index-record-v1");
    hash.update(key);
    hash.update(&bytes[..13 + len]);
    if hash.finalize().as_bytes() != &bytes[13 + len..] {
        return Err(Error::Corrupt);
    }
    Ok(&bytes[13..13 + len])
}
pub(super) fn require_durable_writer() -> Result<(), Error> {
    if cfg!(unix) {
        Ok(())
    } else {
        Err(Error::Unsupported)
    }
}
fn install(
    dir: &private_fs::PrivateDir,
    name: &str,
    bytes: &[u8],
    cap: usize,
) -> Result<(), Error> {
    if bytes.len() > cap {
        return Err(Error::Limit);
    }
    match dir.read(name, cap) {
        Ok(old) => {
            if old != bytes {
                return Err(Error::Corrupt);
            }
            // Another process may have published this entry but crashed before
            // its directory sync. Idempotent retries establish that durability.
            #[cfg(unix)]
            File::open(dir.path())?.sync_all()?;
            return Ok(());
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut temp = tempfile::NamedTempFile::new_in(dir.path())?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(dir.path().join(name)) {
        Ok(_) => {}
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            if dir.read(name, cap)? != bytes {
                return Err(Error::Corrupt);
            }
        }
        Err(e) => return Err(Error::Io(e.error)),
    }
    #[cfg(unix)]
    File::open(dir.path())?.sync_all()?;
    Ok(())
}
fn lock(root: &private_fs::PrivateDir) -> Result<File, Error> {
    let file = root.open_lock("writer.lock")?;
    file.lock_exclusive()?;
    Ok(file)
}
impl Storage {
    pub(super) fn init(
        path: &Path,
        config: Config,
        limits: &Limits,
        mut roots: Vec<m::CommitId>,
    ) -> Result<Self, Error> {
        require_durable_writer()?;
        if roots.len() > limits.max_roots {
            return Err(Error::Limit);
        }
        roots.sort_unstable();
        roots.dedup();
        if roots.is_empty() {
            roots.push(m::CommitId::ROOT)
        }
        let root = private_fs::PrivateDir::create(path)?;
        // Refuse foreign contents before creating even the coordination lock.
        // Repeat under the lock because another initializer can race this read.
        if std::fs::read_dir(root.path())?
            .any(|e| e.map_or(true, |e| e.file_name() != "writer.lock"))
        {
            return Err(Error::Corrupt);
        }
        let _lock = lock(&root)?;
        // Existing unknown/partial stores are never adopted or repaired.
        if std::fs::read_dir(root.path())?
            .any(|e| e.map_or(true, |e| e.file_name() != "writer.lock"))
        {
            return Err(Error::Corrupt);
        }
        let segments = private_fs::PrivateDir::create(root.path().join("segments"))?;
        let operations = private_fs::PrivateDir::create(root.path().join("operations"))?;
        let manifest = Manifest {
            version: 1,
            identity: random(),
            config,
        };
        let storage = Self {
            location: root.path().canonicalize()?,
            root,
            segments,
            operations,
            manifest,
        };
        let segment = storage.segment(&[], &roots, &[], limits)?;
        let seed = envelope(2, b"seed", &segment);
        install(&storage.root, "seed", &seed, 128)?;
        let config = toml::to_string(&storage.manifest).map_err(|_| Error::Invalid)?;
        install(
            &storage.root,
            "manifest",
            &envelope(0, b"manifest", config.as_bytes()),
            16384,
        )?;
        // create() validates permissions but does not sync the new root's
        // entry in its already-durable parent. All children are synced above.
        #[cfg(unix)]
        File::open(storage.root.path().parent().ok_or(Error::Invalid)?)?.sync_all()?;
        Ok(storage)
    }
    pub(super) fn open(path: &Path, expected: &Config, limits: &Limits) -> Result<Self, Error> {
        let root = private_fs::PrivateDir::open_existing(path)?;
        let bytes = root.read("manifest", 16384)?;
        let bytes = payload(0, b"manifest", &bytes)?;
        let manifest: Manifest =
            toml::from_str(std::str::from_utf8(bytes).map_err(|_| Error::Corrupt)?)
                .map_err(|_| Error::Corrupt)?;
        if manifest.version != 1 {
            return Err(Error::Unsupported);
        }
        if &manifest.config != expected {
            return Err(Error::Foreign);
        }
        manifest.config.validate()?;
        if manifest.identity == [0; 32] {
            return Err(Error::Corrupt);
        }
        let segments = private_fs::PrivateDir::open_existing(root.path().join("segments"))?;
        let operations = private_fs::PrivateDir::open_existing(root.path().join("operations"))?;
        let storage = Self {
            location: root.path().canonicalize()?,
            root,
            segments,
            operations,
            manifest,
        };
        storage.load(&[storage.seed()?], limits)?;
        #[cfg(unix)]
        File::open(storage.root.path())?.sync_all()?;
        #[cfg(unix)]
        File::open(storage.root.path().parent().ok_or(Error::Invalid)?)?.sync_all()?;
        Ok(storage)
    }
    pub(super) fn location(&self) -> &Path {
        &self.location
    }
    pub(super) fn identity(&self) -> [u8; 32] {
        self.manifest.identity
    }
    pub(super) fn seed(&self) -> Result<SegmentId, Error> {
        let bytes = self.root.read("seed", 128)?;
        payload(2, b"seed", &bytes)?
            .try_into()
            .map_err(|_| Error::Corrupt)
    }
    pub(super) fn segment(
        &self,
        parents: &[SegmentId],
        roots: &[m::CommitId],
        drafts: &[Arc<Draft>],
        limits: &Limits,
    ) -> Result<SegmentId, Error> {
        require_durable_writer()?;
        if parents.len() > limits.max_segments
            || roots.len() > limits.max_roots
            || drafts.len() > limits.max_drafts
        {
            return Err(Error::Limit);
        }
        let mut size = 12usize
            .checked_add(parents.len().checked_mul(32).ok_or(Error::Limit)?)
            .and_then(|n| n.checked_add(roots.len().checked_mul(32)?))
            .ok_or(Error::Limit)?;
        for draft in drafts {
            size = size
                .checked_add(70)
                .and_then(|n| n.checked_add(draft.parents.len().checked_mul(32)?))
                .ok_or(Error::Limit)?;
        }
        if size
            .checked_add(45)
            .is_none_or(|n| n > limits.max_segment_bytes)
        {
            return Err(Error::Limit);
        }
        let mut out = Vec::with_capacity(size);
        put_count(&mut out, parents.len());
        for id in parents {
            out.extend_from_slice(id)
        }
        put_count(&mut out, roots.len());
        for id in roots {
            out.extend_from_slice(&id.0)
        }
        put_count(&mut out, drafts.len());
        for draft in drafts {
            if draft.parents.len() > limits.max_parents {
                return Err(Error::Limit);
            }
            out.extend_from_slice(&draft.id().0);
            out.extend_from_slice(&draft.change.0);
            out.extend_from_slice(&draft.descriptor.size_bytes.to_le_bytes());
            out.extend_from_slice(&draft.descriptor.format_version.to_le_bytes());
            out.extend_from_slice(&draft.order.to_le_bytes());
            put_count(&mut out, draft.parents.len());
            for parent in &draft.parents {
                out.extend_from_slice(&parent.0)
            }
            if out.len().saturating_add(45) > limits.max_segment_bytes {
                return Err(Error::Limit);
            }
        }
        let id = *blake3::hash(&out).as_bytes();
        let bytes = envelope(1, &id, &out);
        install(&self.segments, &hex(&id), &bytes, limits.max_segment_bytes)?;
        Ok(id)
    }
    pub(super) fn operation(
        &self,
        id: &[u8; 64],
        limits: &Limits,
    ) -> Result<Vec<SegmentId>, Error> {
        self.operation_export(id, limits, None)
    }
    fn operation_export(
        &self,
        id: &[u8; 64],
        limits: &Limits,
        mut budget: Option<&mut ExportReadBudget>,
    ) -> Result<Vec<SegmentId>, Error> {
        let cap = match budget.as_deref_mut() {
            Some(b) => b.cap(limits.max_segment_bytes)?,
            None => limits.max_segment_bytes,
        };
        let bytes = match self.operations.read(hex(id), cap) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::MissingOperation);
            }
            Err(e)
                if cap < limits.max_segment_bytes
                    && matches!(
                        e.kind(),
                        std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
                    ) =>
            {
                return Err(Error::Limit);
            }
            Err(e) => return Err(e.into()),
        };
        if let Some(b) = budget {
            b.charge(bytes.len())?;
        }
        let mut reader = Reader(payload(3, id, &bytes)?);
        let n = reader.count_width(limits.max_segments, 32)?;
        let mut tips = Vec::with_capacity(n);
        for _ in 0..n {
            tips.push(reader.array()?)
        }
        reader.end()?;
        if tips.is_empty() {
            return Err(Error::Corrupt);
        }
        Ok(tips)
    }
    pub(super) fn associate(
        &self,
        id: &[u8; 64],
        tips: &[SegmentId],
        limits: &Limits,
    ) -> Result<(), Error> {
        require_durable_writer()?;
        if tips.is_empty() || tips.len() > limits.max_segments {
            return Err(Error::Limit);
        }
        if tips
            .len()
            .checked_mul(32)
            .and_then(|n| n.checked_add(49))
            .is_none_or(|n| n > limits.max_segment_bytes)
        {
            return Err(Error::Limit);
        }
        let _lock = lock(&self.root)?;
        let mut body = Vec::with_capacity(4 + 32 * tips.len());
        put_count(&mut body, tips.len());
        for tip in tips {
            body.extend_from_slice(tip)
        }
        install(
            &self.operations,
            &hex(id),
            &envelope(3, id, &body),
            limits.max_segment_bytes,
        )
    }
    pub(super) fn load(
        &self,
        tips: &[SegmentId],
        limits: &Limits,
    ) -> Result<(Vec<m::CommitId>, Vec<Arc<Draft>>), Error> {
        let (roots, drafts, _) = self.load_layout(tips, limits)?;
        Ok((roots, drafts))
    }
    fn load_layout(
        &self,
        tips: &[SegmentId],
        limits: &Limits,
    ) -> Result<(Vec<m::CommitId>, Vec<Arc<Draft>>, Layout), Error> {
        self.load_layout_export(tips, limits, None)
    }
    pub(super) fn export_operation(
        &self,
        id: &[u8; 64],
        limits: &Limits,
        budget: &mut ExportReadBudget,
    ) -> Result<(Vec<m::CommitId>, Vec<Arc<Draft>>), Error> {
        let tips = self.operation_export(id, limits, Some(&mut *budget))?;
        let (roots, drafts, _) = self.load_layout_export(&tips, limits, Some(&mut *budget))?;
        Ok((roots, drafts))
    }
    fn load_layout_export(
        &self,
        tips: &[SegmentId],
        limits: &Limits,
        mut budget: Option<&mut ExportReadBudget>,
    ) -> Result<(Vec<m::CommitId>, Vec<Arc<Draft>>, Layout), Error> {
        let mut layout = Layout::default();
        if tips.is_empty() || tips.len() > limits.max_segments {
            return Err(Error::Limit);
        }
        let mut pending: Vec<_> = tips.iter().map(|id| (*id, 0usize)).collect();
        let mut visited = BTreeSet::new();
        let mut roots = BTreeSet::new();
        let mut drafts: BTreeMap<m::CommitId, Arc<Draft>> = BTreeMap::new();
        let mut total = 0usize;
        while let Some((id, depth)) = pending.pop() {
            if visited.contains(&id) {
                continue;
            }
            if depth > limits.max_depth || visited.len() >= limits.max_segments {
                return Err(Error::Limit);
            }
            visited.insert(id);
            let cap = match budget.as_deref_mut() {
                Some(b) => b.cap(limits.max_segment_bytes)?,
                None => limits.max_segment_bytes,
            };
            let bytes = self.segments.read(hex(&id), cap).map_err(|e| {
                if cap < limits.max_segment_bytes
                    && matches!(
                        e.kind(),
                        std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput
                    )
                {
                    Error::Limit
                } else {
                    Error::Io(e)
                }
            })?;
            if let Some(b) = budget.as_deref_mut() {
                b.charge(bytes.len())?;
            }

            total = total.checked_add(bytes.len()).ok_or(Error::Limit)?;
            if total > limits.max_checkpoint_bytes {
                return Err(Error::Limit);
            }
            let body = payload(1, &id, &bytes)?;
            if blake3::hash(body).as_bytes() != &id {
                return Err(Error::Corrupt);
            }
            let mut r = Reader(body);
            let n = r.count_width(limits.max_segments, 32)?;
            if n > 0 {
                layout.linked_segments += 1;
                layout.linked_bytes += bytes.len();
            }
            if pending
                .len()
                .checked_add(n)
                .is_none_or(|n| n > limits.max_segments)
            {
                return Err(Error::Limit);
            }
            for _ in 0..n {
                pending.push((r.array()?, depth + 1))
            }
            let n = r.count_width(limits.max_roots, 32)?;
            for _ in 0..n {
                roots.insert(m::CommitId(r.array()?));
                if roots.len() > limits.max_roots {
                    return Err(Error::Limit);
                }
            }
            let n = r.count_width(limits.max_drafts, 70)?;
            for _ in 0..n {
                let commit = m::CommitId(r.array()?);
                let change = m::ChangeId(r.array()?);
                let size = u64::from_le_bytes(r.array()?);
                let version = u16::from_le_bytes(r.array()?);
                let order = u64::from_le_bytes(r.array()?);
                if order == 0 {
                    return Err(Error::Corrupt);
                }
                if commit == m::CommitId::ROOT
                    || size > limits.max_metadata_bytes as u64
                    || version != m::FORMAT_VERSION
                {
                    return Err(Error::Corrupt);
                }
                let n = r.count_width(limits.max_parents, 32)?;
                if n == 0 || size == 0 {
                    return Err(Error::Corrupt);
                }
                let mut parents = Vec::with_capacity(n);
                for _ in 0..n {
                    parents.push(m::CommitId(r.array()?))
                }
                let draft = Arc::new(Draft {
                    order,
                    descriptor: m::Descriptor {
                        key: m::ObjectKey {
                            kind: m::Kind::Commit,
                            id: m::ObjectId(commit.0),
                        },
                        size_bytes: size,
                        format_version: version,
                    },
                    change,
                    parents,
                });
                if let Some(old) = drafts.insert(commit, draft.clone()) {
                    if !old.canonical_eq(&draft) {
                        return Err(Error::Corrupt);
                    }
                    if old.order < draft.order {
                        drafts.insert(commit, old);
                    }
                }
                if drafts.len() > limits.max_drafts {
                    return Err(Error::Limit);
                }
            }
            r.end()?;
        }
        if roots.is_empty() {
            roots.insert(m::CommitId::ROOT);
        }
        layout.segments = visited.len();
        layout.bytes = total;
        Ok((
            roots.into_iter().collect(),
            order_drafts(drafts, limits)?,
            layout,
        ))
    }
    /// Preserve all parent meaning, including hidden/promoted drafts and original
    /// ordering. No old segment or operation association is replaced or deleted.
    pub(super) fn persist_checkpoint(
        &self,
        parents: &[SegmentId],
        added_roots: &[m::CommitId],
        added: &[Arc<Draft>],
        limits: &Limits,
    ) -> Result<Vec<SegmentId>, Error> {
        require_durable_writer()?;
        // Read through the destination storage, even for unchanged or merged
        // checkpoints. A missing/corrupt parent must never be hidden by flattening.
        let (roots, drafts, layout) = self.load_layout(parents, limits)?;
        if added_roots.is_empty() && added.is_empty() {
            return Ok(parents.to_vec());
        }
        let delta = encoded_size(parents.len(), added_roots.len(), added)?;
        let threshold = 64usize
            .min(limits.max_depth.saturating_sub(1).max(1))
            .min(limits.max_segments.saturating_sub(1).max(1));
        let flatten = layout.linked_segments >= threshold
            || layout.linked_bytes > limits.max_checkpoint_bytes / 4
            || layout
                .segments
                .checked_add(1)
                .is_none_or(|n| n > limits.max_segments)
            || layout
                .linked_segments
                .checked_add(1)
                .is_none_or(|n| n > limits.max_depth)
            || layout
                .bytes
                .checked_add(delta)
                .is_none_or(|n| n > limits.max_checkpoint_bytes)
            || delta > limits.max_segment_bytes;
        if !flatten {
            return Ok(vec![self.segment(parents, added_roots, added, limits)?]);
        }
        let mut all_roots: BTreeSet<_> = roots.into_iter().collect();
        for root in added_roots {
            all_roots.insert(*root);
            if all_roots.len() > limits.max_roots {
                return Err(Error::Limit);
            }
        }
        let mut all_drafts: BTreeMap<_, _> = drafts.into_iter().map(|d| (d.id(), d)).collect();
        for draft in added {
            if draft.parents.len() > limits.max_parents {
                return Err(Error::Limit);
            }
            if let Some(old) = all_drafts.get(&draft.id()) {
                if !old.canonical_eq(draft) {
                    return Err(Error::Corrupt);
                }
                if old.order <= draft.order {
                    continue;
                }
            }
            all_drafts.insert(draft.id(), draft.clone());
            if all_drafts.len() > limits.max_drafts {
                return Err(Error::Limit);
            }
        }
        let roots: Vec<_> = all_roots.into_iter().collect();
        let drafts = order_drafts(all_drafts, limits)?;
        // Plan exact encoded sizes before any shard publication. Ranges borrow
        // complete records; no per-shard copies or split parent vectors.
        let mut plan = Vec::new();
        let (mut root, mut draft, mut total) = (0usize, 0usize, 0usize);
        while root < roots.len() || draft < drafts.len() {
            if plan.len() >= limits.max_segments {
                return Err(Error::Limit);
            }
            let (r0, d0) = (root, draft);
            let mut bytes = 57usize;
            while root < roots.len()
                && bytes
                    .checked_add(32)
                    .is_some_and(|n| n <= limits.max_segment_bytes)
            {
                bytes += 32;
                root += 1;
            }
            while draft < drafts.len() {
                let record = encoded_size(0, 0, &drafts[draft..draft + 1])?
                    .checked_sub(57)
                    .ok_or(Error::Limit)?;
                let next = bytes.checked_add(record).ok_or(Error::Limit)?;
                if next > limits.max_segment_bytes {
                    break;
                }
                bytes = next;
                draft += 1;
            }
            if root == r0 && draft == d0 {
                return Err(Error::Limit);
            }
            total = total.checked_add(bytes).ok_or(Error::Limit)?;
            if total > limits.max_checkpoint_bytes {
                return Err(Error::Limit);
            }
            plan.push((r0, root, d0, draft));
        }
        if plan.is_empty() {
            return Err(Error::Corrupt);
        }
        // Operation records also have a bounded complete tip list.
        if plan
            .len()
            .checked_mul(32)
            .and_then(|n| n.checked_add(49))
            .is_none_or(|n| n > limits.max_segment_bytes)
        {
            return Err(Error::Limit);
        }
        let mut tips = Vec::with_capacity(plan.len());
        for (r0, r1, d0, d1) in plan {
            tips.push(self.segment(&[], &roots[r0..r1], &drafts[d0..d1], limits)?);
        }
        self.load(&tips, limits)?;
        Ok(tips)
    }
}
fn put_count(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u32).to_le_bytes())
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let (head, tail) = self.0.split_at_checked(N).ok_or(Error::Corrupt)?;
        self.0 = tail;
        head.try_into().map_err(|_| Error::Corrupt)
    }
    fn count_width(&mut self, cap: usize, width: usize) -> Result<usize, Error> {
        let n = u32::from_le_bytes(self.array()?) as usize;
        if n > cap
            || n.checked_mul(width)
                .is_none_or(|bytes| bytes > self.0.len())
        {
            return Err(Error::Limit);
        }
        Ok(n)
    }
    fn end(self) -> Result<(), Error> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Corrupt)
        }
    }
}
pub(super) fn order_drafts(
    drafts: BTreeMap<m::CommitId, Arc<Draft>>,
    limits: &Limits,
) -> Result<Vec<Arc<Draft>>, Error> {
    if drafts.len() > limits.max_drafts {
        return Err(Error::Limit);
    }
    let edge_bound = drafts.values().try_fold(0usize, |sum, draft| {
        if draft.parents.len() > limits.max_parents {
            return Err(Error::Limit);
        }
        sum.checked_add(draft.parents.len()).ok_or(Error::Limit)
    })?;
    let work_bound = edge_bound
        .checked_add(drafts.len().checked_mul(2).ok_or(Error::Limit)?)
        .ok_or(Error::Limit)?;
    let mut work = 0usize;
    let mut done = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let mut output = Vec::with_capacity(drafts.len());
    let mut ordered: Vec<_> = drafts.keys().copied().collect();
    ordered.sort_by_key(|id| (drafts[id].order, *id));
    for id in ordered {
        let mut stack = vec![(id, false, 0usize)];
        while let Some((id, exit, depth)) = stack.pop() {
            work = work.checked_add(1).ok_or(Error::Limit)?;
            if work > work_bound {
                return Err(Error::Limit);
            }
            if done.contains(&id) {
                continue;
            }
            if exit {
                visiting.remove(&id);
                done.insert(id);
                output.push(drafts[&id].clone());
                continue;
            }
            if !visiting.insert(id) {
                return Err(Error::Corrupt);
            }
            if depth > limits.max_drafts {
                return Err(Error::Limit);
            }
            let draft = &drafts[&id];
            if stack
                .len()
                .checked_add(draft.parents.len())
                .is_none_or(|n| n > limits.max_drafts + limits.max_parents)
            {
                return Err(Error::Limit);
            }
            stack.push((id, true, depth));
            for parent in draft.parents.iter().rev() {
                if drafts.contains_key(parent) && !done.contains(parent) {
                    stack.push((*parent, false, depth + 1))
                }
            }
        }
    }
    Ok(output)
}

pub(super) fn prepare_fresh_directory(path: &Path) -> Result<(), Error> {
    require_durable_writer()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if !path.is_absolute()
            || path.as_os_str().len() > 4096
            || path.components().any(|v| {
                matches!(
                    v,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(Error::Invalid);
        }
        let parent = private_fs::PrivateDir::open_existing(path.parent().ok_or(Error::Invalid)?)?;
        match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                if !metadata.is_dir()
                    || metadata.file_type().is_symlink()
                    || metadata.uid() != std::fs::metadata(parent.path())?.uid()
                {
                    return Err(Error::Invalid);
                }
                if std::fs::read_dir(path)?.next().is_some() {
                    return Err(Error::Corrupt);
                }
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
                File::open(path)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
