//! Complete private Legacy backup bytes. Semantic verification is deliberately
//! separate from authenticated capsule/source authority and target staging.
use crate::capture::{
    CapturedBlob, LocalCaptureSink, OwnedCaptureBlob, PrivateLegacyCaptureCandidate,
};
use anyhow::{Context, Result, bail, ensure};
use futures::AsyncReadExt;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, VerifiedMetadata,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tinytree_cloud as c;
use tinytree_model as m;
type Owner = Arc<dyn std::any::Any + Send + Sync>;
const MAGIC: &[u8; 8] = b"TTPRIV1\0";
const END: u8 = 255;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateReviewBase {
    pub view_version: [u8; 32],
    pub policy_epoch: u64,
    pub base: [u8; 32],
    pub tree: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCaptureState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_base: Option<PrivateReviewBase>,
    pub operation_heads: Vec<String>,
    pub working_copy: c::PortableWorkingCopy,
    pub operations: Vec<c::OperationRecord>,
    pub roots: Vec<c::CapturedRoot>,
    pub remote: Option<c::PortableRemoteState>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPrivateManifest {
    pub schema_version: u32,
    pub codec: String,
    pub workspace_id: [u8; 32],
    pub source_workspace_id: [u8; 32],
    pub exporter: [u8; 16],
    pub generation: u64,
    pub capture: LegacyCaptureState,
}
#[derive(Clone, Debug)]
pub struct BundleLimits {
    pub max_bytes: u64,
    pub max_entries: usize,
    pub max_metadata_bytes: usize,
    pub max_file_bytes: usize,
    pub max_edges: usize,
    pub max_depth: usize,
    pub max_elapsed_ms: u64,
}
impl Default for BundleLimits {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            max_entries: 100_000,
            max_metadata_bytes: 32 * 1024 * 1024,
            max_file_bytes: 1024 * 1024,
            max_edges: 1_000_000,
            max_depth: 128,
            max_elapsed_ms: 300_000,
        }
    }
}
#[derive(Clone, Debug)]
struct Entry {
    offset: u64,
    size: u64,
    hash: [u8; 32],
}
/// File-backed semantic proof. No constructor accepts a public JSON ACK as authority.
pub struct VerifiedPrivateBundle {
    manifest: LegacyPrivateManifest,
    manifest_bytes: Vec<u8>,
    operations: Vec<c::VerifiedObject>,
    native: Vec<m::Descriptor>,
    reader: Arc<BundleReader>,
    size: u64,
    digest: [u8; 32],
}
impl VerifiedPrivateBundle {
    pub fn manifest(&self) -> &LegacyPrivateManifest {
        &self.manifest
    }
    pub fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }
    pub fn capture_data(&self) -> &LegacyCaptureState {
        &self.manifest.capture
    }
    pub fn operation_heads(&self) -> &[String] {
        &self.manifest.capture.operation_heads
    }
    pub fn working_copy(&self) -> &c::PortableWorkingCopy {
        &self.manifest.capture.working_copy
    }
    pub fn review_base(&self) -> Option<&PrivateReviewBase> {
        self.manifest.capture.review_base.as_ref()
    }
    pub fn remote(&self) -> Option<&c::PortableRemoteState> {
        self.manifest.capture.remote.as_ref()
    }
    pub fn operations(&self) -> &[c::VerifiedObject] {
        &self.operations
    }
    pub fn native_descriptors(&self) -> &[m::Descriptor] {
        &self.native
    }
    pub fn native_reader(&self) -> Arc<dyn NativeObjectIo> {
        self.reader.clone()
    }
    pub fn size_bytes(&self) -> u64 {
        self.size
    }
    pub fn blake3(&self) -> [u8; 32] {
        self.digest
    }
    pub fn file(&self) -> Result<File> {
        let mut file = self
            .reader
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("bundle file poisoned"))?
            .try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
}
pub struct ProducedPrivateBundle {
    verified: VerifiedPrivateBundle,
    _owner: Owner,
}
impl ProducedPrivateBundle {
    pub fn verified(&self) -> &VerifiedPrivateBundle {
        &self.verified
    }
    pub fn into_verified(self) -> VerifiedPrivateBundle {
        self.verified
    }
    pub fn file(&self) -> Result<File> {
        self.verified.file()
    }
    pub fn size_bytes(&self) -> u64 {
        self.verified.size_bytes()
    }
    pub fn blake3(&self) -> [u8; 32] {
        self.verified.blake3()
    }
}
pub struct PrivateBundleWriter {
    file: File,
    limits: BundleLimits,
    size: u64,
    entries: usize,
    metadata: usize,
    seen: BTreeSet<(u8, Vec<u8>)>,
    started: Instant,
}
impl BundleLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=512 * 1024 * 1024).contains(&self.max_bytes)
                && (1..=100_000).contains(&self.max_entries)
                && (1..=32 * 1024 * 1024).contains(&self.max_metadata_bytes)
                && (1..=1024 * 1024).contains(&self.max_file_bytes)
                && (1..=8_000_000).contains(&self.max_edges)
                && (1..=128).contains(&self.max_depth)
                && (1..=3_600_000).contains(&self.max_elapsed_ms),
            "private bundle bounds"
        );
        Ok(())
    }
    fn model(&self) -> m::Limits {
        m::Limits {
            max_object_bytes: self.max_file_bytes,
            max_items: self.max_entries,
            max_field_bytes: self.max_file_bytes,
            max_path_bytes: 4096,
        }
    }
    fn cloud(&self) -> c::Limits {
        let mut l = c::Limits::default();
        l.max_payload_bytes = self.max_metadata_bytes;
        l.max_decode_bytes = self
            .max_metadata_bytes
            .saturating_mul(16)
            .min(512 * 1024 * 1024);
        l
    }
}
fn checked_key(k: m::ObjectKey) -> (u8, [u8; 32]) {
    (k.kind as u8, k.id.0)
}
fn parse_id<const N: usize>(s: &str) -> Result<[u8; N]> {
    ensure!(
        s.len() == N * 2
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "private bundle identity"
    );
    let mut out = [0; N];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}
fn deadline(start: Instant, l: &BundleLimits) -> Result<()> {
    ensure!(
        start.elapsed() < Duration::from_millis(l.max_elapsed_ms),
        "private bundle elapsed limit"
    );
    Ok(())
}
impl PrivateBundleWriter {
    pub fn new(mut file: File, limits: BundleLimits) -> Result<Self> {
        limits.validate()?;
        ensure!(
            file.metadata()?.is_file() && file.metadata()?.len() == 0,
            "private bundle requires empty owned file"
        );
        file.seek(SeekFrom::Start(0))?;
        file.write_all(MAGIC)?;
        Ok(Self {
            file,
            limits,
            size: 8,
            entries: 0,
            metadata: 0,
            seen: BTreeSet::new(),
            started: Instant::now(),
        })
    }
    fn frame(&mut self, kind: u8, id: &[u8], bytes: &[u8]) -> Result<()> {
        let hash = *blake3::hash(bytes).as_bytes();
        self.header(kind, id, bytes.len() as u64, hash)?;
        self.file.write_all(bytes)?;
        Ok(())
    }
    fn header(&mut self, kind: u8, id: &[u8], size: u64, hash: [u8; 32]) -> Result<()> {
        deadline(self.started, &self.limits)?;
        ensure!(
            self.entries < self.limits.max_entries
                && id.len() <= 64
                && self.seen.insert((kind, id.to_vec())),
            "private bundle duplicate/entry budget"
        );
        let next = self
            .size
            .checked_add(42 + id.len() as u64)
            .and_then(|n| n.checked_add(size))
            .context("bundle length overflow")?;
        ensure!(next < self.limits.max_bytes, "private bundle byte limit");
        if kind != 17 {
            self.metadata = self
                .metadata
                .checked_add(usize::try_from(size)?)
                .filter(|n| *n <= self.limits.max_metadata_bytes)
                .context("private bundle metadata limit")?;
        }
        self.file.write_all(&[kind, id.len() as u8])?;
        self.file.write_all(id)?;
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&hash)?;
        self.size = next;
        self.entries += 1;
        Ok(())
    }
    pub async fn finish(
        mut self,
        candidate: PrivateLegacyCaptureCandidate,
        native: Arc<dyn NativeObjectIo>,
        identity: c::CaptureExportIdentity,
        owner: Owner,
        limits: BundleLimits,
    ) -> Result<ProducedPrivateBundle> {
        limits.validate()?;
        ensure!(
            identity.parents.is_empty(),
            "private bundle has no external manifest dependencies"
        );
        ensure!(
            limits.max_bytes == self.limits.max_bytes
                && limits.max_entries == self.limits.max_entries
                && limits.max_metadata_bytes == self.limits.max_metadata_bytes
                && limits.max_file_bytes == self.limits.max_file_bytes
                && limits.max_edges == self.limits.max_edges
                && limits.max_depth == self.limits.max_depth
                && limits.max_elapsed_ms == self.limits.max_elapsed_ms,
            "private bundle writer bounds changed"
        );
        let source = Arc::new(candidate);
        let owner: Owner = Arc::new((owner, source.clone()));
        let mut pending = source
            .state
            .roots
            .iter()
            .map(|r| {
                Ok(m::ObjectKey {
                    kind: m::Kind::try_from(r.kind)?,
                    id: m::ObjectId(r.id),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut seen = BTreeSet::new();
        let mut work = 0usize;
        while let Some(key) = pending.pop() {
            deadline(self.started, &limits)?;
            if key.kind == m::Kind::Commit && key.id.0 == [0; 32] {
                continue;
            }
            if !seen.insert(checked_key(key)) {
                continue;
            }
            ensure!(
                seen.len() <= limits.max_entries,
                "private native object limit"
            );
            let d = native
                .descriptor_with_owner(key, owner.clone())
                .await?
                .context("private native object missing")?;
            ensure!(
                d.key == key
                    && d.format_version == m::FORMAT_VERSION
                    && d.size_bytes <= limits.max_file_bytes as u64,
                "private native descriptor bound"
            );
            if key.kind == m::Kind::File {
                let read = native
                    .read_file_with_owner(m::FileId(key.id.0), owner.clone())
                    .await?;
                ensure!(read.descriptor() == d, "private file descriptor changed");
                let (_, mut input) = read.into_parts();
                self.header(17, &key.id.0, d.size_bytes, key.id.0)?;
                let mut hash = blake3::Hasher::new();
                let mut remaining = d.size_bytes;
                let mut buffer = vec![0u8; 65536];
                while remaining > 0 {
                    deadline(self.started, &limits)?;
                    let max = remaining.min(buffer.len() as u64) as usize;
                    let n = input.read(&mut buffer[..max]).await?;
                    ensure!(n > 0, "private FILE truncated");
                    self.file.write_all(&buffer[..n])?;
                    hash.update(&buffer[..n]);
                    remaining -= n as u64;
                }
                let mut extra = [0];
                ensure!(
                    input.read(&mut extra).await? == 0 && hash.finalize().as_bytes() == &key.id.0,
                    "private FILE hash/size mismatch"
                );
            } else {
                let value = native.read_metadata_with_owner(key, owner.clone()).await?;
                ensure!(value.descriptor() == d, "private metadata changed");
                m::verify(&d, value.bytes(), limits.model())?;
                let object = m::decode(value.bytes(), limits.model())?;
                let refs = object.references();
                work = work
                    .checked_add(refs.len())
                    .filter(|n| *n <= limits.max_edges)
                    .context("private native edge limit")?;
                pending.extend(refs);
                self.frame(16 + key.kind as u8, &key.id.0, value.bytes())?;
            }
        }
        let manifest = LegacyPrivateManifest {
            schema_version: 1,
            codec: c::CODEC.into(),
            workspace_id: identity.workspace,
            source_workspace_id: source.source_workspace,
            exporter: identity.exporter,
            generation: identity.sequence,
            capture: source.state.clone(),
        };
        let bytes = serde_json::to_vec(&manifest)?;
        self.frame(6, &[], &bytes)?;
        self.file.write_all(&[END])?;
        self.file.sync_all()?;
        let digest = hash_file(&mut self.file, &limits, self.started)?;
        let verified = verify_file_until(self.file, digest, limits, self.started)?;
        Ok(ProducedPrivateBundle {
            verified,
            _owner: owner,
        })
    }
}
#[async_trait::async_trait]
impl LocalCaptureSink for PrivateBundleWriter {
    async fn write(&mut self, blob: OwnedCaptureBlob) -> Result<()> {
        match blob.blob() {
            CapturedBlob::OperationView(v) => {
                let (kind, id) = match v.id() {
                    c::ObjectId::Operation(id) => (1, id),
                    c::ObjectId::View(id) => (2, id),
                };
                self.frame(kind, &id, blob.bytes())?;
            }
            CapturedBlob::Index(_) => {
                bail!("private Legacy capture cannot contain federated index artifacts")
            }
        };
        Ok(())
    }
}
fn hash_file(file: &mut File, l: &BundleLimits, start: Instant) -> Result<[u8; 32]> {
    let size = file.metadata()?.len();
    ensure!(size <= l.max_bytes, "private bundle file bound");
    file.seek(SeekFrom::Start(0))?;
    let mut hash = blake3::Hasher::new();
    let mut buffer = vec![0u8; 65536];
    let mut read = 0u64;
    loop {
        deadline(start, l)?;
        let n = Read::read(file, &mut buffer)?;
        if n == 0 {
            break;
        }
        read = read
            .checked_add(n as u64)
            .filter(|n| *n <= l.max_bytes)
            .context("private bundle grew")?;
        hash.update(&buffer[..n]);
    }
    ensure!(read == size, "private bundle size changed");
    Ok(*hash.finalize().as_bytes())
}
fn read_array<const N: usize>(file: &mut File) -> Result<[u8; N]> {
    let mut b = [0; N];
    file.read_exact(&mut b)?;
    Ok(b)
}
#[derive(Debug)]
struct BundleReader {
    file: Mutex<File>,
    entries: BTreeMap<(u8, [u8; 32]), Entry>,
    limits: BundleLimits,
}
impl BundleReader {
    fn bytes(&self, key: m::ObjectKey) -> Result<(m::Descriptor, Vec<u8>)> {
        let e = self
            .entries
            .get(&checked_key(key))
            .context("private object absent")?;
        ensure!(
            e.size <= self.limits.max_file_bytes as u64,
            "private object read bound"
        );
        let mut bytes = vec![0; usize::try_from(e.size)?];
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("bundle file poisoned"))?;
        // Independent positional reads do not disturb a concurrently cloned upload reader.
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut bytes, e.offset)?;
        }
        #[cfg(not(unix))]
        {
            file.seek(SeekFrom::Start(e.offset))?;
            file.read_exact(&mut bytes)?;
        }
        let d = m::Descriptor {
            key,
            size_bytes: e.size,
            format_version: m::FORMAT_VERSION,
        };
        m::verify(&d, &bytes, self.limits.model())?;
        ensure!(
            blake3::hash(&bytes).as_bytes() == &e.hash,
            "private entry changed"
        );
        Ok((d, bytes))
    }
    fn object(&self, key: m::ObjectKey) -> Result<m::Object> {
        let (_, bytes) = self.bytes(key)?;
        Ok(m::decode(&bytes, self.limits.model())?)
    }
}
struct OwnedBytes {
    inner: futures::io::Cursor<Vec<u8>>,
    _owner: Owner,
}
impl futures::AsyncRead for OwnedBytes {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        output: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, output)
    }
}
fn backend(error: impl std::fmt::Display) -> jj_lib::backend::BackendError {
    jj_lib::backend::BackendError::Other(Box::new(std::io::Error::other(error.to_string())))
}
#[async_trait::async_trait]
impl NativeObjectIo for BundleReader {
    async fn descriptor(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        Ok(self.entries.get(&checked_key(key)).map(|e| m::Descriptor {
            key,
            size_bytes: e.size,
            format_version: m::FORMAT_VERSION,
        }))
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        _owner: Owner,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.descriptor(key).await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<VerifiedMetadata> {
        let (d, bytes) = self.bytes(key).map_err(backend)?;
        VerifiedMetadata::verify(d, bytes, self.limits.model())
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        _owner: Owner,
    ) -> jj_lib::backend::BackendResult<VerifiedMetadata> {
        self.read_metadata(key).await
    }
    async fn write_metadata(&self, _: m::Object) -> jj_lib::backend::BackendResult<m::Descriptor> {
        Err(backend("private bundle is read-only"))
    }
    async fn read_file(&self, id: m::FileId) -> jj_lib::backend::BackendResult<FileRead> {
        let (d, bytes) = self
            .bytes(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            })
            .map_err(backend)?;
        FileRead::from_preverified_pinned_source(d, Box::pin(futures::io::Cursor::new(bytes)))
    }
    async fn read_file_with_owner(
        &self,
        id: m::FileId,
        owner: Owner,
    ) -> jj_lib::backend::BackendResult<FileRead> {
        let (d, bytes) = self
            .bytes(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            })
            .map_err(backend)?;
        FileRead::from_preverified_pinned_source(
            d,
            Box::pin(OwnedBytes {
                inner: futures::io::Cursor::new(bytes),
                _owner: owner,
            }),
        )
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Send + Unpin),
        _: u64,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        Err(backend("private bundle is read-only"))
    }
    async fn copy_catalog_snapshot(
        &self,
        l: CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<CopyCatalogSnapshot> {
        let keys = self
            .entries
            .keys()
            .filter(|k| k.0 == m::Kind::CopyHistory as u8)
            .copied()
            .collect::<Vec<_>>();
        if keys.len() > l.max_objects {
            return Err(backend("private copy catalog limit"));
        }
        let mut entries = vec![];
        let mut bytes = 0usize;
        for (k, id) in keys {
            let key = m::ObjectKey {
                kind: m::Kind::try_from(k).map_err(backend)?,
                id: m::ObjectId(id),
            };
            let d = self.descriptor(key).await?.unwrap();
            bytes = bytes
                .checked_add(d.size_bytes as usize)
                .filter(|n| *n <= l.max_metadata_bytes)
                .ok_or_else(|| backend("private copy catalog bytes"))?;
            entries.push(self.read_metadata(key).await?);
        }
        CopyCatalogSnapshot::from_verified(entries, l)
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        l: CopyCatalogLimits,
        _owner: Owner,
    ) -> jj_lib::backend::BackendResult<CopyCatalogSnapshot> {
        self.copy_catalog_snapshot(l).await
    }
}
/// Verify complete private file semantics; this does NOT authenticate its origin.
pub fn verify_file(
    file: File,
    expected_blake3: [u8; 32],
    limits: BundleLimits,
) -> Result<VerifiedPrivateBundle> {
    verify_file_until(file, expected_blake3, limits, Instant::now())
}
fn verify_file_until(
    mut file: File,
    expected_blake3: [u8; 32],
    limits: BundleLimits,
    started: Instant,
) -> Result<VerifiedPrivateBundle> {
    limits.validate()?;
    let size = file.metadata()?.len();
    ensure!(size > 9 && size <= limits.max_bytes, "private bundle size");
    let digest = hash_file(&mut file, &limits, started)?;
    ensure!(digest == expected_blake3, "private bundle transfer digest");
    file.seek(SeekFrom::Start(0))?;
    ensure!(
        &read_array::<8>(&mut file)? == MAGIC,
        "private bundle version"
    );
    let mut entries = BTreeMap::new();
    let mut seen = BTreeSet::new();
    let mut operations = vec![];
    let mut metadata = 0usize;
    let mut count = 0usize;
    let mut manifest_bytes = None;
    let mut retained = 0usize;
    loop {
        deadline(started, &limits)?;
        let kind = read_array::<1>(&mut file)?[0];
        if kind == END {
            ensure!(
                file.stream_position()? == size,
                "private bundle trailing bytes"
            );
            break;
        }
        ensure!(
            manifest_bytes.is_none(),
            "private manifest must be final frame"
        );
        count += 1;
        ensure!(count <= limits.max_entries, "private bundle entry limit");
        let width = read_array::<1>(&mut file)?[0] as usize;
        ensure!(
            (matches!(kind, 1 | 2) && width == 64)
                || (kind == 6 && width == 0)
                || ((17..=21).contains(&kind) && width == 32),
            "private bundle frame kind"
        );
        let mut id = vec![0; width];
        file.read_exact(&mut id)?;
        ensure!(
            seen.insert((kind, id.clone())),
            "private bundle duplicate entry"
        );
        let n = u64::from_le_bytes(read_array::<8>(&mut file)?);
        let hash = read_array::<32>(&mut file)?;
        let offset = file.stream_position()?;
        ensure!(
            offset.checked_add(n).is_some_and(|end| end < size),
            "private bundle frame length"
        );
        if kind != 17 {
            metadata = metadata
                .checked_add(usize::try_from(n)?)
                .filter(|n| *n <= limits.max_metadata_bytes)
                .context("private metadata limit")?;
        } else {
            ensure!(n <= limits.max_file_bytes as u64, "private FILE limit");
        }
        let e = Entry {
            offset,
            size: n,
            hash,
        };
        if kind == 17 {
            let mut h = blake3::Hasher::new();
            let mut remaining = n;
            let mut buffer = vec![0; 65536];
            while remaining > 0 {
                deadline(started, &limits)?;
                let amount = remaining.min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..amount])?;
                h.update(&buffer[..amount]);
                remaining -= amount as u64;
            }
            ensure!(
                h.finalize().as_bytes() == &hash && id.as_slice() == hash.as_slice(),
                "private FILE identity"
            );
        } else {
            ensure!(
                kind <= 6 || n <= limits.max_file_bytes as u64,
                "private native metadata size"
            );
            let mut bytes = vec![0; usize::try_from(n)?];
            file.read_exact(&mut bytes)?;
            ensure!(
                blake3::hash(&bytes).as_bytes() == &hash,
                "private frame digest"
            );
            match kind {
                1 | 2 => {
                    let raw: [u8; 64] = id.as_slice().try_into()?;
                    let object_id = if kind == 1 {
                        c::ObjectId::Operation(raw)
                    } else {
                        c::ObjectId::View(raw)
                    };
                    let object = c::VerifiedObject::verify(
                        object_id,
                        c::BlobDigest::of(&bytes),
                        &bytes,
                        limits.cloud(),
                    )?;
                    retained = retained
                        .checked_add(object.retained_size_upper_bound()?)
                        .filter(|n| *n <= 256 * 1024 * 1024)
                        .context("private OP/VIEW memory bound")?;
                    operations.push(object);
                }
                6 => manifest_bytes = Some(bytes),
                18..=21 => {
                    let key = m::ObjectKey {
                        kind: m::Kind::try_from(kind - 16)?,
                        id: m::ObjectId(id.as_slice().try_into()?),
                    };
                    m::verify(
                        &m::Descriptor {
                            key,
                            size_bytes: n,
                            format_version: m::FORMAT_VERSION,
                        },
                        &bytes,
                        limits.model(),
                    )?;
                }
                _ => unreachable!(),
            }
        }
        if (17..=21).contains(&kind) {
            let key = (kind - 16, id.as_slice().try_into()?);
            ensure!(
                entries.insert(key, e).is_none(),
                "private duplicate native identity"
            );
        }
    }
    let manifest_bytes = manifest_bytes.context("private manifest missing")?;
    let manifest: LegacyPrivateManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        serde_json::to_vec(&manifest)? == manifest_bytes,
        "private manifest must be canonical"
    );
    operations.sort_by_key(c::VerifiedObject::id);
    let reader = Arc::new(BundleReader {
        file: Mutex::new(file),
        entries,
        limits: limits.clone(),
    });
    verify_semantics(&manifest, &operations, &reader, &limits, started)?;
    let native = reader
        .entries
        .iter()
        .map(|((kind, id), e)| {
            Ok(m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::try_from(*kind)?,
                    id: m::ObjectId(*id),
                },
                size_bytes: e.size,
                format_version: m::FORMAT_VERSION,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(VerifiedPrivateBundle {
        manifest,
        manifest_bytes,
        operations,
        native,
        reader,
        size,
        digest,
    })
}
fn verify_semantics(
    manifest: &LegacyPrivateManifest,
    objects: &[c::VerifiedObject],
    reader: &BundleReader,
    limits: &BundleLimits,
    started: Instant,
) -> Result<()> {
    use jj_lib::object_id::ObjectId as _;
    ensure!(
        manifest.schema_version == 1
            && manifest.codec == c::CODEC
            && manifest.generation > 0
            && manifest.workspace_id != [0; 32]
            && manifest.source_workspace_id != [0; 32],
        "private manifest version/generation"
    );
    let state = &manifest.capture;
    ensure!(
        !state.operation_heads.is_empty()
            && state.operation_heads.len() <= limits.max_entries
            && state.operation_heads.windows(2).all(|v| v[0] < v[1])
            && state.operations.len() <= limits.max_entries
            && state.roots.len() <= limits.max_entries,
        "private capture inventory bounds"
    );
    ensure!(
        state
            .operations
            .first()
            .is_some_and(|o| o.operation == "00".repeat(64)
                && o.view == "00".repeat(64)
                && o.parents.is_empty())
            && state
                .operations
                .windows(2)
                .all(|v| v[0].operation < v[1].operation),
        "private operation inventory ordering"
    );
    let table = objects
        .iter()
        .map(|o| (o.id(), o))
        .collect::<BTreeMap<_, _>>();
    let mut operation_ids = BTreeSet::new();
    let mut records = vec![];
    let mut work = 0usize;
    for record in &state.operations {
        deadline(started, limits)?;
        let id = parse_id::<64>(&record.operation)?;
        operation_ids.insert(id);
        if id == [0; 64] {
            continue;
        }
        let object = table
            .get(&c::ObjectId::Operation(id))
            .context("private declared OP missing")?;
        let actual = object.operation().context("private OP kind")?;
        ensure!(
            actual.view_id.as_bytes() == parse_id::<64>(&record.view)?.as_slice()
                && actual.parents.len() == record.parents.len(),
            "private OP/view binding"
        );
        for (parent, declared) in actual.parents.iter().zip(&record.parents) {
            ensure!(
                parent.as_bytes() == parse_id::<64>(declared)?.as_slice(),
                "private OP parent binding"
            );
        }
        records.push(id);
        work = work
            .checked_add(record.parents.len())
            .filter(|n| *n <= limits.max_edges)
            .context("private operation edge limit")?;
    }
    ensure!(
        records.len()
            == objects
                .iter()
                .filter(|o| matches!(o.id(), c::ObjectId::Operation(_)))
                .count(),
        "private undeclared operation"
    );
    let wc = &state.working_copy;
    ensure!(
        wc.wc_operation == wc.snapshot_operation,
        "private WC operation mismatch"
    );
    let mut roots = BTreeSet::new();
    for root in &state.roots {
        let kind = m::Kind::try_from(root.kind)?;
        ensure!(
            !(kind == m::Kind::Commit && root.id == [0; 32]) && roots.insert((root.kind, root.id)),
            "private root duplicate/intrinsic"
        );
    }
    ensure!(
        state
            .roots
            .windows(2)
            .all(|v| (v[0].kind, v[0].id) < (v[1].kind, v[1].id)),
        "private root ordering"
    );
    let mut coverage_roots = BTreeSet::new();
    for o in objects {
        for id in &o.dependencies().native_commits {
            if *id != [0; 32] {
                coverage_roots.insert((m::Kind::Commit as u8, *id));
            }
        }
    }
    for id in &wc.tree.terms {
        coverage_roots.insert((m::Kind::Tree as u8, *id));
    }
    ensure!(
        coverage_roots.is_subset(&roots),
        "private declared roots omit OP/WC dependencies"
    );
    let recovery = c::Manifest {
        workspace: manifest.workspace_id,
        exporter: manifest.exporter,
        sequence: manifest.generation,
        operation_heads: state
            .operation_heads
            .iter()
            .map(|id| parse_id::<64>(id))
            .collect::<Result<_>>()?,
        undo_operations: records,
        intent_operations: vec![],
        working_copy: c::WorkingCopy {
            workspace_name: wc.workspace.clone(),
            wc_operation: parse_id(&wc.wc_operation)?,
            snapshot_operation: parse_id(&wc.snapshot_operation)?,
            snapshot_commit: wc.snapshot_commit,
            tree: m::TreeMerge {
                terms: wc.tree.terms.iter().map(|id| m::TreeId(*id)).collect(),
                labels: if wc.tree.labels.is_empty() {
                    vec![String::new(); wc.tree.terms.len()]
                } else {
                    wc.tree.labels.clone()
                },
            },
        },
        parents: vec![],
        members: objects
            .iter()
            .map(|o| {
                Ok(c::Member {
                    id: o.id(),
                    selected: o.digest().context("private intrinsic payload")?,
                })
            })
            .collect::<Result<_>>()?,
        intent_roots: vec![],
        native_roots: coverage_roots
            .iter()
            .map(|(k, id)| {
                Ok(m::ObjectKey {
                    kind: m::Kind::try_from(*k)?,
                    id: m::ObjectId(*id),
                })
            })
            .collect::<Result<_>>()?,
        coverage: c::Coverage::FullRequired,
        detached: vec![],
    };
    let mut bounds = c::ManifestLimits::default();
    bounds.max_members = limits.max_entries;
    bounds.max_operation_roots = limits.max_entries;
    bounds.max_native_roots = limits.max_entries;
    bounds.max_edges = limits.max_edges;
    bounds.max_decode_bytes = 512 * 1024 * 1024;
    bounds.max_total_payload_bytes = limits.max_metadata_bytes as u64;
    c::verify_coverage(&recovery, objects, bounds)?;
    if let Some(remote) = &state.remote {
        ensure!(
            remote.observations.len() <= 128 && remote.archived_pushes.len() <= 128,
            "private remote bounds"
        );
        let mut refs = BTreeSet::new();
        let mut bookmarks = BTreeSet::new();
        for o in &remote.observations {
            crate::remote_state::validate_names(&o.reference, &o.bookmark)?;
            ensure!(
                refs.insert(&o.reference)
                    && bookmarks.insert(&o.bookmark)
                    && (o.head.is_none() || o.generation > 0),
                "private remote observation"
            );
            if let Some(id) = &o.head {
                required_remote_root(id, &roots)?;
            }
        }
        let mut requests = BTreeSet::new();
        for i in &remote.archived_pushes {
            crate::remote_state::validate_names(&i.reference, &i.bookmark)?;
            parse_id::<16>(&i.request_id)?;
            ensure!(
                requests.insert(&i.request_id)
                    && i.confirmed_generation
                        .is_none_or(|g| i.generation.checked_add(1) == Some(g))
                    && operation_ids.contains(&parse_id::<64>(&i.base_operation)?),
                "private archived remote intent"
            );
            required_remote_root(
                i.head
                    .as_ref()
                    .context("private archived push head absent")?,
                &roots,
            )?;
        }
        if let Some(landing) = &remote.landing {
            landing.validate()?;
            for op in landing.retained_operations() {
                ensure!(
                    operation_ids.contains(&parse_id::<64>(op)?),
                    "private landing operation missing"
                );
            }
            for id in landing.retained_commit_heads() {
                required_remote_root(id, &roots)?;
            }
        }
    }
    let mut seen = BTreeSet::new();
    let mut pending = roots.iter().copied().collect::<Vec<_>>();
    let mut trees = vec![];
    while let Some((kind, id)) = pending.pop() {
        deadline(started, limits)?;
        if kind == m::Kind::Commit as u8 && id == [0; 32] {
            continue;
        }
        if !seen.insert((kind, id)) {
            continue;
        }
        ensure!(
            seen.len() <= limits.max_entries,
            "private native traversal bound"
        );
        let key = m::ObjectKey {
            kind: m::Kind::try_from(kind)?,
            id: m::ObjectId(id),
        };
        let entry = reader
            .entries
            .get(&(kind, id))
            .context("private native closure missing")?;
        if key.kind == m::Kind::File {
            ensure!(
                entry.size <= limits.max_file_bytes as u64,
                "private file size"
            );
            continue;
        }
        let object = reader.object(key)?;
        let edges = object.references();
        work = work
            .checked_add(edges.len())
            .filter(|n| *n <= limits.max_edges)
            .context("private closure work limit")?;
        pending.extend(edges.into_iter().map(checked_key));
        if let m::Object::Commit(commit) = object {
            trees.extend(
                commit
                    .root_tree
                    .terms
                    .into_iter()
                    .map(|t| (t.0, String::new(), 0usize)),
            );
        }
    }
    ensure!(
        seen.len() == reader.entries.len(),
        "private unreferenced native payload"
    );
    for (kind, id) in &roots {
        if *kind == m::Kind::Tree as u8 {
            trees.push((*id, String::new(), 0));
        }
    }
    let actual_wc = if wc.snapshot_commit == [0; 32] {
        m::synthetic_root()
    } else {
        match reader.object(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(wc.snapshot_commit),
        })? {
            m::Object::Commit(c) => c,
            _ => bail!("private WC commit kind"),
        }
    };
    ensure!(
        actual_wc.root_tree == recovery.working_copy.tree,
        "private WC tree differs from actual native commit"
    );
    if let Some(base) = &state.review_base {
        ensure!(
            base.policy_epoch > 0
                && base.policy_epoch <= 9_007_199_254_740_991
                && base.base != [0; 32]
                && roots.contains(&(m::Kind::Commit as u8, base.base)),
            "private review base binding"
        );
        let m::Object::Commit(commit) = reader.object(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(base.base),
        })?
        else {
            bail!("private review base COMMIT kind")
        };
        ensure!(
            commit.root_tree.terms == [m::TreeId(base.tree)]
                && commit.root_tree.labels == [String::new()],
            "private review base TREE differs from COMMIT"
        );
    }
    let mut contexts = BTreeSet::new();
    let mut path_bytes = 0usize;
    let mut queued_path_bytes = 0usize;
    while let Some((id, path, depth)) = trees.pop() {
        deadline(started, limits)?;
        queued_path_bytes = queued_path_bytes
            .checked_sub(path.len())
            .context("private path accounting")?;
        if !contexts.insert((id, path.clone())) {
            continue;
        }
        path_bytes = path_bytes
            .checked_add(path.len())
            .filter(|n| *n <= limits.max_metadata_bytes)
            .context("private tree context bytes")?;
        ensure!(
            contexts.len() <= limits.max_edges && depth <= limits.max_depth && path.len() <= 4096,
            "private tree context limit"
        );
        let m::Object::Tree(tree) = reader.object(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(id),
        })?
        else {
            bail!("private tree kind")
        };
        for entry in tree.entries {
            work = work
                .checked_add(1)
                .filter(|n| *n <= limits.max_edges)
                .context("private tree validation work")?;
            let full = if path.is_empty() {
                entry.name
            } else {
                format!("{path}/{}", entry.name)
            };
            ensure!(full.len() <= 4096, "private path length");
            match entry.value {
                m::TreeValue::Tree(id) => {
                    queued_path_bytes = queued_path_bytes
                        .checked_add(full.len())
                        .filter(|n| n.saturating_add(path_bytes) <= limits.max_metadata_bytes)
                        .context("private queued path bound")?;
                    trees.push((id.0, full, depth + 1));
                }
                m::TreeValue::File {
                    id,
                    size_bytes,
                    copy_id,
                    ..
                } => {
                    ensure!(
                        reader
                            .entries
                            .get(&(m::Kind::File as u8, id.0))
                            .is_some_and(|d| d.size == size_bytes),
                        "private tree FILE size mismatch"
                    );
                    if let Some(id) = copy_id {
                        let m::Object::CopyHistory(c) = reader.object(m::ObjectKey {
                            kind: m::Kind::CopyHistory,
                            id: m::ObjectId(id.0),
                        })?
                        else {
                            bail!("private copy kind")
                        };
                        ensure!(c.current_path == full, "private copy path mismatch");
                    }
                }
                m::TreeValue::Symlink { id, target_bytes } => {
                    let m::Object::Symlink(target) = reader.object(m::ObjectKey {
                        kind: m::Kind::Symlink,
                        id: m::ObjectId(id.0),
                    })?
                    else {
                        bail!("private symlink kind")
                    };
                    ensure!(
                        target.len() as u64 == target_bytes,
                        "private symlink size mismatch"
                    );
                }
            }
        }
    }
    Ok(())
}
fn required_remote_root(id: &str, roots: &BTreeSet<(u8, [u8; 32])>) -> Result<()> {
    let id = parse_id::<32>(id)?;
    ensure!(
        id != [0; 32] && roots.contains(&(m::Kind::Commit as u8, id)),
        "private remote root missing"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(include_file: bool) -> Result<(File, [u8; 32], m::Descriptor)> {
        let limits = BundleLimits::default();
        let payload = b"unpublished private fixture bytes";
        let leaf = m::describe_file(payload);
        let tree = m::empty_tree();
        let manifest = LegacyPrivateManifest {
            schema_version: 1,
            codec: c::CODEC.into(),
            workspace_id: [1; 32],
            source_workspace_id: [2; 32],
            exporter: [3; 16],
            generation: 1,
            capture: LegacyCaptureState {
                review_base: None,
                operation_heads: vec!["00".repeat(64)],
                working_copy: c::PortableWorkingCopy {
                    workspace: "default".into(),
                    wc_operation: "00".repeat(64),
                    snapshot_operation: "00".repeat(64),
                    snapshot_commit: [0; 32],
                    tree: c::CaptureTree {
                        terms: vec![tree.descriptor.key.id.0],
                        labels: vec![],
                    },
                },
                operations: vec![c::OperationRecord {
                    operation: "00".repeat(64),
                    view: "00".repeat(64),
                    parents: vec![],
                }],
                roots: vec![
                    c::CapturedRoot {
                        kind: 1,
                        id: leaf.key.id.0,
                    },
                    c::CapturedRoot {
                        kind: 2,
                        id: tree.descriptor.key.id.0,
                    },
                ],
                remote: None,
            },
        };
        let mut writer = PrivateBundleWriter::new(tempfile::tempfile()?, limits.clone())?;
        if include_file {
            writer.frame(17, &leaf.key.id.0, payload)?;
        }
        writer.frame(18, &tree.descriptor.key.id.0, &tree.bytes)?;
        writer.frame(6, &[], &serde_json::to_vec(&manifest)?)?;
        writer.file.write_all(&[END])?;
        writer.file.sync_all()?;
        let digest = hash_file(&mut writer.file, &limits, Instant::now())?;
        Ok((writer.file, digest, leaf))
    }
    #[tokio::test]
    async fn verified_private_reader_rehashes_native_bytes_without_source_cache() -> Result<()> {
        let (mut file, digest, leaf) = fixture(true)?;
        let verified = verify_file(file.try_clone()?, digest, BundleLimits::default())?;
        assert_eq!(verified.working_copy().snapshot_operation, "00".repeat(64));
        assert_eq!(verified.native_descriptors().len(), 2);
        let reader = verified.native_reader();
        let (_, mut body) = reader
            .read_file(m::FileId(leaf.key.id.0))
            .await?
            .into_parts();
        let mut bytes = Vec::new();
        body.read_to_end(&mut bytes).await?;
        assert_eq!(bytes, b"unpublished private fixture bytes");
        let offset = verified.reader.entries[&checked_key(leaf.key)].offset;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(b"X")?;
        file.sync_all()?;
        assert!(reader.read_file(m::FileId(leaf.key.id.0)).await.is_err());
        assert!(verify_file(file, digest, BundleLimits::default()).is_err());
        Ok(())
    }
    #[test]
    fn private_bundle_rejects_missing_native_closure_even_with_correct_file_digest() -> Result<()> {
        let (file, digest, _) = fixture(false)?;
        assert!(verify_file(file, digest, BundleLimits::default()).is_err());
        Ok(())
    }
}
