//! Metadata-only native source projection. This validates a filesystem manifest,
//! not a complete commit/history closure or remote retention proof. Provider
//! authorization and pinned-file lifetimes remain NativeObjectIo obligations.
use anyhow::{Result, ensure};
use async_trait::async_trait;
use futures::AsyncReadExt;
use jj_native_backend::NativeObjectIo;
use local_cas::{Artifact, BlobSource};
use output_core::{SourceContent, SourceEntry, SourceManifest};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::File,
    io::{Seek, SeekFrom, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tinytree_model as m;
use tokio::sync::Semaphore;

/// Construct only from trusted host repository configuration and its EFFECTIVE
/// authorization/security identity, never a client-selected alias or endpoint.
#[derive(Clone)]
pub struct Scope {
    backend: String,
    instance: String,
}
impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scope").finish_non_exhaustive()
    }
}
impl Scope {
    pub fn from_configured_identity(
        repository: &str,
        provider_security_identity: &[u8],
    ) -> Result<Self> {
        ensure!(
            !repository.is_empty()
                && repository.len() <= 512
                && !provider_security_identity.is_empty()
                && provider_security_identity.len() <= 4096,
            "invalid configured native source scope"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-workspace-scope-v1\0");
        for field in [repository.as_bytes(), provider_security_identity] {
            hash.update(&(field.len() as u64).to_le_bytes());
            hash.update(field);
        }
        Ok(Self {
            backend: format!("native-workspace-v1:{}", hash.finalize().to_hex()),
            instance: blake3::hash(repository.as_bytes()).to_hex().to_string(),
        })
    }
    pub fn backend(&self) -> &str {
        &self.backend
    }
    pub fn instance(&self) -> &str {
        &self.instance
    }
    fn artifact(&self, descriptor: m::Descriptor) -> Artifact {
        Artifact {
            backend: self.backend.clone(),
            instance: self.instance.clone(),
            digest_function: 9,
            hash: hex(&descriptor.key.id.0),
            size: descriptor.size_bytes,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_metadata_objects: usize,
    pub max_metadata_bytes: u64,
    pub max_metadata_object_bytes: usize,
    pub max_entries: usize,
    pub max_depth: usize,
    pub max_path_bytes: usize,
    pub max_total_path_bytes: usize,
    pub max_symlink_bytes: usize,
    pub max_file_bytes: u64,
    pub max_file_transfers: usize,
    pub max_authorized_files: usize,
    pub file_buffer_bytes: usize,
    pub max_missing_batch: usize,
    pub operation_timeout_seconds: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_metadata_objects: 10_000,
            max_metadata_bytes: 64 * 1024 * 1024,
            max_metadata_object_bytes: 4 * 1024 * 1024,
            max_entries: 100_000,
            max_depth: 256,
            max_path_bytes: 4096,
            max_total_path_bytes: 32 * 1024 * 1024,
            max_symlink_bytes: 64 * 1024,
            max_file_bytes: 256 * 1024 * 1024,
            max_file_transfers: 4,
            max_authorized_files: 100_000,
            file_buffer_bytes: 64 * 1024,
            max_missing_batch: 1024,
            operation_timeout_seconds: 30,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_metadata_objects > 0
                && self.max_metadata_objects <= 1_000_000
                && self.max_entries > 0
                && self.max_entries <= 1_000_000
                && self.max_metadata_bytes > 0
                && self.max_metadata_bytes <= 1024 * 1024 * 1024
                && self.max_metadata_object_bytes > 0
                && self.max_metadata_object_bytes <= 16 * 1024 * 1024
                && self.max_depth > 0
                && self.max_depth <= 1024
                && self.max_path_bytes > 0
                && self.max_path_bytes <= 64 * 1024
                && self.max_total_path_bytes > 0
                && self.max_total_path_bytes <= 256 * 1024 * 1024
                && self.max_symlink_bytes > 0
                && self.max_symlink_bytes <= 1024 * 1024
                && self.max_file_bytes <= 16 * 1024 * 1024 * 1024
                && self.max_authorized_files > 0
                && self.max_authorized_files <= 1_000_000
                && self.max_file_transfers > 0
                && self.max_file_transfers <= 64
                && self.file_buffer_bytes > 0
                && self.file_buffer_bytes <= 1024 * 1024
                && self.max_missing_batch > 0
                && self.max_missing_batch <= 100_000
                && self.operation_timeout_seconds > 0
                && self.operation_timeout_seconds <= 3600,
            "invalid native workspace limits"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileProvenance {
    pub id: m::FileId,
    pub size_bytes: u64,
    pub executable: bool,
    pub copy_id: Option<m::CopyId>,
}
/// Initial projection deliberately rejects, rather than resolves or reorders,
/// multi-term root merges. Original terms remain available to the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnresolvedMerge {
    pub merge: m::TreeMerge,
}
impl fmt::Display for UnresolvedMerge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("native source projection does not yet support unresolved root merges")
    }
}
impl std::error::Error for UnresolvedMerge {}

pub struct Compiler {
    provider: Arc<dyn NativeObjectIo>,
    scope: Scope,
    limits: Limits,
    compilation: Arc<Semaphore>,
    source: Arc<NativeBlobSource>,
}
impl Compiler {
    pub fn new(provider: Arc<dyn NativeObjectIo>, scope: Scope, limits: Limits) -> Result<Self> {
        limits.validate()?;
        let source = Arc::new(NativeBlobSource {
            provider: provider.clone(),
            scope: scope.clone(),
            descriptors: Mutex::new(HashMap::new()),
            transfers: Arc::new(Semaphore::new(limits.max_file_transfers)),
            limits: limits.clone(),
            guards: Mutex::new(vec![]),
        });
        Ok(Self {
            provider,
            scope,
            source,
            limits,
            compilation: Arc::new(Semaphore::new(1)),
        })
    }
    /// Physical file copy jobs retain these guards, even after cancellation.
    pub fn with_lifetime_guard<T: Any + Send + Sync>(self, guard: Arc<T>) -> Self {
        self.source.guards.lock().unwrap().push(guard);
        self
    }
    /// Stable append-only source shared across successful checkouts. Failed
    /// compilations register nothing; old IDs remain authorized for old pins.
    pub fn source(&self) -> Arc<NativeBlobSource> {
        self.source.clone()
    }

    pub async fn compile_tree(&self, root: m::TreeId) -> Result<CompiledWorkspace> {
        self.compile_merge(&m::TreeMerge {
            terms: vec![root],
            labels: vec![String::new()],
        })
        .await
    }
    pub async fn compile_commit(&self, commit: m::CommitId) -> Result<CompiledWorkspace> {
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.limits.operation_timeout_seconds);
        tokio::time::timeout_at(deadline, async {
            let _admission = self.compilation.acquire().await?;
            let mut budget = Budget::default();
            let object = self
                .metadata(
                    m::ObjectKey {
                        kind: m::Kind::Commit,
                        id: m::ObjectId(commit.0),
                    },
                    &mut budget,
                )
                .await?;
            let m::Object::Commit(commit) = object else {
                anyhow::bail!("native source commit type mismatch")
            };
            self.compile_inner(&commit.root_tree, budget).await
        })
        .await
        .map_err(|_| anyhow::anyhow!("native source compile deadline exceeded"))?
    }
    pub async fn compile_merge(&self, merge: &m::TreeMerge) -> Result<CompiledWorkspace> {
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.limits.operation_timeout_seconds);
        tokio::time::timeout_at(deadline, async {
            let _admission = self.compilation.acquire().await?;
            self.compile_inner(merge, Budget::default()).await
        })
        .await
        .map_err(|_| anyhow::anyhow!("native source compile deadline exceeded"))?
    }
    async fn metadata(&self, key: m::ObjectKey, budget: &mut Budget) -> Result<m::Object> {
        budget.objects = budget
            .objects
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("native metadata count exceeded"))?;
        ensure!(
            budget.objects <= self.limits.max_metadata_objects,
            "native metadata count exceeded"
        );
        let descriptor = self
            .provider
            .descriptor(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("native source metadata unavailable"))?;
        ensure!(
            descriptor.key == key
                && descriptor.format_version == m::FORMAT_VERSION
                && descriptor.size_bytes <= self.limits.max_metadata_object_bytes as u64,
            "invalid native source metadata descriptor"
        );
        budget.bytes = budget
            .bytes
            .checked_add(descriptor.size_bytes)
            .ok_or_else(|| anyhow::anyhow!("native metadata bytes exceeded"))?;
        ensure!(
            budget.bytes <= self.limits.max_metadata_bytes,
            "native metadata bytes exceeded"
        );
        let verified = self.provider.read_metadata(key).await?;
        ensure!(
            verified.descriptor() == descriptor,
            "native source metadata descriptor changed"
        );
        // Reapply this compiler's stricter canonical bounds, even if the provider
        // admitted a valid object using a more permissive model limit.
        m::verify(
            &descriptor,
            verified.bytes(),
            m::Limits {
                max_object_bytes: self.limits.max_metadata_object_bytes,
                max_items: self.limits.max_entries,
                max_field_bytes: self
                    .limits
                    .max_symlink_bytes
                    .max(self.limits.max_path_bytes),
                max_path_bytes: self.limits.max_path_bytes,
            },
        )?;
        Ok(verified.into_object())
    }
    async fn compile_inner(
        &self,
        merge: &m::TreeMerge,
        mut budget: Budget,
    ) -> Result<CompiledWorkspace> {
        if merge.terms.len() != 1 {
            return Err(UnresolvedMerge {
                merge: merge.clone(),
            }
            .into());
        }
        ensure!(
            merge.labels == [String::new()],
            "invalid resolved native tree merge labels"
        );
        enum Work {
            Enter(m::TreeId, String, usize),
            Exit(m::TreeId),
        }
        let root = merge.terms[0];
        let mut pending = vec![Work::Enter(root, String::new(), 0)];
        let mut ancestors = HashSet::new();
        let mut entries = Vec::new();
        let mut files = BTreeMap::new();
        let mut trees = BTreeMap::new();
        let mut descriptors = HashMap::new();
        let mut total_path_bytes = 0usize;
        while let Some(work) = pending.pop() {
            let Work::Enter(id, prefix, depth) = work else {
                if let Work::Exit(id) = work {
                    ancestors.remove(&id);
                }
                continue;
            };
            trees.insert(prefix.clone(), id);
            ensure!(
                depth <= self.limits.max_depth && ancestors.insert(id),
                "native tree cycle or depth exceeded"
            );
            let m::Object::Tree(tree) = self
                .metadata(
                    m::ObjectKey {
                        kind: m::Kind::Tree,
                        id: m::ObjectId(id.0),
                    },
                    &mut budget,
                )
                .await?
            else {
                anyhow::bail!("native tree type mismatch")
            };
            ensure!(
                tree.entries
                    .windows(2)
                    .all(|w| w[0].name.as_bytes() < w[1].name.as_bytes()),
                "native tree order is not canonical"
            );
            pending.push(Work::Exit(id));
            let mut children = Vec::new();
            for entry in tree.entries {
                ensure!(
                    !entry.name.is_empty()
                        && entry.name != "."
                        && entry.name != ".."
                        && !entry.name.contains(['/', '\0', '\\']),
                    "invalid native source path component"
                );
                let path = if prefix.is_empty() {
                    entry.name
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                total_path_bytes = total_path_bytes
                    .checked_add(path.len())
                    .ok_or_else(|| anyhow::anyhow!("native source path budget exceeded"))?;
                ensure!(
                    path.len() <= self.limits.max_path_bytes
                        && total_path_bytes <= self.limits.max_total_path_bytes
                        && entries.len() < self.limits.max_entries,
                    "native source namespace budget exceeded"
                );
                let (mode, content) = match entry.value {
                    m::TreeValue::Tree(child) => {
                        children.push(Work::Enter(child, path.clone(), depth + 1));
                        (0o755, SourceContent::Directory)
                    }
                    m::TreeValue::File {
                        id,
                        executable,
                        size_bytes,
                        copy_id,
                    } => {
                        ensure!(
                            size_bytes <= self.limits.max_file_bytes,
                            "native source file exceeds limit"
                        );
                        let d = m::Descriptor {
                            key: m::ObjectKey {
                                kind: m::Kind::File,
                                id: m::ObjectId(id.0),
                            },
                            size_bytes,
                            format_version: m::FORMAT_VERSION,
                        };
                        ensure!(
                            descriptors.insert(d.key, d).is_none_or(|old| old == d),
                            "conflicting native file lengths"
                        );
                        files.insert(
                            path.clone(),
                            FileProvenance {
                                id,
                                executable,
                                size_bytes,
                                copy_id,
                            },
                        );
                        (
                            if executable { 0o755 } else { 0o644 },
                            SourceContent::File(self.scope.artifact(d)),
                        )
                    }
                    m::TreeValue::Symlink { id, target_bytes } => {
                        ensure!(
                            target_bytes <= self.limits.max_symlink_bytes as u64,
                            "native source symlink exceeds limit"
                        );
                        let m::Object::Symlink(target) = self
                            .metadata(
                                m::ObjectKey {
                                    kind: m::Kind::Symlink,
                                    id: m::ObjectId(id.0),
                                },
                                &mut budget,
                            )
                            .await?
                        else {
                            anyhow::bail!("native symlink type mismatch")
                        };
                        ensure!(
                            target.len() as u64 == target_bytes && !target.contains('\0'),
                            "native source symlink length or target mismatch"
                        );
                        (0o777, SourceContent::Symlink(target))
                    }
                };
                entries.push(SourceEntry {
                    path,
                    mode,
                    content,
                });
            }
            pending.extend(children.into_iter().rev());
        }
        entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
        // Validate the complete union before making any file newly eligible for
        // hydration. This source survives later metadata-only root switches.
        self.source.register(&descriptors)?;
        let source = self.source.clone();
        Ok(CompiledWorkspace {
            merge: merge.clone(),
            root,
            entries,
            files,
            trees,
            source,
        })
    }
}
#[derive(Default)]
struct Budget {
    objects: usize,
    bytes: u64,
}

pub struct CompiledWorkspace {
    merge: m::TreeMerge,
    root: m::TreeId,
    entries: Vec<SourceEntry>,
    files: BTreeMap<String, FileProvenance>,
    trees: BTreeMap<String, m::TreeId>,
    source: Arc<NativeBlobSource>,
}
impl CompiledWorkspace {
    pub fn root_merge(&self) -> &m::TreeMerge {
        &self.merge
    }
    pub fn file_provenance(&self) -> &BTreeMap<String, FileProvenance> {
        &self.files
    }
    pub fn tree_provenance(&self) -> &BTreeMap<String, m::TreeId> {
        &self.trees
    }
    pub fn source(&self) -> Arc<NativeBlobSource> {
        self.source.clone()
    }
    /// `identity` is the destination Tree's persistent namespace identity, not a
    /// repository selector. Source/backend authorization is fixed at compile.
    pub fn manifest(&self, identity: &str) -> SourceManifest {
        SourceManifest {
            identity: identity.to_owned(),
            root: self.root.0,
            entries: self.entries.clone(),
        }
    }
}

pub struct NativeBlobSource {
    provider: Arc<dyn NativeObjectIo>,
    scope: Scope,
    descriptors: Mutex<HashMap<m::ObjectKey, m::Descriptor>>,
    transfers: Arc<Semaphore>,
    limits: Limits,
    guards: Mutex<Vec<Arc<dyn Any + Send + Sync>>>,
}
impl fmt::Debug for NativeBlobSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeBlobSource")
            .field("files", &self.descriptors.lock().unwrap().len())
            .finish_non_exhaustive()
    }
}
impl NativeBlobSource {
    fn register(&self, additions: &HashMap<m::ObjectKey, m::Descriptor>) -> Result<()> {
        let mut descriptors = self.descriptors.lock().unwrap();
        let mut extra = 0usize;
        for (key, d) in additions {
            match descriptors.get(key) {
                Some(old) => ensure!(
                    old == d,
                    "conflicting native file lengths across source generations"
                ),
                None => extra += 1,
            }
        }
        ensure!(
            descriptors
                .len()
                .checked_add(extra)
                .is_some_and(|n| n <= self.limits.max_authorized_files),
            "native source authorization catalog exceeds limit"
        );
        descriptors.extend(additions.iter().map(|(k, d)| (*k, *d)));
        Ok(())
    }
    fn validate(&self, artifact: &Artifact) -> Result<m::Descriptor> {
        artifact.validate()?;
        ensure!(
            artifact.backend == self.scope.backend
                && artifact.instance == self.scope.instance
                && artifact.digest_function == 9
                && artifact.size <= self.limits.max_file_bytes,
            "native artifact authorization scope mismatch"
        );
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(unhex(&artifact.hash)?),
        };
        let d = self
            .descriptors
            .lock()
            .unwrap()
            .get(&key)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("artifact is not in this native source manifest"))?;
        ensure!(
            d.size_bytes == artifact.size,
            "native artifact size mismatch"
        );
        Ok(d)
    }
}
impl NativeBlobSource {
    async fn fetch_impl(
        &self,
        artifact: &Artifact,
        target: &mut File,
        context: Option<local_cas::FetchContext>,
    ) -> Result<()> {
        let descriptor = self.validate(artifact)?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(self.limits.operation_timeout_seconds);
        let cancelled = Arc::new(AtomicBool::new(false));
        struct Cancel(Arc<AtomicBool>);
        impl Drop for Cancel {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let _cancel = Cancel(cancelled.clone());
        let permit = tokio::time::timeout_at(deadline, self.transfers.clone().acquire_owned())
            .await
            .map_err(|_| anyhow::anyhow!("native file fetch deadline exceeded"))??;
        let provider = self.provider.clone();
        let guards = self.guards.lock().unwrap().clone();
        let buffer_size = self.limits.file_buffer_bytes;
        let target = target.try_clone()?;
        // The owned job retains admission and host lifetime guards even when
        // the caller disappears while the provider is producing a pinned read.
        let job = tokio::spawn(async move {
            let owners = Arc::new((permit, provider.clone(), guards, context));
            // Local binding order ensures the target closes BEFORE cache
            // ownership/reservation/temp cleanup, including early errors.
            let target = target;
            let pinned = provider
                .read_file_for_cache(m::FileId(descriptor.key.id.0), owners.clone())
                .await?;
            ensure!(
                !cancelled.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline,
                "native file fetch cancelled or timed out"
            );
            let (actual, reader) = pinned.into_parts();
            ensure!(
                actual == descriptor,
                "native file descriptor differs from source manifest"
            );
            let physical_deadline =
                Instant::now() + deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::task::spawn_blocking(move || -> Result<()> {
                let _owners = owners;
                let mut target = target;
                let mut reader = reader;
                ensure!(
                    !cancelled.load(Ordering::Acquire) && Instant::now() < physical_deadline,
                    "native file copy cancelled or timed out"
                );
                target.set_len(0)?;
                target.seek(SeekFrom::Start(0))?;
                let mut buffer = vec![0; buffer_size];
                let mut total = 0u64;
                let mut hash = blake3::Hasher::new();
                loop {
                    ensure!(
                        !cancelled.load(Ordering::Acquire) && Instant::now() < physical_deadline,
                        "native file copy cancelled or timed out"
                    );
                    let count = futures::executor::block_on(reader.read(&mut buffer))?;
                    ensure!(
                        !cancelled.load(Ordering::Acquire) && Instant::now() < physical_deadline,
                        "native file copy cancelled or timed out"
                    );
                    if count == 0 {
                        break;
                    }
                    total = total
                        .checked_add(count as u64)
                        .ok_or_else(|| anyhow::anyhow!("native file length overflow"))?;
                    ensure!(
                        total <= descriptor.size_bytes,
                        "native file exceeds declared size"
                    );
                    hash.update(&buffer[..count]);
                    target.write_all(&buffer[..count])?;
                }
                ensure!(
                    total == descriptor.size_bytes
                        && hash.finalize().as_bytes() == &descriptor.key.id.0,
                    "native file hash or length mismatch"
                );
                Ok(())
            })
            .await?
        });
        tokio::time::timeout_at(deadline, job)
            .await
            .map_err(|_| anyhow::anyhow!("native file fetch deadline exceeded"))??
    }
}

#[async_trait]
impl BlobSource for NativeBlobSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()> {
        self.fetch_impl(artifact, target, None).await
    }
    async fn fetch_with_context(
        &self,
        artifact: &Artifact,
        target: &mut File,
        context: local_cas::FetchContext,
    ) -> Result<()> {
        self.fetch_impl(artifact, target, Some(context)).await
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>> {
        ensure!(
            artifacts.len() <= self.limits.max_missing_batch,
            "native source missing batch exceeds limit"
        );
        for artifact in artifacts {
            self.validate(artifact)?;
        }
        // The immutable manifest remains the source authority. Offline or remote
        // cache absence must not turn a source entry into a namespace deletion.
        Ok(vec![])
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "invalid native file ID");
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
