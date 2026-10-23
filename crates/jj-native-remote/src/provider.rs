use crate::{Inner, backend, backend_error, hex, io, unhex};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures::AsyncRead;
use jj_lib::backend::BackendResult;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, VerifiedMetadata,
};
use local_cas::{Artifact, BlobSource};
use std::sync::Arc;
use tinytree_model as m;

#[derive(Clone)]
pub struct RemoteObjectIo {
    pub(crate) inner: Arc<Inner>,
}
impl std::fmt::Debug for RemoteObjectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteObjectIo").finish_non_exhaustive()
    }
}
impl RemoteObjectIo {
    async fn promise(&self, id: m::FileId) -> Result<Option<m::Descriptor>> {
        let ledger = self.inner.ledger.clone();
        let claim = io::blocking(move || ledger.claim(id.0)).await?;
        let Some(claim) = claim else { return Ok(None) };
        let ledger = self.inner.ledger.clone();
        let root = m::CommitId(unhex(&claim.root)?);
        let tree = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(unhex(&claim.tree)?),
        };
        let valid = io::blocking(move || {
            Ok(ledger.root(root)?.is_some_and(|root| root.complete)
                && ledger.member(&hex(&root.0), tree)?)
        })
        .await?;
        ensure!(
            valid,
            "native derived file index lacks a completed metadata admission"
        );
        let metadata = self
            .inner
            .local
            .read_metadata(tree)
            .await
            .map_err(backend)?;
        let m::Object::Tree(tree) = metadata.into_object() else {
            anyhow::bail!("native derived file provenance is not a tree")
        };
        let sizes: Vec<_> = tree
            .entries
            .into_iter()
            .filter_map(|entry| match entry.value {
                m::TreeValue::File {
                    id: found,
                    size_bytes,
                    ..
                } if found == id => Some(size_bytes),
                _ => None,
            })
            .collect();
        ensure!(
            !sizes.is_empty() && sizes.iter().all(|size| *size == claim.size),
            "native derived file size does not match canonical tree"
        );
        Ok(Some(m::Descriptor {
            key: m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            },
            size_bytes: claim.size,
            format_version: m::FORMAT_VERSION,
        }))
    }
    pub(crate) fn artifact(&self, descriptor: m::Descriptor) -> Artifact {
        Artifact {
            backend: format!("tinytree-native-v1:{}", self.inner.ledger.binding.scope),
            instance: self.inner.config.remote.repository_id.clone(),
            digest_function: 9,
            hash: hex(&descriptor.key.id.0),
            size: descriptor.size_bytes,
        }
    }
    pub(crate) async fn cached_file(
        &self,
        descriptor: m::Descriptor,
    ) -> Result<Arc<local_cas::CachedBlob>> {
        let artifact = self.artifact(descriptor);
        if let Some(blob) = self
            .inner
            .cache
            .clone()
            .cached_content(artifact.clone())
            .await?
        {
            return Ok(blob);
        }
        let ledger = self.inner.ledger.clone();
        let id = descriptor.key.id.0;
        let claim = io::blocking(move || ledger.claim(id))
            .await?
            .context("native lazy file lacks admission provenance")?;
        let ledger = self.inner.ledger.clone();
        let root_id = m::CommitId(unhex(&claim.root)?);
        let root = io::blocking(move || ledger.root(root_id))
            .await?
            .context("native lazy file admission root missing")?;
        if root.binding != self.inner.ledger.binding.scope {
            crate::RemoteRepositoryControl {
                inner: self.inner.clone(),
            }
            .refresh_scope_hold(root_id)
            .await?;
        }
        self.inner
            .cache
            .clone()
            .content_from(
                artifact,
                Arc::new(RemoteSource {
                    inner: self.inner.clone(),
                }),
            )
            .await
    }
}
#[async_trait]
impl NativeObjectIo for RemoteObjectIo {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        if let Some(descriptor) = self.inner.local.descriptor(key).await? {
            return Ok(Some(descriptor));
        }
        if key.kind == m::Kind::File {
            self.promise(m::FileId(key.id.0))
                .await
                .map_err(backend_error)
        } else {
            Ok(None)
        }
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.inner.local.read_metadata(key).await
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.inner.local.write_metadata(object).await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        if self.inner.local.descriptor(key).await?.is_some() {
            return self.inner.local.read_file(id).await;
        }
        let result = async {
            let descriptor = self
                .promise(id)
                .await?
                .context("native file has no admitted canonical provenance")?;
            let blob = self.cached_file(descriptor).await?;
            let reader = io::Reader::open(blob).await?;
            FileRead::from_preverified_pinned_source(descriptor, Box::pin(reader)).map_err(backend)
        }
        .await;
        result.map_err(backend_error)
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId> {
        self.inner.local.write_file(input, max_bytes).await
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.inner.local.copy_catalog_snapshot(limits).await
    }
}
struct RemoteSource {
    inner: Arc<Inner>,
}
#[async_trait]
impl BlobSource for RemoteSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut std::fs::File) -> Result<()> {
        let deadline = self.inner.deadline();
        tokio::time::timeout_at(deadline, async {
            ensure!(
                artifact.backend
                    == format!("tinytree-native-v1:{}", self.inner.ledger.binding.scope)
                    && artifact.instance == self.inner.config.remote.repository_id
                    && artifact.digest_function == 9,
                "native cache artifact authorization scope mismatch"
            );
            let client = self.inner.connector.get().await?;
            let verified = client
                .download_file(m::ObjectId(unhex(&artifact.hash)?), &self.inner.staging)
                .await?;
            ensure!(
                verified.descriptor().size_bytes == artifact.size,
                "native fetched file length conflicts with admitted tree"
            );
            let reader = verified.open()?;
            let quota = Arc::new(
                self.inner
                    .spool
                    .clone()
                    .acquire_many_owned(u32::try_from(artifact.size)?)
                    .await?,
            );
            let target = target.try_clone()?;
            let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
            struct Cancel(Arc<std::sync::atomic::AtomicBool>);
            impl Drop for Cancel {
                fn drop(&mut self) {
                    self.0.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            let _cancel = Cancel(cancelled.clone());
            let inner = self.inner.clone();
            let expected = artifact.size;
            io::blocking(move || {
                use std::io::{Read, Write};
                // Field order closes both descriptors before releasing either the
                // native transfer budget or ownership of the shared cache directory.
                struct Copy {
                    reader: tinytree_client::VerifiedReader,
                    target: std::fs::File,
                    _quota: Arc<tokio::sync::OwnedSemaphorePermit>,
                    _inner: Arc<Inner>,
                }
                let mut copy = Copy {
                    reader,
                    target,
                    _quota: quota,
                    _inner: inner,
                };
                let mut buffer = vec![0; 65536];
                let mut count = 0u64;
                loop {
                    ensure!(
                        !cancelled.load(std::sync::atomic::Ordering::Acquire)
                            && tokio::time::Instant::now() < deadline,
                        "native cache copy cancelled or timed out"
                    );
                    let n = copy.reader.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    count = count
                        .checked_add(n as u64)
                        .context("native cache copy length overflow")?;
                    ensure!(
                        count <= expected,
                        "native cache copy exceeds promised length"
                    );
                    copy.target.write_all(&buffer[..n])?;
                }
                ensure!(count == expected, "native cache copy is incomplete");
                Ok(())
            })
            .await?;
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("native cache hydration deadline exceeded"))?
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>> {
        let client = self.inner.connector.get().await?;
        let mut keys = Vec::new();
        for artifact in artifacts {
            ensure!(
                artifact.backend
                    == format!("tinytree-native-v1:{}", self.inner.ledger.binding.scope)
                    && artifact.instance == self.inner.config.remote.repository_id,
                "native cache artifact authorization scope mismatch"
            );
            keys.push(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(unhex(&artifact.hash)?),
            });
        }
        let missing = client.find_missing(keys).await?;
        Ok(artifacts
            .iter()
            .filter(|artifact| missing.iter().any(|key| hex(&key.id.0) == artifact.hash))
            .cloned()
            .collect())
    }
}
