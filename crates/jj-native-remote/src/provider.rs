use crate::{Inner, backend, backend_error, hex, io, unhex};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures::AsyncRead;
use jj_lib::backend::BackendResult;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, ObjectIoResources,
    VerifiedMetadata,
};
use local_cas::{Artifact, BlobSource, FetchContext};
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
        self.promise_owned(id, Arc::new(())).await
    }
    pub(crate) async fn promise_owned(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Option<m::Descriptor>> {
        if let Some(descriptor) = self.resident_cloud_descriptor(id, owner.clone()).await? {
            return Ok(Some(descriptor));
        }
        if let Some(authority) = self.cloud_file_authority(id, owner.clone()).await? {
            // An explicit FILE terminal need not have a parent TREE size. Use
            // the bounded, version-scoped verified transfer, never invent one.
            return Ok(Some(
                self.download_cloud_file_owned(authority, id, owner)
                    .await?
                    .descriptor(),
            ));
        }
        let ledger = self.inner.ledger.clone();
        let claim = io::blocking_owned(owner.clone(), move || ledger.claim(id.0)).await?;
        let Some(claim) = claim else { return Ok(None) };
        let ledger = self.inner.ledger.clone();
        let authority_key = claim.root.clone();
        let tree = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(unhex(&claim.tree)?),
        };
        let retained = io::blocking_owned(owner.clone(), move || ledger.authority(&authority_key))
            .await?
            .context("native file provenance root disappeared")?;
        self.inner.ledger.validate_authority(&retained)?;
        self.prove_owned(&retained, tree, owner.clone(), self.inner.deadline())
            .await?;
        let metadata = if matches!(retained, crate::ledger::Authority::CloudQuery(_)) {
            self.verified_metadata_with_owner(tree, owner).await?
        } else {
            self.inner
                .local
                .read_metadata_with_owner(tree, owner)
                .await
                .map_err(backend)?
        };
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
    async fn resident_cloud_descriptor(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Option<m::Descriptor>> {
        if let Some(descriptor) = self
            .terminal_cloud_file_descriptor(id, owner.clone())
            .await?
        {
            return Ok(Some(descriptor));
        }
        if let Some(authority) = self.cloud_file_authority(id, owner.clone()).await? {
            if authority.terminal(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            })? {
                // A current exact terminal without a verified descriptor is a
                // network miss, not permission to reuse an older TREE claim.
                return Ok(None);
            }
        }
        let ledger = self.inner.ledger.clone();
        let Some(claim) = io::blocking_owned(owner.clone(), move || ledger.claim(id.0)).await?
        else {
            return Ok(None);
        };
        if !claim.root.starts_with("v/") {
            return Ok(None);
        }
        let tree = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(unhex(&claim.tree)?),
        };
        let ledger = self.inner.ledger.clone();
        let root = claim.root.clone();
        io::blocking_owned(owner.clone(), move || {
            let authority = ledger
                .authority(&root)?
                .context("native cloud file authority missing")?;
            ensure!(
                matches!(&authority, crate::ledger::Authority::CloudVersion(_)),
                "native cloud file authority kind mismatch"
            );
            ledger.validate_authority(&authority)?;
            ensure!(
                ledger.member(&root, tree)?,
                "native cloud file tree membership missing"
            );
            Ok(())
        })
        .await?;
        if self
            .inner
            .local
            .descriptor_with_owner(tree, owner.clone())
            .await
            .map_err(backend)?
            .is_none()
        {
            return Ok(None);
        }
        let metadata = self
            .inner
            .local
            .read_metadata_with_owner(tree, owner)
            .await
            .map_err(backend)?;
        let m::Object::Tree(tree) = metadata.into_object() else {
            anyhow::bail!("native file provenance is not a tree")
        };
        let mut found = false;
        for entry in tree.entries {
            if let m::TreeValue::File {
                id: candidate,
                size_bytes,
                ..
            } = entry.value
            {
                if candidate == id {
                    ensure!(
                        size_bytes == claim.size,
                        "native file size differs from canonical tree"
                    );
                    found = true;
                }
            }
        }
        ensure!(found, "native file missing from canonical tree");
        Ok(Some(m::Descriptor {
            key: m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            },
            size_bytes: claim.size,
            format_version: m::FORMAT_VERSION,
        }))
    }
    async fn cloud_file_read(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Option<FileRead>> {
        if let Some(descriptor) = self.resident_cloud_descriptor(id, owner.clone()).await? {
            if let Some(blob) = io::cached(
                self.inner
                    .cache
                    .clone()
                    .cached_content(self.artifact(descriptor)),
            )
            .await?
            {
                let reader = io::Reader::open_for_delivery(blob, owner.clone()).await?;
                return Ok(Some(
                    FileRead::from_preverified_pinned_source(descriptor, Box::pin(reader))
                        .map_err(backend)?,
                ));
            }
        }
        let Some(authority) = self.cloud_file_authority(id, owner.clone()).await? else {
            return Ok(None);
        };
        let verified = self
            .download_cloud_file_owned(authority, id, owner.clone())
            .await?;
        let descriptor = verified.descriptor();
        let reader = io::blocking_owned(owner.clone(), move || {
            verified.open().map_err(anyhow::Error::from)
        })
        .await?;
        let reader = io::Reader::verified(reader, owner);
        Ok(Some(
            FileRead::from_preverified_pinned_source(descriptor, Box::pin(reader))
                .map_err(backend)?,
        ))
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
        if let Some(blob) =
            io::cached(self.inner.cache.clone().cached_content(artifact.clone())).await?
        {
            return Ok(blob);
        }
        if self
            .cloud_file_authority(m::FileId(descriptor.key.id.0), Arc::new(()))
            .await?
            .is_none()
        {
            self.refresh_file_scope(descriptor).await?;
        }
        io::cached(self.inner.cache.clone().content_from(
            artifact,
            Arc::new(RemoteSource {
                inner: self.inner.clone(),
                scope: io::scope(),
            }),
        ))
        .await
    }
    async fn refresh_file_scope(&self, descriptor: m::Descriptor) -> Result<()> {
        let ledger = self.inner.ledger.clone();
        let id = descriptor.key.id.0;
        let claim = io::blocking(move || ledger.claim(id))
            .await?
            .context("native lazy file lacks admission provenance")?;
        let ledger = self.inner.ledger.clone();
        let authority_key = claim.root;
        let authority = io::blocking(move || ledger.authority(&authority_key))
            .await?
            .context("native lazy file admission authority missing")?;
        if let Some(root) = authority.root() {
            if root.binding != self.inner.ledger.binding.scope {
                crate::RemoteRepositoryControl {
                    inner: self.inner.clone(),
                }
                .refresh_scope_hold(m::CommitId(unhex(&root.head)?))
                .await?;
            }
        } else {
            self.inner.ledger.validate_authority(&authority)?;
        }
        Ok(())
    }
}
#[async_trait]
impl NativeObjectIo for RemoteObjectIo {
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        let budget = self.inner.config.control_work_budget();
        Some(ObjectIoResources {
            memory_bytes: budget.memory_bytes,
            temporary_bytes: budget.temporary_bytes,
        })
    }
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<Option<m::Descriptor>> {
        self.owned_descriptor(key, owner)
            .await
            .map_err(backend_error)
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.verified_metadata(key).await.map_err(backend_error)
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        self.verified_metadata_with_owner(key, owner)
            .await
            .map_err(backend_error)
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.inner.local.write_metadata(object).await
    }
    async fn write_metadata_with_owner(
        &self,
        object: m::Object,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<m::Descriptor> {
        self.inner
            .local
            .write_metadata_with_owner(object, owner)
            .await
    }

    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.snapshot_file_authority(id, Arc::new(()))
            .await
            .map_err(backend_error)?;
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
    async fn read_file_with_owner(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        // The hit-only/raw-delivery implementation also satisfies ordinary owned
        // reads. It deliberately avoids recursively filling the shared cache.
        self.read_file_for_cache(id, owner).await
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        self.snapshot_file_authority(id, owner.clone())
            .await
            .map_err(backend_error)?;
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        if self
            .inner
            .local
            .descriptor_with_owner(key, owner.clone())
            .await?
            .is_some()
        {
            return self.inner.local.read_file_for_cache(id, owner).await;
        }
        if let Some(read) = self
            .cloud_file_read(id, owner.clone())
            .await
            .map_err(backend_error)?
        {
            return Ok(read);
        }
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        let operation = async {
            let descriptor = self
                .promise_owned(id, owner.clone())
                .await?
                .context("native file has no admitted canonical provenance")?;
            let artifact = self.artifact(descriptor);
            // HIT ONLY: this call may already own the same cache's sole fetch
            // slot/byte reservation under the workspace's artifact scope.
            if let Some(blob) =
                io::cached(self.inner.cache.clone().cached_content(artifact)).await?
            {
                let reader = io::Reader::open_for_delivery(blob, owner.clone()).await?;
                return FileRead::from_preverified_pinned_source(descriptor, Box::pin(reader))
                    .map_err(backend);
            }
            self.refresh_file_scope(descriptor).await?;
            let verified = if let Some(authority) =
                self.snapshot_file_authority(id, owner.clone()).await?
            {
                self.download_snapshot_file_owned(authority, id, owner.clone())
                    .await?
            } else {
                let client = self
                    .inner
                    .connector
                    .get()
                    .await?
                    .with_rpc_lifetime_guard(Arc::new(owner.clone()));
                io::network(client.download_file(m::ObjectId(id.0), &self.inner.staging)).await?
            };
            ensure!(
                verified.descriptor() == descriptor,
                "native fetched file differs from admitted tree"
            );
            // No second spool and no cache hydration. The verified temporary's
            // reader owns client quota, staging ownership and delivery guards.
            let reader = io::blocking_owned(owner.clone(), move || {
                verified.open().map_err(anyhow::Error::from)
            })
            .await?;
            let reader = io::Reader::verified(reader, owner);
            FileRead::from_preverified_pinned_source(descriptor, Box::pin(reader)).map_err(backend)
        };
        let result = tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| anyhow::anyhow!("native cache delivery deadline exceeded"))
            .and_then(|result| result);
        result.map_err(backend_error)
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId> {
        self.inner.local.write_file(input, max_bytes).await
    }
    async fn write_file_with_owner(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<m::FileId> {
        self.inner
            .local
            .write_file_with_owner(input, max_bytes, owner)
            .await
    }

    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.copy_catalog_snapshot_with_owner(limits, Arc::new(()))
            .await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.owned_catalog(limits, owner).await
    }
}
struct RemoteSource {
    inner: Arc<Inner>,
    scope: Option<Arc<io::ControlScope>>,
}
impl RemoteSource {
    async fn fetch_impl(
        &self,
        artifact: &Artifact,
        target: &mut std::fs::File,
        context: Option<FetchContext>,
    ) -> Result<()> {
        let operation = async {
            let deadline = self.scope.as_ref().map_or_else(
                || self.inner.deadline(),
                |scope| scope.deadline.min(self.inner.deadline()),
            );
            tokio::time::timeout_at(deadline, async {
                ensure!(
                    artifact.backend
                        == format!("tinytree-native-v1:{}", self.inner.ledger.binding.scope)
                        && artifact.instance == self.inner.config.remote.repository_id
                        && artifact.digest_function == 9,
                    "native cache artifact authorization scope mismatch"
                );
                // This is the lower-level raw download inside the existing
                // cache fill, never another public read/cache request.
                let owner: Arc<dyn std::any::Any + Send + Sync> =
                    Arc::new((context.clone(), self.scope.clone()));
                let provider = RemoteObjectIo {
                    inner: self.inner.clone(),
                };
                let id = m::FileId(unhex(&artifact.hash)?);
                let verified = if let Some(authority) =
                    provider.snapshot_file_authority(id, owner.clone()).await?
                {
                    provider
                        .download_snapshot_file_owned(authority, id, owner)
                        .await?
                } else if let Some(authority) =
                    provider.cloud_file_authority(id, owner.clone()).await?
                {
                    provider
                        .download_cloud_file_owned(authority, id, owner)
                        .await?
                } else {
                    let client = self
                        .inner
                        .connector
                        .get()
                        .await?
                        .with_rpc_lifetime_guard(Arc::new(owner));
                    io::network(client.download_file(m::ObjectId(id.0), &self.inner.staging))
                        .await?
                };
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
                // Assemble one ordered owner BEFORE dispatch, including when a
                // queued closure is discarded without ever starting its body.
                struct Copy {
                    reader: tinytree_client::VerifiedReader,
                    target: std::fs::File,
                    _quota: Arc<tokio::sync::OwnedSemaphorePermit>,
                    _inner: Arc<Inner>,
                    _context: Option<FetchContext>,
                }
                let copy = Copy {
                    reader,
                    target,
                    _quota: quota,
                    _inner: inner,
                    _context: context.clone(),
                };
                io::blocking(move || {
                    use std::io::{Read, Write};
                    let mut copy = copy;
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
        };
        if let Some(scope) = &self.scope {
            io::control_scope(scope.clone(), operation).await
        } else {
            operation.await
        }
    }
}
#[async_trait]
impl BlobSource for RemoteSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut std::fs::File) -> Result<()> {
        self.fetch_impl(artifact, target, None).await
    }
    async fn fetch_with_context(
        &self,
        artifact: &Artifact,
        target: &mut std::fs::File,
        context: FetchContext,
    ) -> Result<()> {
        self.fetch_impl(artifact, target, Some(context)).await
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
        let missing = io::network(client.find_missing(keys)).await?;
        Ok(artifacts
            .iter()
            .filter(|artifact| missing.iter().any(|key| hex(&key.id.0) == artifact.hash))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, RemoteRepository};
    use p::repository_server::{Repository, RepositoryServer};
    use std::{
        path::Path,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tinytree_client::protocol as p;
    use tonic::{Request, Response, Status, Streaming};
    struct UnusedSource;
    #[async_trait]
    impl BlobSource for UnusedSource {
        async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> Result<()> {
            anyhow::bail!("unexpected default cache source")
        }
        async fn missing(&self, a: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(a.to_vec())
        }
    }
    struct DelayedFile {
        inner: tinytree_server::RepositoryService,
        calls: Arc<AtomicUsize>,
    }
    #[tonic::async_trait]
    impl Repository for DelayedFile {
        async fn open_repository(
            &self,
            r: Request<p::OpenRepositoryRequest>,
        ) -> Result<Response<p::RepositoryView>, Status> {
            self.inner.open_repository(r).await
        }
        async fn get_commit_graph(
            &self,
            r: Request<p::GetCommitGraphRequest>,
        ) -> Result<Response<p::CommitGraphPage>, Status> {
            self.inner.get_commit_graph(r).await
        }
        type BatchReadObjectsStream =
            <tinytree_server::RepositoryService as Repository>::BatchReadObjectsStream;
        async fn batch_read_objects(
            &self,
            r: Request<p::BatchReadObjectsRequest>,
        ) -> Result<Response<Self::BatchReadObjectsStream>, Status> {
            self.inner.batch_read_objects(r).await
        }
        async fn find_missing_objects(
            &self,
            r: Request<p::FindMissingObjectsRequest>,
        ) -> Result<Response<p::MissingObjects>, Status> {
            self.inner.find_missing_objects(r).await
        }
        type ReadFileStream = <tinytree_server::RepositoryService as Repository>::ReadFileStream;
        async fn read_file(
            &self,
            r: Request<p::ReadFileRequest>,
        ) -> Result<Response<Self::ReadFileStream>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(500)).await;
            self.inner.read_file(r).await
        }
        async fn write_object(
            &self,
            r: Request<Streaming<p::WriteFrame>>,
        ) -> Result<Response<p::WriteReceipt>, Status> {
            self.inner.write_object(r).await
        }
        async fn publish_reference(
            &self,
            r: Request<p::PublishReferenceRequest>,
        ) -> Result<Response<p::Publication>, Status> {
            self.inner.publish_reference(r).await
        }
        async fn get_publication(
            &self,
            r: Request<p::GetPublicationRequest>,
        ) -> Result<Response<p::Publication>, Status> {
            self.inner.get_publication(r).await
        }
    }
    fn private(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detached_native_hydration_uses_original_control_deadline_not_fresh_allowance() {
        let root = tempfile::tempdir().unwrap();
        private(root.path());
        let store =
            tinytree_store::Store::open(root.path().join("server"), Default::default()).unwrap();
        store.create_repository("default").unwrap();
        let bytes = b"bounded native source";
        let descriptor = m::describe_file(bytes);
        store.put("default", descriptor, bytes, 0).unwrap();
        let service = tinytree_server::RepositoryService::new(
            store,
            Default::default(),
            vec!["default".into()],
            vec![],
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let count = calls.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RepositoryServer::new(DelayedFile {
                    inner: service,
                    calls: count,
                }))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let local = root.path().join("local");
        std::fs::create_dir(&local).unwrap();
        let direct =
            Arc::new(jj_native_backend::DirectObjectIo::init(&local, Default::default()).unwrap());
        let cache = local_cas::LocalCas::open(
            root.path().join("cache"),
            Arc::new(UnusedSource),
            Default::default(),
        )
        .unwrap();
        let mut config = Config::default();
        config.operation_timeout_ms = 2000;
        config.remote.endpoint = format!("http://{address}");
        config.remote.compression = false;
        let repo = RemoteRepository::open(
            direct,
            cache,
            &root.path().join("ledger"),
            &root.path().join("staging"),
            config,
        )
        .unwrap();
        repo.io.inner.connector.get().await.unwrap();
        let permit = repo.io.inner.control.clone().acquire_owned().await.unwrap();
        let scope = Arc::new(io::ControlScope::new(
            permit,
            tokio::time::Instant::now() + Duration::from_millis(50),
            Arc::new(()),
        ));
        let source = RemoteSource {
            inner: repo.io.inner.clone(),
            scope: Some(scope),
        };
        let artifact = repo.io.artifact(descriptor);
        let mut target = tempfile::tempfile().unwrap();
        // The outer allowance is intentionally shorter than the server delay,
        // but longer than the original control deadline. Fresh-deadline behavior
        // would hit this outer timeout rather than finish its scoped failure.
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            source.fetch(&artifact, &mut target),
        )
        .await
        .expect("hydration renewed an expired control deadline");
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(target.metadata().unwrap().len(), 0);
        drop(source);
        repo.control.drain_control().await;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_cloud_admission_during_copy_catalog_snapshot_refuses_partial_success() {
        let directory = tempfile::tempdir().unwrap();
        private(directory.path());
        let local = directory.path().join("local");
        std::fs::create_dir(&local).unwrap();
        private(&local);
        let direct =
            Arc::new(jj_native_backend::DirectObjectIo::init(&local, Default::default()).unwrap());
        let cache = local_cas::LocalCas::open(
            directory.path().join("cache"),
            Arc::new(UnusedSource),
            Default::default(),
        )
        .unwrap();
        let repo = RemoteRepository::open(
            direct.clone(),
            cache,
            &directory.path().join("ledger"),
            &directory.path().join("staging"),
            Config::default(),
        )
        .unwrap();
        let boundary = Arc::new(crate::metadata::CatalogBoundary::default());
        *repo.io.inner.catalog_boundary.lock().unwrap() = Some(boundary.clone());
        let provider = repo.io.clone();
        let request = tokio::spawn(async move {
            provider
                .copy_catalog_snapshot(CopyCatalogLimits {
                    max_objects: 16,
                    max_metadata_bytes: 16384,
                    max_edges: 32,
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), boundary.entered.notified())
            .await
            .unwrap();
        let copy = m::Object::CopyHistory(m::CopyHistory {
            current_path: "copy".into(),
            parents: vec![],
            salt: b"catalog boundary".to_vec(),
        });
        let descriptor = m::encode(&copy, Default::default()).unwrap().descriptor;
        let ledger = repo.io.inner.ledger.clone();
        let verified = copy.clone();
        io::blocking(move || {
            let authority = ledger.install_catalog_test_authority(descriptor.key)?;
            ledger.verified_cloud_metadata_batch(
                &crate::ledger::Authority::CloudVersion(authority),
                &[(descriptor, verified)],
            )
        })
        .await
        .unwrap();
        direct.write_metadata(copy).await.unwrap();
        // Prove the real Direct snapshot would contain fetched partial data.
        assert_eq!(
            direct
                .copy_catalog_snapshot(CopyCatalogLimits {
                    max_objects: 16,
                    max_metadata_bytes: 16384,
                    max_edges: 32,
                })
                .await
                .unwrap()
                .into_entries()
                .len(),
            1
        );
        boundary.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&result, Err(jj_lib::backend::BackendError::Unsupported(_))),
            "concurrent first admission must not publish a complete catalog: {result:?}"
        );
        repo.wait_for_idle().await;
    }
}
