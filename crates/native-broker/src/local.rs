//! In-process read admission for daemon-owned providers. Raw providers remain
//! below the facade so dependency proofs never reacquire the same pool.
use crate::{BrokerService, Config, client::run_owned, wire};
use futures::AsyncRead;
use jj_lib::backend::BackendResult;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, ObjectIoResources,
    VerifiedMetadata,
};
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tinytree_model as m;
type Owner = Arc<dyn Any + Send + Sync>;

pub(crate) struct GuardedIo {
    service: BrokerService,
    provider: Arc<dyn NativeObjectIo>,
    config: Config,
}
impl std::fmt::Debug for GuardedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedIo").finish_non_exhaustive()
    }
}
impl GuardedIo {
    pub(crate) fn new(
        service: BrokerService,
        provider: Arc<dyn NativeObjectIo>,
        config: Config,
    ) -> Self {
        Self {
            service,
            provider,
            config,
        }
    }
    async fn read<T, F, Fut>(&self, method: &'static str, owner: Owner, job: F) -> BackendResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn NativeObjectIo>, Owner) -> Fut + Send + 'static,
        Fut: Future<Output = BackendResult<T>> + Send + 'static,
    {
        let service = self.service.clone();
        let provider = self.provider.clone();
        run_owned(owner.clone(), async move {
            let deadline = service.operation_deadline();
            let work = service.admit_local(method, deadline).await?;
            // Both guards follow physical provider jobs, not just this waiter.
            let owner: Owner = Arc::new((owner, work));
            reapi::policy::timeout(deadline, async move {
                job(provider, owner).await.map_err(wire::backend)
            })
            .await
        })
        .await
    }
}
struct OwnedReader {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    _owner: Owner,
}
impl AsyncRead for OwnedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        self.reader.as_mut().poll_read(cx, bytes)
    }
}
#[async_trait::async_trait]
impl NativeObjectIo for GuardedIo {
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        self.provider.metadata_work_budget()
    }
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Owner,
    ) -> BackendResult<Option<m::Descriptor>> {
        self.read("/Descriptor", owner, move |provider, owner| async move {
            provider.descriptor_with_owner(key, owner).await
        })
        .await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.read_metadata_with_owner(key, Arc::new(())).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Owner,
    ) -> BackendResult<VerifiedMetadata> {
        let max_bytes = self.config.max_metadata_bytes;
        self.read("/ReadMetadata", owner, move |provider, owner| async move {
            let metadata = provider.read_metadata_with_owner(key, owner).await?;
            if metadata.descriptor().key != key || metadata.bytes().len() > max_bytes {
                return Err(wire::other(tonic::Status::resource_exhausted(
                    "broker metadata exceeds limit",
                )));
            }
            Ok(metadata)
        })
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
        owner: Owner,
    ) -> BackendResult<CopyCatalogSnapshot> {
        if limits.max_objects == 0
            || limits.max_objects > self.config.max_catalog_objects
            || limits.max_metadata_bytes == 0
            || limits.max_metadata_bytes > self.config.max_catalog_bytes
            || limits.max_edges == 0
            || limits.max_edges > self.config.max_catalog_edges
        {
            return Err(wire::other(tonic::Status::resource_exhausted(
                "broker catalog request exceeds limit",
            )));
        }
        self.read("/CopyCatalog", owner, move |provider, owner| async move {
            provider
                .copy_catalog_snapshot_with_owner(limits, owner)
                .await
        })
        .await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.read_file_for_cache(id, Arc::new(())).await
    }
    async fn read_file_with_owner(&self, id: m::FileId, owner: Owner) -> BackendResult<FileRead> {
        let max_bytes = self.config.max_file_bytes;
        self.read("/ReadFile", owner, move |provider, owner| async move {
            let file = provider.read_file_with_owner(id, owner.clone()).await?;
            let (descriptor, reader) = file.into_parts();
            if descriptor.key.id.0 != id.0 || descriptor.size_bytes > max_bytes {
                return Err(wire::other(tonic::Status::resource_exhausted(
                    "broker file exceeds limit",
                )));
            }
            FileRead::from_preverified_pinned_source(
                descriptor,
                Box::pin(OwnedReader {
                    reader,
                    _owner: owner,
                }),
            )
        })
        .await
    }
    async fn read_file_for_cache(&self, id: m::FileId, owner: Owner) -> BackendResult<FileRead> {
        let max_bytes = self.config.max_file_bytes;
        self.read("/ReadFile", owner, move |provider, owner| async move {
            let file = provider.read_file_for_cache(id, owner.clone()).await?;
            let (descriptor, reader) = file.into_parts();
            if descriptor.key.id.0 != id.0 || descriptor.size_bytes > max_bytes {
                return Err(wire::other(tonic::Status::resource_exhausted(
                    "broker file exceeds limit",
                )));
            }
            FileRead::from_preverified_pinned_source(
                descriptor,
                Box::pin(OwnedReader {
                    reader,
                    _owner: owner,
                }),
            )
        })
        .await
    }
    // Workspace controllers also use this facade for local snapshot writes.
    // Those retain existing provider staging admission and durable ACK semantics;
    // they do not perform remote dependency hydration.
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.provider.write_metadata(object).await
    }
    async fn write_metadata_with_owner(
        &self,
        object: m::Object,
        owner: Owner,
    ) -> BackendResult<m::Descriptor> {
        self.read("/WriteMetadata", owner, move |provider, owner| async move {
            provider.write_metadata_with_owner(object, owner).await
        })
        .await
    }
    async fn write_file_with_owner(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
        owner: Owner,
    ) -> BackendResult<m::FileId> {
        let work = self
            .service
            .admit_local("/WriteFile", self.service.operation_deadline())
            .await
            .map_err(wire::other)?;
        self.provider
            .write_file_with_owner(input, max_bytes, Arc::new((owner, work)))
            .await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId> {
        self.provider.write_file(input, max_bytes).await
    }
}
