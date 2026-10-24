//! Transient, metadata-only restore providers sharing the configured repository.
use crate::{RemoteObjectIo, RemoteQueryIo, io};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use jj_lib::backend::BackendResult;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, ObjectIoResources,
    VerifiedMetadata,
};
use native_broker::{NativeQueryIo, QueryOwner};
use std::{sync::Arc, time::Duration};
use tinytree_model as m;

/// A paired configured source, not an independently opened query registry. It
/// shares all provider provenance, authentication, admission and persistent state.
#[derive(Clone)]
pub struct RemoteRestoreIo {
    objects: Arc<RemoteObjectIo>,
    query: Arc<RemoteQueryIo>,
}
impl std::fmt::Debug for RemoteRestoreIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRestoreIo").finish_non_exhaustive()
    }
}
pub(crate) struct RestoreBound {
    pub deadline: tokio::time::Instant,
    pub cleanup: Duration,
    pub owner: QueryOwner,
}
impl RemoteRestoreIo {
    pub fn new(objects: Arc<RemoteObjectIo>, query: Arc<RemoteQueryIo>) -> Result<Self> {
        ensure!(
            query.shares_inner(&objects),
            "restore providers do not share a repository"
        );
        Ok(Self { objects, query })
    }
    /// Validate the erased host inputs before reserving their declared work.
    /// This checks identity, not an untrusted serialized fingerprint.
    pub fn matches(
        &self,
        objects: &Arc<dyn NativeObjectIo>,
        query: &Arc<dyn NativeQueryIo>,
    ) -> bool {
        let actual_objects: Arc<dyn NativeObjectIo> = self.objects.clone();
        let actual_query: Arc<dyn NativeQueryIo> = self.query.clone();
        Arc::ptr_eq(objects, &actual_objects) && Arc::ptr_eq(query, &actual_query)
    }
    /// Bind graph/data work to one absolute deadline across independent runtimes.
    /// Only release-only query closes may use the separately bounded cleanup
    /// allowance. No deadline is persisted into ordinary workspace providers.
    pub fn bind(
        &self,
        original: tokio::time::Instant,
        cleanup: Duration,
        owner: QueryOwner,
    ) -> Result<(Arc<dyn NativeObjectIo>, Arc<dyn NativeQueryIo>)> {
        ensure!(
            cleanup > Duration::ZERO && cleanup <= Duration::from_secs(5),
            "invalid restore cleanup bound"
        );
        let bound = Arc::new(RestoreBound {
            deadline: original,
            cleanup,
            owner,
        });
        Ok((
            Arc::new(RestoreObjects {
                inner: self.objects.clone(),
                bound: bound.clone(),
            }),
            Arc::new(self.query.for_restore(bound)),
        ))
    }
}
struct RestoreObjects {
    inner: Arc<RemoteObjectIo>,
    bound: Arc<RestoreBound>,
}
impl std::fmt::Debug for RestoreObjects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestoreObjects").finish_non_exhaustive()
    }
}
fn metadata_only<T>() -> BackendResult<T> {
    Err(crate::backend_error(anyhow::anyhow!(
        "restore provider is metadata-read-only"
    )))
}
impl RestoreObjects {
    async fn read<T>(
        &self,
        owner: QueryOwner,
        make: impl std::future::Future<Output = BackendResult<T>>,
    ) -> BackendResult<T> {
        let owner: QueryOwner = Arc::new((self.bound.owner.clone(), owner));
        io::operation_context_scope(self.bound.deadline, owner, async {
            io::check_deadline().map_err(crate::backend_error)?;
            make.await
        })
        .await
    }
}
#[async_trait]
impl NativeObjectIo for RestoreObjects {
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        self.inner.metadata_work_budget()
    }
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> BackendResult<Option<m::Descriptor>> {
        if key.kind == m::Kind::File {
            return metadata_only();
        }
        self.read(owner.clone(), self.inner.descriptor_with_owner(key, owner))
            .await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.read_metadata_with_owner(key, Arc::new(())).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> BackendResult<VerifiedMetadata> {
        if key.kind == m::Kind::File {
            return metadata_only();
        }
        self.read(
            owner.clone(),
            self.inner.read_metadata_with_owner(key, owner),
        )
        .await
    }
    async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
        metadata_only()
    }
    async fn read_file(&self, _: m::FileId) -> BackendResult<FileRead> {
        metadata_only()
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Send + Unpin),
        _: u64,
    ) -> BackendResult<m::FileId> {
        metadata_only()
    }
    async fn copy_catalog_snapshot(
        &self,
        _: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        metadata_only()
    }
}
