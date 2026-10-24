//! Raw selected-snapshot reads, without public-cache recursion or live refs.
use crate::{RemoteObjectIo, io, ledger::CloudQueryAuthority};
use anyhow::{Result, ensure};
use std::{any::Any, collections::HashMap, sync::Arc};
use tinytree_model as m;
impl RemoteObjectIo {
    async fn snapshot_client(
        &self,
        authority: &CloudQueryAuthority,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<(
        tinytree_client::NativeClient,
        tinytree_client::VerifiedRetainedVersion,
    )> {
        self.inner
            .ledger
            .validate_authority(&crate::ledger::Authority::CloudQuery(authority.clone()))?;
        let client = self
            .inner
            .connector
            .get()
            .await?
            .with_rpc_lifetime_guard(Arc::new(owner));
        let client = match io::scope() {
            Some(scope) => client.with_operation_deadline(scope.deadline),
            None => client,
        };
        let proof = io::network(
            client.cloud_retained_version_selected(authority.selector(), authority.selected_raw()),
        )
        .await?;
        ensure!(
            authority.agrees(&proof),
            "snapshot version ownership or source binding changed"
        );
        Ok((client, proof))
    }
    pub(crate) async fn snapshot_metadata(
        &self,
        authority: &CloudQueryAuthority,
        requested: m::ObjectKey,
        parents: &HashMap<m::ObjectKey, m::ObjectKey>,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<tinytree_client::VerifiedMetadata> {
        let ledger = self.inner.ledger.clone();
        let captured = authority.clone();
        let parents = parents.clone();
        let (terminal, path) = io::blocking_owned(owner.clone(), move || {
            ledger.cloud_query_path(&captured, requested, &parents)
        })
        .await?;
        let (client, proof) = self.snapshot_client(authority, owner).await?;
        Ok(io::network(client.cloud_read_snapshot_metadata(
            &proof,
            authority.indexes(),
            terminal,
            &path,
            requested,
        ))
        .await?)
    }
    pub(crate) async fn snapshot_file_authority(
        &self,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<CloudQueryAuthority>> {
        let ledger = self.inner.ledger.clone();
        io::blocking_owned(owner, move || ledger.cloud_query_file(id)).await
    }
    pub(crate) async fn download_snapshot_file_owned(
        &self,
        authority: CloudQueryAuthority,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<tinytree_client::VerifiedFile> {
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        tokio::time::timeout_at(deadline, async {
            let ledger = self.inner.ledger.clone();
            let captured = authority.clone();
            let key = m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            };
            let (terminal, path) = io::blocking_owned(owner.clone(), move || {
                ledger.cloud_query_path(&captured, key, &HashMap::new())
            })
            .await?;
            let (client, proof) = self.snapshot_client(&authority, owner).await?;
            Ok(io::network(client.cloud_download_snapshot_file(
                &proof,
                authority.indexes(),
                terminal,
                &path,
                id,
                &self.inner.staging,
            ))
            .await?)
        })
        .await
        .map_err(|_| anyhow::anyhow!("snapshot file deadline exceeded"))?
    }
}
