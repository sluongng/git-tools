//! Local durable writes plus account-view-scoped projected source reads.
//! This adapter never calls the canonical repository, query or publish APIs.
use crate::{client::run_owned, wire};
use async_trait::async_trait;
use futures::AsyncRead;
use jj_lib::backend::BackendResult;
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, DirectObjectIo, FileRead, NativeObjectIo,
    ObjectIoResources, VerifiedMetadata,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tinytree_client::{NativeClient, forge_protocol as f};
use tinytree_model as m;
use tonic::Status;
type Owner = Arc<dyn std::any::Any + Send + Sync>;
const MAX_OBJECT: usize = 1024 * 1024;
const MAX_FILE: usize = 64 * 1024 * 1024;
const CHUNK: usize = 65536;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeRemoteConfig {
    pub account_id: String,
    pub view: String,
    pub view_version: Option<[u8; 32]>,
    pub client: tinytree_client::Config,
}
impl ForgeRemoteConfig {
    pub fn validate(&self) -> Result<(), Status> {
        let name = |s: &str| {
            !s.is_empty()
                && s.len() <= 64
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        };
        if !name(&self.account_id) || !name(&self.view) {
            return Err(Status::invalid_argument(
                "invalid forge account/view configuration",
            ));
        }
        self.client
            .validate()
            .map_err(|_| Status::invalid_argument("invalid forge remote transport configuration"))
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeProjection {
    pub account_id: String,
    pub view: String,
    pub view_version: [u8; 32],
    pub revision: [u8; 32],
    pub policy_epoch: u64,
    pub root_prefix: String,
    pub projected_commit: [u8; 32],
    pub projected_tree: [u8; 32],
    pub paths: Vec<String>,
    pub readme: Option<serde_json::Value>,
    pub readme_error: Option<serde_json::Value>,
    pub complete: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectReply {
    kind: u8,
    id: [u8; 32],
    data: Vec<u8>,
    revision: [u8; 32],
    view_version: [u8; 32],
    policy_epoch: u64,
    size_bytes: u64,
    format_version: u16,
    offset: u64,
    eof: bool,
}
#[derive(Clone)]
pub struct ForgeObjectIo {
    local: Arc<DirectObjectIo>,
    client: NativeClient,
    config: ForgeRemoteConfig,
    projection: ForgeProjection,
}
impl std::fmt::Debug for ForgeObjectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForgeObjectIo").finish_non_exhaustive()
    }
}
impl ForgeObjectIo {
    pub async fn open(
        local: Arc<DirectObjectIo>,
        config: ForgeRemoteConfig,
    ) -> Result<Self, Status> {
        Self::open_with_ca_certificate(local, config, None).await
    }
    pub async fn open_with_ca_certificate(
        local: Arc<DirectObjectIo>,
        config: ForgeRemoteConfig,
        ca: Option<Arc<[u8]>>,
    ) -> Result<Self, Status> {
        config.validate()?;
        let client = match ca {
            Some(ca) => {
                NativeClient::connect_with_ca_certificate(config.client.clone(), ca).await?
            }
            None => NativeClient::connect(config.client.clone()).await?,
        };
        let reply = client
            .forge_resolve_view(f::ForgeCallRequest {
                operation: f::ForgeOperation::ResolveView as i32,
                account_id: config.account_id.clone(),
                view: config.view.clone(),
                request_id: String::new(),
                payload_json: serde_json::to_vec(&json!({"version":config.view_version}))
                    .map_err(|_| Status::internal("forge request encoding failed"))?,
            })
            .await?;
        let projection: ForgeProjection = serde_json::from_slice(&reply.result_json)
            .map_err(|_| Status::data_loss("invalid forge projection reply"))?;
        if projection.account_id != config.account_id
            || projection.view != config.view
            || config
                .view_version
                .is_some_and(|v| v != projection.view_version)
            || projection.projected_commit == [0; 32]
            || !projection.complete
        {
            return Err(Status::data_loss(
                "forge projection scope is incomplete or mismatched",
            ));
        }
        Ok(Self {
            local,
            client,
            config,
            projection,
        })
    }
    fn bounded(&self) -> Self {
        let mut value = self.clone();
        value.client = value.client.with_operation_deadline(
            tokio::time::Instant::now() + self.config.client.rpc.operation_timeout(),
        );
        value
    }
    pub fn projection(&self) -> &ForgeProjection {
        &self.projection
    }
    pub fn local(&self) -> &Arc<DirectObjectIo> {
        &self.local
    }
    async fn read_chunk(
        &self,
        key: m::ObjectKey,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ObjectReply, Status> {
        let input=f::ForgeCallRequest {
            operation:f::ForgeOperation::ReadObject as i32,account_id:self.config.account_id.clone(),view:self.config.view.clone(),request_id:String::new(),
            payload_json:serde_json::to_vec(&json!({"version":self.projection.view_version,"kind":key.kind as u8,"id":key.id.0,"offset":offset,"max_bytes":max_bytes})).map_err(|_|Status::internal("forge request encoding failed"))?,
        };
        let reply = self.client.forge_call(input).await?;
        let value: ObjectReply = serde_json::from_slice(&reply.result_json)
            .map_err(|_| Status::data_loss("invalid forge object reply"))?;
        let cap = if key.kind == m::Kind::File {
            MAX_FILE
        } else {
            MAX_OBJECT
        };
        if value.kind != key.kind as u8
            || value.id != key.id.0
            || value.view_version != self.projection.view_version
            || value.revision != self.projection.revision
            || value.policy_epoch != self.projection.policy_epoch
            || value.size_bytes > cap as u64
            || value.format_version != m::FORMAT_VERSION
            || value.offset != offset
            || value.data.len() > max_bytes
            || offset > value.size_bytes
            || offset
                .checked_add(value.data.len() as u64)
                .is_none_or(|end| end > value.size_bytes)
            || (max_bytes > 0
                && value.eof != (offset + value.data.len() as u64 == value.size_bytes))
        {
            return Err(Status::data_loss(
                "forge object identity, range or bound mismatch",
            ));
        }
        Ok(value)
    }
    async fn remote_descriptor(&self, key: m::ObjectKey) -> Result<m::Descriptor, Status> {
        let value = self.read_chunk(key, 0, 0).await?;
        Ok(m::Descriptor {
            key,
            size_bytes: value.size_bytes,
            format_version: value.format_version,
        })
    }
    async fn fetch(&self, key: m::ObjectKey) -> Result<(m::Descriptor, Vec<u8>), Status> {
        let descriptor = self.remote_descriptor(key).await?;
        let mut data = Vec::with_capacity(descriptor.size_bytes as usize);
        loop {
            let value = self.read_chunk(key, data.len() as u64, CHUNK).await?;
            if value.size_bytes != descriptor.size_bytes || (value.data.is_empty() && !value.eof) {
                return Err(Status::data_loss(
                    "forge object changed or stopped making progress",
                ));
            }
            data.extend_from_slice(&value.data);
            if value.eof {
                break;
            }
        }
        m::verify(
            &descriptor,
            &data,
            m::Limits {
                max_object_bytes: if key.kind == m::Kind::File {
                    MAX_FILE
                } else {
                    MAX_OBJECT
                },
                ..Default::default()
            },
        )
        .map_err(|_| Status::data_loss("forge object canonical hash mismatch"))?;
        Ok((descriptor, data))
    }
    async fn hydrate(&self, key: m::ObjectKey, owner: Owner) -> Result<m::Descriptor, Status> {
        self.hydrate_at(key, owner, 128).await
    }
    async fn hydrate_at(
        &self,
        key: m::ObjectKey,
        owner: Owner,
        remaining: u16,
    ) -> Result<m::Descriptor, Status> {
        if remaining == 0 {
            return Err(Status::resource_exhausted(
                "forge projected ancestry bound exceeded",
            ));
        }
        if let Some(value) = self
            .local
            .descriptor_with_owner(key, owner.clone())
            .await
            .map_err(wire::backend)?
        {
            return Ok(value);
        }
        let (descriptor, bytes) = self.fetch(key).await?;
        let installed = if key.kind == m::Kind::File {
            let mut source = futures::io::Cursor::new(bytes);
            let id = self
                .local
                .write_file_with_owner(&mut source, descriptor.size_bytes, owner)
                .await
                .map_err(wire::backend)?;
            if id.0 != key.id.0 {
                return Err(Status::data_loss(
                    "forge file installation identity mismatch",
                ));
            }
            descriptor
        } else {
            let object = m::decode(
                &bytes,
                m::Limits {
                    max_object_bytes: MAX_OBJECT,
                    ..Default::default()
                },
            )
            .map_err(|_| Status::data_loss("invalid forge metadata"))?;
            if let m::Object::CopyHistory(copy) = &object {
                if !copy.parents.is_empty() {
                    return Err(Status::data_loss(
                        "forge seed exposes unexpected copy ancestry",
                    ));
                }
            }
            if let m::Object::Commit(commit) = &object {
                let baseline = key.id.0 == self.projection.projected_commit;
                if !commit.predecessors.is_empty()
                    || commit.secure_signature.is_some()
                    || commit.parents.len() != 1
                    || commit.root_tree.terms.len() != 1
                    || (baseline
                        && (commit.parents[0] != m::CommitId::ROOT
                            || commit.root_tree.terms[0]
                                != m::TreeId(self.projection.projected_tree)))
                    || (!baseline && commit.parents[0] == m::CommitId::ROOT)
                {
                    return Err(Status::data_loss(
                        "forge projection exposes unexpected ancestry",
                    ));
                }
                if !baseline {
                    Box::pin(self.hydrate_at(
                        m::ObjectKey {
                            kind: m::Kind::Commit,
                            id: m::ObjectId(commit.parents[0].0),
                        },
                        owner.clone(),
                        remaining - 1,
                    ))
                    .await?;
                }
            }
            self.local
                .write_metadata_with_owner(object, owner)
                .await
                .map_err(wire::backend)?
        };
        if installed != descriptor {
            return Err(Status::data_loss(
                "forge metadata installation identity mismatch",
            ));
        }
        Ok(installed)
    }
}
#[async_trait]
impl NativeObjectIo for ForgeObjectIo {
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        Some(ObjectIoResources {
            memory_bytes: 256 * 1024 * 1024,
            temporary_bytes: 2 * MAX_FILE as u64,
        })
    }
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Owner,
    ) -> BackendResult<Option<m::Descriptor>> {
        let this = self.bounded();
        run_owned(owner.clone(), async move {
            if let Some(value) = this
                .local
                .descriptor_with_owner(key, owner)
                .await
                .map_err(wire::backend)?
            {
                return Ok(Some(value));
            }
            this.remote_descriptor(key).await.map(Some)
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
        let this = self.bounded();
        run_owned(owner.clone(), async move {
            this.hydrate(key, owner.clone()).await?;
            this.local
                .read_metadata_with_owner(key, owner)
                .await
                .map_err(wire::backend)
        })
        .await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.read_file_with_owner(id, Arc::new(())).await
    }
    async fn read_file_with_owner(&self, id: m::FileId, owner: Owner) -> BackendResult<FileRead> {
        let this = self.bounded();
        run_owned(owner.clone(), async move {
            this.hydrate(
                m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(id.0),
                },
                owner.clone(),
            )
            .await?;
            this.local
                .read_file_with_owner(id, owner)
                .await
                .map_err(wire::backend)
        })
        .await
    }
    async fn read_file_for_cache(&self, id: m::FileId, owner: Owner) -> BackendResult<FileRead> {
        self.read_file_with_owner(id, owner).await
    }
    async fn read_local_file_bounded(
        &self,
        expected: m::Descriptor,
        max_bytes: u64,
        owner: Owner,
    ) -> BackendResult<FileRead> {
        self.local
            .read_local_file_bounded(expected, max_bytes, owner)
            .await
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.write_metadata_with_owner(object, Arc::new(())).await
    }
    async fn write_metadata_with_owner(
        &self,
        object: m::Object,
        owner: Owner,
    ) -> BackendResult<m::Descriptor> {
        let this = self.bounded();
        run_owned(owner.clone(), async move {
            if let m::Object::CopyHistory(copy) = &object {
                if copy.parents.len() > 128 {
                    return Err(Status::resource_exhausted(
                        "forge copy parent bound exceeded",
                    ));
                }
                for parent in &copy.parents {
                    this.hydrate(
                        m::ObjectKey {
                            kind: m::Kind::CopyHistory,
                            id: m::ObjectId(parent.0),
                        },
                        owner.clone(),
                    )
                    .await?;
                }
            }
            this.local
                .write_metadata_with_owner(object, owner)
                .await
                .map_err(wire::backend)
        })
        .await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId> {
        self.local.write_file(input, max_bytes).await
    }
    async fn write_file_with_owner(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
        owner: Owner,
    ) -> BackendResult<m::FileId> {
        self.local
            .write_file_with_owner(input, max_bytes, owner)
            .await
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.local.copy_catalog_snapshot(limits).await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Owner,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.local
            .copy_catalog_snapshot_with_owner(limits, owner)
            .await
    }
}

/// Immutable projected-reference control. Canonical publication is unavailable.
#[async_trait]
impl crate::NativeRepositoryControl for ForgeObjectIo {
    fn resources(&self) -> crate::ControlResources {
        crate::ControlResources {
            memory_bytes: 256 * 1024 * 1024,
            temporary_bytes: 2 * MAX_FILE as u64,
        }
    }
    async fn observe_reference(&self, name: &str) -> Result<crate::ReferenceSnapshot, Status> {
        self.observe_reference_with_owner(name, Arc::new(())).await
    }
    async fn observe_reference_with_owner(
        &self,
        name: &str,
        owner: Owner,
    ) -> Result<crate::ReferenceSnapshot, Status> {
        if name != "main" {
            return Err(Status::permission_denied(
                "forge views expose only the projected main snapshot",
            ));
        }
        let this = self.bounded();
        let head = m::CommitId(self.projection.projected_commit);
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head.0),
        };
        run_owned(owner.clone(), async move {
            this.hydrate(key, owner).await?;
            Ok(crate::ReferenceSnapshot {
                name: "main".into(),
                generation: this.projection.policy_epoch,
                head: Some(crate::AdmittedCommit {
                    head,
                    metadata_objects: 1,
                }),
            })
        })
        .await
        .map_err(wire::backend)
    }
    async fn admit_commit(&self, head: m::CommitId) -> Result<crate::AdmittedCommit, Status> {
        self.admit_commit_with_owner(head, Arc::new(())).await
    }
    async fn admit_commit_with_owner(
        &self,
        head: m::CommitId,
        owner: Owner,
    ) -> Result<crate::AdmittedCommit, Status> {
        // Admission here is local source readability, not server review approval.
        let this = self.bounded();
        run_owned(owner.clone(), async move {
            this.hydrate(
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(head.0),
                },
                owner.clone(),
            )
            .await?;
            this.local
                .read_metadata_with_owner(
                    m::ObjectKey {
                        kind: m::Kind::Commit,
                        id: m::ObjectId(head.0),
                    },
                    owner,
                )
                .await
                .map_err(wire::backend)?;
            Ok(crate::AdmittedCommit {
                head,
                metadata_objects: 1,
            })
        })
        .await
        .map_err(wire::backend)
    }
    async fn publish(
        &self,
        _: crate::PublishRequest,
    ) -> Result<crate::ConfirmedPublication, Status> {
        Err(Status::permission_denied(
            "forge changes must use patchset review, not direct publication",
        ))
    }
    async fn publication(&self, _: &str) -> Result<Option<crate::ConfirmedPublication>, Status> {
        Err(Status::permission_denied(
            "canonical publication receipts are unavailable in a forge view",
        ))
    }
    async fn publish_with_owner(
        &self,
        _: crate::PublishRequest,
        _: Owner,
    ) -> Result<crate::ConfirmedPublication, Status> {
        Err(Status::permission_denied(
            "forge changes must use patchset review, not direct publication",
        ))
    }
    async fn publication_with_owner(
        &self,
        _: &str,
        _: Owner,
    ) -> Result<Option<crate::ConfirmedPublication>, Status> {
        Err(Status::permission_denied(
            "canonical publication receipts are unavailable in a forge view",
        ))
    }
}
