//! Portable raw index-artifact transport. Hash checks do not replace canonical
//! artifact decoding, complete graph verification or a native restore journal.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CloudIndexArtifactId {
    Root([u8; 32]),
    Node([u8; 32]),
}
impl CloudIndexArtifactId {
    pub fn of_root(bytes: &[u8]) -> Self {
        Self::Root(tinytree_model::cloud_index_root_id(bytes))
    }
    pub fn of_node(bytes: &[u8]) -> Self {
        Self::Node(tinytree_model::cloud_index_node_id(bytes))
    }
    fn wire(self) -> c::IndexArtifactId {
        let (kind, id) = match self {
            Self::Root(id) => (c::IndexArtifactKind::Root, id),
            Self::Node(id) => (c::IndexArtifactKind::Node, id),
        };
        c::IndexArtifactId {
            kind: kind as i32,
            semantic_id: id.to_vec(),
        }
    }
    fn matches(self, bytes: &[u8]) -> bool {
        match self {
            Self::Root(_) => self == Self::of_root(bytes),
            Self::Node(_) => self == Self::of_node(bytes),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudIndexArtifactDescriptor {
    pub id: CloudIndexArtifactId,
    pub selected: CloudRawDigest,
}
impl CloudIndexArtifactDescriptor {
    fn wire(self) -> c::IndexArtifactDescriptor {
        c::IndexArtifactDescriptor {
            id: Some(self.id.wire()),
            selected: Some(c::RawDigest {
                blake3: self.selected.blake3.to_vec(),
                size_bytes: self.selected.size_bytes,
            }),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudIndexWriteReceipt {
    pub descriptor: CloudIndexArtifactDescriptor,
    pub staging_lease: String,
    pub expires_at_unix_seconds: i64,
}
/// Hash-checked selected bytes. No payload extraction may bypass the retained
/// memory owner. Native callers must separately verify the canonical artifact
/// and complete graph before journaled, no-clobber restore installation.
pub struct CloudIndexArtifact {
    descriptor: CloudIndexArtifactDescriptor,
    payload: Vec<u8>,
    _owner: Arc<RetainedCloud>,
}
impl CloudIndexArtifact {
    pub fn descriptor(&self) -> CloudIndexArtifactDescriptor {
        self.descriptor
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}
impl std::fmt::Debug for CloudIndexArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudIndexArtifact")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}
fn index_descriptor(
    value: c::IndexArtifactDescriptor,
    max: usize,
) -> Result<CloudIndexArtifactDescriptor, Status> {
    let id = value.id.ok_or_else(corrupt)?;
    let raw = array(&id.semantic_id)?;
    let id = match c::IndexArtifactKind::try_from(id.kind).map_err(|_| corrupt())? {
        c::IndexArtifactKind::Root => CloudIndexArtifactId::Root(raw),
        c::IndexArtifactKind::Node => CloudIndexArtifactId::Node(raw),
        _ => return Err(corrupt()),
    };
    let raw = value.selected.ok_or_else(corrupt)?;
    if raw.size_bytes > max as u64 {
        return Err(limit());
    }
    Ok(CloudIndexArtifactDescriptor {
        id,
        selected: CloudRawDigest {
            blake3: array(&raw.blake3)?,
            size_bytes: raw.size_bytes,
        },
    })
}
impl NativeClient {
    pub async fn cloud_put_index_artifact(
        &self,
        id: CloudIndexArtifactId,
        payload: &[u8],
    ) -> Result<CloudIndexWriteReceipt, Status> {
        let l = &self.inner.config.cloud;
        if payload.len() > l.max_object_bytes {
            return Err(limit());
        }
        let deadline = self.deadline();
        let bound = payload.len().checked_add(4096).ok_or_else(limit)?;
        let budget = self
            .cloud_admit(deadline, self.cloud_memory(bound)?)
            .await?;
        let bytes = payload.to_vec();
        let expected = blocking_owned(deadline, budget, None, move || {
            if !id.matches(&bytes) {
                return Err(Status::invalid_argument(
                    "index artifact identity does not match payload",
                ));
            }
            Ok(CloudIndexArtifactDescriptor {
                id,
                selected: CloudRawDigest::of(&bytes),
            })
        })
        .await?;
        let (receipt, _budget) = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/PutIndexArtifact",
                Shape::CloudIndexReceipt,
                4096,
                bound,
                || c::PutIndexArtifactRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    descriptor: Some(expected.wire()),
                    payload: payload.to_vec(),
                },
            )
            .await?;
        let receipt: c::IndexArtifactWriteReceipt = receipt;
        let actual = index_descriptor(receipt.descriptor.ok_or_else(corrupt)?, l.max_object_bytes)?;
        // Canonical artifact identity binds exact bytes, unlike semantic OP/VIEW
        // identities that may retain another valid selected protobuf encoding.
        if actual != expected
            || !lease(&receipt.staging_lease)
            || receipt.expires_at_unix_seconds <= 0
        {
            return Err(corrupt());
        }
        Ok(CloudIndexWriteReceipt {
            descriptor: actual,
            staging_lease: receipt.staging_lease,
            expires_at_unix_seconds: receipt.expires_at_unix_seconds,
        })
    }
    pub async fn cloud_lookup_index_artifacts(
        &self,
        ids: &[CloudIndexArtifactId],
    ) -> Result<Vec<Option<CloudIndexArtifactDescriptor>>, Status> {
        let l = &self.inner.config.cloud;
        if ids.len() > l.max_lookup_items {
            return Err(limit());
        }
        let bound = ids
            .len()
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(limit)?;
        let (value, _budget) = self
            .cloud_call(
                self.deadline(),
                "/tinytree.cloud.v1.WorkspaceCloud/LookupIndexArtifacts",
                Shape::CloudIndexSelections,
                bound,
                bound,
                || c::LookupIndexArtifactsRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    ids: ids.iter().map(|id| id.wire()).collect(),
                },
            )
            .await?;
        let value: c::LookupIndexArtifactsResponse = value;
        if value.selections.len() != ids.len() {
            return Err(corrupt());
        }
        value
            .selections
            .into_iter()
            .zip(ids)
            .map(|(v, id)| {
                v.descriptor
                    .map(|v| {
                        let v = index_descriptor(v, l.max_object_bytes)?;
                        if v.id != *id {
                            return Err(corrupt());
                        }
                        Ok(v)
                    })
                    .transpose()
            })
            .collect()
    }
    pub async fn cloud_read_index_artifact(
        &self,
        expected: CloudIndexArtifactDescriptor,
    ) -> Result<CloudIndexArtifact, Status> {
        let l = &self.inner.config.cloud;
        if expected.selected.size_bytes > l.max_object_bytes as u64 {
            return Err(limit());
        }
        let maximum = usize::try_from(expected.selected.size_bytes).map_err(|_| limit())?;
        let memory = self.cloud_memory(maximum.max(l.max_chunk_bytes))?;
        self.cloud_payload_fits(maximum, memory)?;
        let retained = self.cloud_retained(maximum)?;
        let deadline = self.deadline();
        let (payload, budget) = self
            .retry(deadline, true, |allowance| async move {
                let budget = self.cloud_admit(deadline, memory).await?;
                let mut grpc = self.rpc_client(budget.clone());
                let mut stream = grpc
                    .cloud_index_read(
                        self.request(
                            c::ReadIndexArtifactRequest {
                                repository_id: self.inner.config.repository_id.clone(),
                                id: Some(expected.id.wire()),
                            },
                            allowance,
                        ),
                        l,
                    )
                    .await?
                    .into_inner();
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(maximum).map_err(|_| limit())?;
                let mut eof = false;
                loop {
                    let item = reapi::policy::timeout(
                        deadline.min(Instant::now() + self.inner.config.rpc.stream_idle_timeout()),
                        async { stream.message().await },
                    )
                    .await?;
                    let Some(chunk) = item else { break };
                    if eof
                        || index_descriptor(
                            chunk.descriptor.ok_or_else(corrupt)?,
                            l.max_object_bytes,
                        )? != expected
                        || chunk.offset != bytes.len() as u64
                        || chunk.data.len() > l.max_chunk_bytes
                        || bytes
                            .len()
                            .checked_add(chunk.data.len())
                            .is_none_or(|n| n > maximum)
                        || (chunk.data.is_empty() && !chunk.eof)
                    {
                        return Err(corrupt());
                    }
                    bytes.extend_from_slice(&chunk.data);
                    eof = chunk.eof;
                    if eof && bytes.len() != maximum {
                        return Err(corrupt());
                    }
                }
                if !eof || bytes.len() != maximum {
                    return Err(corrupt());
                }
                Ok((bytes, budget))
            })
            .await?;
        blocking_owned(deadline, budget, None, move || {
            let value = CloudIndexArtifact {
                descriptor: expected,
                payload,
                _owner: retained,
            };
            if CloudRawDigest::of(value.payload()) != expected.selected
                || !expected.id.matches(value.payload())
            {
                return Err(corrupt());
            }
            Ok(value)
        })
        .await
    }
}
