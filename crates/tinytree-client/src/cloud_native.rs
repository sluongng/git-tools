//! Retained-version authority originates only in an authenticated GetVersion.
//! It is distinct from independent canonical manifest/artifact coverage proof.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetainedVersionSelector {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: CloudManifestId,
}
/// Immutable, client-bound server proof. Mutable public CloudVersion fields
/// cannot be converted into this capability; only a fresh version RPC creates it.
pub struct VerifiedRetainedVersion {
    authority: c::NativeVersionAuthority,
    selector: RetainedVersionSelector,
    version: CloudVersion,
    _client: Arc<Inner>,
}
impl std::fmt::Debug for VerifiedRetainedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedRetainedVersion")
            .field("generation", &self.selector.generation)
            .finish_non_exhaustive()
    }
}
impl VerifiedRetainedVersion {
    pub fn selector(&self) -> RetainedVersionSelector {
        self.selector
    }
    pub fn manifest(&self) -> &[u8] {
        self.version.manifest()
    }
    pub fn manifest_selected(&self) -> CloudRawDigest {
        CloudRawDigest::of(self.manifest())
    }
    pub fn owner_binding(&self) -> [u8; 32] {
        self.authority
            .owner_binding
            .as_slice()
            .try_into()
            .expect("verified authority")
    }
    pub fn binding_digest(&self) -> [u8; 32] {
        self.authority
            .binding_digest
            .as_slice()
            .try_into()
            .expect("verified authority")
    }
}
impl NativeClient {
    pub async fn cloud_retained_version(
        &self,
        workspace: [u8; 32],
        generation: u64,
    ) -> Result<VerifiedRetainedVersion, Status> {
        let version = self.cloud_version(workspace, generation).await?;
        self.checked_retained_version(version)
    }
    /// Expected fields pin a response size/selection, NOT an authorization grant.
    /// This permits small exact versions under conservative provider pools
    /// without increasing defaults or trusting a server-declared allocation.
    pub async fn cloud_retained_version_selected(
        &self,
        selector: RetainedVersionSelector,
        selected: CloudRawDigest,
    ) -> Result<VerifiedRetainedVersion, Status> {
        let maximum = usize::try_from(selected.size_bytes).map_err(|_| limit())?;
        let version = self
            .cloud_version_bounded(selector.workspace_id, selector.generation, maximum)
            .await?;
        if version.manifest_id != selector.manifest_id
            || CloudRawDigest::of(version.manifest()) != selected
        {
            return Err(corrupt());
        }
        self.checked_retained_version(version)
    }
    fn checked_retained_version(
        &self,
        version: CloudVersion,
    ) -> Result<VerifiedRetainedVersion, Status> {
        let authority = version
            .native_authority
            .as_ref()
            .ok_or_else(|| Status::unimplemented("retained native authority unavailable"))?;
        if authority.version != 1 {
            return Err(Status::unimplemented(
                "retained native authority version unsupported",
            ));
        }
        let owner_binding = array(&authority.owner_binding)?;
        let raw = authority.manifest_selected.as_ref().ok_or_else(corrupt)?;
        let selected = CloudRawDigest {
            blake3: array(&raw.blake3)?,
            size_bytes: raw.size_bytes,
        };
        if selected != CloudRawDigest::of(version.manifest()) {
            return Err(corrupt());
        }
        let binding = tinytree_model::cloud_native_binding_digest(
            &self.inner.config.repository_id,
            owner_binding,
            version.workspace_id,
            version.generation,
            version.manifest_id.0,
            selected.blake3,
            selected.size_bytes,
        );
        if array::<32>(&authority.binding_digest)? != binding {
            return Err(corrupt());
        }
        Ok(VerifiedRetainedVersion {
            authority: authority.clone(),
            selector: RetainedVersionSelector {
                workspace_id: version.workspace_id,
                generation: version.generation,
                manifest_id: version.manifest_id,
            },
            version,
            _client: self.inner.clone(),
        })
    }
    fn closure_request(
        &self,
        version: &VerifiedRetainedVersion,
        terminal: ObjectKey,
        path: &[ObjectKey],
        requested: ObjectKey,
    ) -> Result<c::ReadNativeClosureRequest, Status> {
        if !Arc::ptr_eq(&self.inner, &version._client) {
            return Err(Status::failed_precondition(
                "retained version belongs to another client",
            ));
        }
        if path.len() > 256 || path.last().copied().unwrap_or(terminal) != requested {
            return Err(Status::invalid_argument("invalid native version edge path"));
        }
        let size = path
            .len()
            .checked_mul(40)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(limit)?;
        if size > self.inner.config.limits.max_request_bytes {
            return Err(limit());
        }
        Ok(c::ReadNativeClosureRequest {
            repository_id: self.inner.config.repository_id.clone(),
            selector: Some(c::RetainedVersionSelector {
                workspace_id: version.selector.workspace_id.to_vec(),
                generation: version.selector.generation,
                manifest_id: version.selector.manifest_id.0.to_vec(),
            }),
            authority: Some(version.authority.clone()),
            requested: Some(wire_key(requested)),
            proof: Some(c::NativeClosureProof {
                terminal: Some(c::native_closure_proof::Terminal::VersionRoot(wire_key(
                    terminal,
                ))),
                edge_path: path.iter().copied().map(wire_key).collect(),
            }),
        })
    }
    pub async fn cloud_read_native_metadata(
        &self,
        version: &VerifiedRetainedVersion,
        terminal: ObjectKey,
        path: &[ObjectKey],
        requested: ObjectKey,
    ) -> Result<VerifiedMetadata, Status> {
        if requested.kind == Kind::File || intrinsic(requested) {
            return Err(Status::invalid_argument(
                "expected nonintrinsic native metadata",
            ));
        }
        // Preflight borrowed shape before allocating an owned request.
        if path.len() > 256 {
            return Err(limit());
        }
        let request_bytes = path
            .len()
            .checked_mul(40)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(limit)?;
        if request_bytes > self.inner.config.limits.max_request_bytes {
            return Err(limit());
        }
        let deadline = self.deadline();
        let memory = self
            .inner
            .config
            .limits
            .max_frame_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(request_bytes * 4 + 16384))
            .ok_or_else(limit)?;
        let client = self
            .clone()
            .with_lifetime_guard(version.version._owner.clone());
        let binding = version.binding_digest();
        struct Raw {
            descriptor: Descriptor,
            bytes: Vec<u8>,
            retained: Arc<RetainedCloud>,
            budget: Arc<Budget>,
        }
        let raw = client
            .retry(deadline, true, |allowance| {
                let client = &client;
                async move {
                    let budget = client.cloud_admit(deadline, memory).await?;
                    let request = client.closure_request(version, terminal, path, requested)?;
                    let mut grpc = client.rpc_client(budget.clone());
                    let mut stream = grpc
                        .cloud_native_read(client.request(request, allowance))
                        .await?
                        .into_inner();
                    let mut retained = None;
                    let mut bytes = Vec::new();
                    let mut descriptor = None;
                    let mut eof = false;
                    let mut frames = 0usize;
                    while let Some(chunk) = client.message(&mut stream, deadline).await? {
                        if eof || array::<32>(&chunk.source_binding_digest)? != binding {
                            return Err(corrupt());
                        }
                        let current =
                            parse_descriptor(chunk.descriptor.as_ref().ok_or_else(corrupt)?)?;
                        if current.key != requested
                            || current.size_bytes
                                > client.inner.config.limits.max_metadata_bytes as u64
                        {
                            return Err(corrupt());
                        }
                        if let Some(expected) = descriptor {
                            if expected != current {
                                return Err(corrupt());
                            }
                        } else {
                            let n = usize::try_from(current.size_bytes).map_err(|_| limit())?;
                            // Native decoded containers/canonical scratch have a separate
                            // conservative charge, not the cloud raw-object 4MiB policy.
                            retained =
                                Some(client.cloud_retained(n.checked_mul(8).ok_or_else(limit)?)?);
                            bytes.try_reserve_exact(n).map_err(|_| limit())?;
                            descriptor = Some(current);
                        }
                        if chunk.offset != bytes.len() as u64
                            || chunk.data.len() > client.inner.config.limits.max_frame_bytes
                            || bytes
                                .len()
                                .checked_add(chunk.data.len())
                                .is_none_or(|n| n as u64 > current.size_bytes)
                            || (chunk.data.is_empty() && !chunk.eof && frames != 0)
                        {
                            return Err(corrupt());
                        }
                        bytes.extend_from_slice(&chunk.data);
                        frames += 1;
                        eof = chunk.eof;
                        if eof && bytes.len() as u64 != current.size_bytes {
                            return Err(corrupt());
                        }
                    }
                    if !eof {
                        return Err(corrupt());
                    }
                    Ok(Raw {
                        descriptor: descriptor.ok_or_else(corrupt)?,
                        bytes,
                        retained: retained.ok_or_else(corrupt)?,
                        budget,
                    })
                }
            })
            .await?;
        let budget = raw.budget.clone();
        let maximum = self.inner.config.limits.max_metadata_bytes;
        blocking_owned(deadline, budget, None, move || {
            let mut raw = raw;
            let limits = tinytree_model::Limits {
                max_object_bytes: maximum,
                ..Default::default()
            };
            tinytree_model::verify(&raw.descriptor, &raw.bytes, limits).map_err(|_| corrupt())?;
            let object = tinytree_model::decode(&raw.bytes, limits).map_err(|_| corrupt())?;
            let canonical = tinytree_model::encode(&object, limits).map_err(|_| corrupt())?;
            if canonical.descriptor != raw.descriptor || canonical.bytes != raw.bytes {
                return Err(corrupt());
            }
            drop(canonical);
            let result = VerifiedMetadata {
                descriptor: raw.descriptor,
                bytes: std::mem::take(&mut raw.bytes),
                object,
                _query_owner: Some(raw.retained.clone()),
            };
            drop(raw);
            Ok(result)
        })
        .await
    }
    pub async fn cloud_download_native_file(
        &self,
        version: &VerifiedRetainedVersion,
        terminal: ObjectKey,
        path: &[ObjectKey],
        file: tinytree_model::FileId,
        parent: &Path,
    ) -> Result<VerifiedFile, Status> {
        let requested = ObjectKey {
            kind: Kind::File,
            id: ObjectId(file.0),
        };
        if path.len() > 256 || parent.as_os_str().len() > 4096 {
            return Err(limit());
        }
        let request_bytes = path
            .len()
            .checked_mul(40)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(limit)?;
        if request_bytes > self.inner.config.limits.max_request_bytes {
            return Err(limit());
        }
        let client = self
            .clone()
            .with_lifetime_guard(version.version._owner.clone());
        let deadline = client.deadline();
        let binding = version.binding_digest();
        let memory = client
            .inner
            .config
            .limits
            .max_frame_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(request_bytes * 4 + 16384))
            .ok_or_else(limit)?;
        client
            .retry(deadline, true, |allowance| {
                let client = &client;
                async move {
                    let budget = client.cloud_admit(deadline, memory).await?;
                    let request = client.closure_request(version, terminal, path, requested)?;
                    let temporary = client.temporary(parent, deadline, budget.clone()).await?;
                    let held = temporary.clone();
                    let mut output = blocking_owned(
                        deadline,
                        budget.clone(),
                        Some(temporary.clone()),
                        move || {
                            held.file
                                .reopen()
                                .map_err(|_| Status::internal("cannot open native temporary file"))
                        },
                    )
                    .await?;
                    let mut grpc = client.rpc_client(budget.clone());
                    let mut stream = grpc
                        .cloud_native_read(client.request(request, allowance))
                        .await?
                        .into_inner();
                    let mut descriptor = None;
                    let mut offset = 0u64;
                    let mut eof = false;
                    let mut frames = 0usize;
                    let mut hash = blake3::Hasher::new();
                    while let Some(chunk) = client.message(&mut stream, deadline).await? {
                        if eof || array::<32>(&chunk.source_binding_digest)? != binding {
                            return Err(corrupt());
                        }
                        let current =
                            parse_descriptor(chunk.descriptor.as_ref().ok_or_else(corrupt)?)?;
                        if current.key != requested
                            || current.size_bytes > client.inner.config.limits.max_file_bytes
                        {
                            return Err(corrupt());
                        }
                        if descriptor.is_some_and(|v| v != current) {
                            return Err(corrupt());
                        }
                        descriptor = Some(current);
                        if chunk.offset != offset
                            || chunk.data.len() > client.inner.config.limits.max_frame_bytes
                            || offset
                                .checked_add(chunk.data.len() as u64)
                                .is_none_or(|n| n > current.size_bytes)
                            || (chunk.data.is_empty() && !chunk.eof && frames != 0)
                        {
                            return Err(corrupt());
                        }
                        offset += chunk.data.len() as u64;
                        hash.update(&chunk.data);
                        frames += 1;
                        eof = chunk.eof;
                        let held = temporary.clone();
                        let owner = budget.clone();
                        output = blocking_owned(
                            deadline,
                            budget.clone(),
                            Some(temporary.clone()),
                            move || {
                                use std::io::Write;
                                let _owners = (held, owner);
                                output.write_all(&chunk.data).map_err(|_| {
                                    Status::internal("cannot write native temporary file")
                                })?;
                                Ok(output)
                            },
                        )
                        .await?;
                    }
                    let descriptor = descriptor.ok_or_else(corrupt)?;
                    if !eof
                        || offset != descriptor.size_bytes
                        || hash.finalize().as_bytes() != &file.0
                    {
                        return Err(corrupt());
                    }
                    drop(output);
                    Ok(VerifiedFile {
                        temporary,
                        descriptor,
                    })
                }
            })
            .await
    }
}
