//! Durable remote operations, deliberately separate from local object I/O.
use crate::{
    Inner, backend,
    closure::Closure,
    hex, io,
    ledger::{Request, Root},
    provider::RemoteObjectIo,
    unhex,
};
use anyhow::{Context, Result, ensure};
use futures::AsyncReadExt;
use jj_native_backend::NativeObjectIo;
use std::sync::{Arc, Mutex};
use tinytree_client::NativeClient;
use tinytree_model as m;
#[derive(Clone)]
pub struct RemoteRepositoryControl {
    pub(crate) inner: Arc<Inner>,
}
impl std::fmt::Debug for RemoteRepositoryControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRepositoryControl")
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub struct AdmittedHead {
    pub head: m::CommitId,
    pub hold_name: String,
    pub hold_generation: u64,
    pub metadata_objects: usize,
    pub public_reference: Option<AdmittedReference>,
    _complete: (),
}
#[derive(Clone, Debug)]
pub struct AdmittedReference {
    pub name: String,
    pub generation: u64,
}
#[derive(Clone, Debug)]
pub struct ObservedReference {
    pub name: String,
    pub generation: u64,
    pub head: Option<AdmittedHead>,
}
#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub head: m::CommitId,
    pub reference_name: String,
    pub expected_generation: u64,
    pub idempotency_key: String,
}
#[derive(Clone, Debug)]
pub struct ConfirmedPublication {
    pub idempotency_key: String,
    pub reference_name: String,
    pub generation: u64,
    pub head: m::CommitId,
}
impl RemoteRepositoryControl {
    pub fn client_id(&self) -> [u8; 16] {
        self.inner.ledger.client
    }
    pub(crate) async fn refresh_scope_hold(&self, head: m::CommitId) -> Result<()> {
        let operation = async {
            let client = self.inner.connector.get().await?;
            let root = self.hold(&client, head).await?;
            self.current_hold(&client, root).await?;
            Ok(())
        };
        if io::in_control() {
            operation.await
        } else {
            let deadline = self.inner.deadline();
            self.run(deadline, operation).await
        }
    }
    pub async fn admit_reference(&self, name: &str) -> Result<AdmittedHead> {
        self.observe_reference(name)
            .await?
            .head
            .ok_or_else(|| tonic::Status::not_found("native reference is absent").into())
    }
    /// Observe absence/tombstone or admit the selected immutable head. Both
    /// values preserve the original public snapshot, never a later generation.
    pub async fn observe_reference(&self, name: &str) -> Result<ObservedReference> {
        ensure!(
            !name.is_empty() && name.len() <= 4096 && !name.contains('\0'),
            "invalid native admission reference name"
        );
        let deadline = self.inner.deadline();
        self.run(deadline, async {
            let client = self.inner.connector.get().await?;
            let view = io::network(client.open(vec![name.to_owned()])).await?;
            let reference = view
                .references
                .first()
                .context("native reference was omitted")?;
            let selected = AdmittedReference {
                name: reference.name.clone(),
                generation: reference.generation,
            };
            let Some(descriptor) = &reference.commit else {
                return Ok(ObservedReference {
                    name: selected.name,
                    generation: selected.generation,
                    head: None,
                });
            };
            let commit = descriptor
                .key
                .as_ref()
                .context("native reference target was omitted")?;
            ensure!(
                commit.kind == m::Kind::Commit as i32,
                "native reference is not a commit"
            );
            let head = m::CommitId(
                commit
                    .native_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid native commit identity"))?,
            );
            let mut admitted = self.admit(head).await?;
            admitted.public_reference = Some(selected.clone());
            Ok(ObservedReference {
                name: selected.name,
                generation: selected.generation,
                head: Some(admitted),
            })
        })
        .await
    }
    pub async fn admit_commit(&self, head: m::CommitId) -> Result<AdmittedHead> {
        let deadline = self.inner.deadline();
        self.run(deadline, self.admit(head)).await
    }
    pub(crate) async fn run<T>(
        &self,
        deadline: tokio::time::Instant,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        // Only admission is cancellable here. Once DirectObjectIo owns a job,
        // await its physical completion; the broker's owned request task keeps
        // this future alive even when the incoming RPC times out/disconnects.
        let permit = tokio::time::timeout_at(deadline, self.inner.control.clone().acquire_owned())
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("native control admission deadline exceeded")
            })??;
        let scope = Arc::new(io::ControlScope::new(permit, deadline));
        let result = io::control_scope(scope, operation).await;
        if tokio::time::Instant::now() >= deadline {
            return Err(tonic::Status::deadline_exceeded(
                "native remote operation deadline exceeded",
            )
            .into());
        }
        result
    }
    /// The trait adapter keeps the shared broker pool charged through every
    /// physical client/cache/copy worker, including timed-out owned jobs.
    pub(crate) async fn drain_control(&self) {
        if let Ok(permit) = self.inner.control.clone().acquire_owned().await {
            drop(permit);
        }
    }
    async fn admit(&self, head: m::CommitId) -> Result<AdmittedHead> {
        ensure!(
            head != m::CommitId::ROOT,
            "intrinsic root does not need remote admission"
        );
        let client = self.inner.connector.get().await?;
        let root = self.hold(&client, head).await?;
        let closure = Closure::collect(&self.inner, head, true).await?;
        // Parent-first order preserves CopyHistory's authoritative parent checks.
        for key in &closure.order {
            io::check_deadline()?;
            let node = &closure.nodes[key];
            if self
                .inner
                .local
                .descriptor(*key)
                .await
                .map_err(backend)?
                .is_none()
            {
                io::check_deadline()?;
                let descriptor = self
                    .inner
                    .local
                    .write_metadata(node.object.clone())
                    .await
                    .map_err(backend)?;
                io::check_deadline()?;
                ensure!(
                    descriptor == node.descriptor,
                    "native metadata installation changed canonical identity"
                );
            }
        }
        let root = self.current_hold(&client, root).await?;
        let descriptors = closure.descriptors();
        let count = descriptors.len();
        let files = closure.files;
        let ledger = self.inner.ledger.clone();
        let installed = root.clone();
        io::blocking(move || ledger.completed(installed, &descriptors, &files)).await?;
        Ok(AdmittedHead {
            head,
            hold_name: root.hold_name,
            hold_generation: root
                .generation
                .context("native hold proof missing generation")?,
            metadata_objects: count,
            public_reference: None,
            _complete: (),
        })
    }
    async fn hold(&self, client: &NativeClient, head: m::CommitId) -> Result<Root> {
        let ledger = self.inner.ledger.clone();
        let existing = io::blocking(move || ledger.root(head)).await?;
        let mut root = existing.unwrap_or(Root {
            head: hex(&head.0),
            hold_name: format!(
                "_tinytree/hold/v1/{}/{}",
                hex(&self.inner.ledger.client),
                hex(&head.0)
            ),
            request: None,
            receipt: None,
            receipt_binding: None,
            generation: None,
            binding: self.inner.ledger.binding.scope.clone(),
            complete: false,
        });
        // A durable intent and stable client identity precede even the lookup.
        let ledger = self.inner.ledger.clone();
        let saved = root.clone();
        io::blocking(move || ledger.save_root(&saved)).await?;
        let view = io::network(client.open(vec![root.hold_name.clone()])).await?;
        let reference = view
            .references
            .first()
            .context("native hold lookup omitted reference")?;
        if let Some(descriptor) = &reference.commit {
            let id = descriptor
                .key
                .as_ref()
                .context("native hold omitted target")?;
            ensure!(
                id.kind == m::Kind::Commit as i32 && id.native_id == head.0,
                "native hold has another immutable target"
            );
            root.generation = Some(reference.generation);
            root.request = None;
            root.binding = self.inner.ledger.binding.scope.clone();
            let ledger = self.inner.ledger.clone();
            let saved = root.clone();
            io::blocking(move || ledger.save_root(&saved)).await?;
            return Ok(root);
        }
        if root
            .request
            .as_ref()
            .is_none_or(|request| request.generation != reference.generation)
        {
            root.request = Some(Request {
                id: format!("hold-{}", uuid::Uuid::new_v4().simple()),
                name: root.hold_name.clone(),
                generation: reference.generation,
                commit: Some(hex(&head.0)),
                leases: Vec::new(),
            });
            let ledger = self.inner.ledger.clone();
            let saved = root.clone();
            io::blocking(move || ledger.save_root(&saved)).await?;
        }
        let request = root
            .request
            .as_ref()
            .context("native pending hold request missing")?;
        io::network(client.publish(request.wire(&self.inner.config.remote.repository_id)?)).await?;
        root.receipt = root.request.clone();
        root.receipt_binding = Some(self.inner.ledger.binding.scope.clone());
        self.current_hold(client, root).await
    }
    async fn current_hold(&self, client: &NativeClient, mut root: Root) -> Result<Root> {
        let view = io::network(client.open(vec![root.hold_name.clone()])).await?;
        let reference = view
            .references
            .first()
            .context("native current hold was omitted")?;
        let key = reference
            .commit
            .as_ref()
            .and_then(|d| d.key.as_ref())
            .context("native hold is no longer current")?;
        ensure!(
            key.kind == m::Kind::Commit as i32 && hex(&key.native_id) == root.head,
            "native current hold target changed"
        );
        root.generation = Some(reference.generation);
        root.binding = self.inner.ledger.binding.scope.clone();
        root.request = None;
        let ledger = self.inner.ledger.clone();
        let saved = root.clone();
        io::blocking(move || ledger.save_root(&saved)).await?;
        Ok(root)
    }
    pub async fn publish(&self, request: PublishRequest) -> Result<ConfirmedPublication> {
        let deadline = self.inner.deadline();
        self.run(deadline, self.publish_inner(request, deadline))
            .await
    }
    async fn publish_inner(
        &self,
        request: PublishRequest,
        deadline: tokio::time::Instant,
    ) -> Result<ConfirmedPublication> {
        ensure!(
            request.head != m::CommitId::ROOT
                && request.reference_name != "_tinytree"
                && !request.reference_name.starts_with("_tinytree/"),
            "native public publication requires a non-root head and public reference"
        );
        ensure!(
            !request.reference_name.is_empty()
                && request.reference_name.len() <= 4096
                && !request.reference_name.contains('\0'),
            "invalid native publication reference name"
        );
        ensure!(
            !request.idempotency_key.is_empty()
                && request.idempotency_key.len() <= 128
                && request
                    .idempotency_key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid native publication idempotency key"
        );
        let pending = Request {
            id: request.idempotency_key.clone(),
            name: request.reference_name.clone(),
            generation: request.expected_generation,
            commit: Some(hex(&request.head.0)),
            leases: Vec::new(),
        };
        let ledger = self.inner.ledger.clone();
        let saved = pending.clone();
        io::blocking(move || ledger.save_pending(&saved)).await?;
        let client = self.inner.connector.get().await?;
        let closure = Closure::collect(&self.inner, request.head, false).await?;
        let mut descriptors = closure.descriptors();
        for (id, size, _) in &closure.files {
            descriptors.push(m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::File,
                    id: *id,
                },
                size_bytes: *size,
                format_version: m::FORMAT_VERSION,
            });
        }
        let mut missing = std::collections::HashSet::new();
        for batch in descriptors.chunks(self.inner.config.remote.limits.max_batch_items) {
            for key in io::network(
                client.find_missing(batch.iter().map(|descriptor| descriptor.key).collect()),
            )
            .await?
            {
                missing.insert(key);
            }
        }
        // Upload parent-first metadata, and only verified immutable file sources.
        for key in &closure.order {
            if missing.contains(key) {
                let node = &closure.nodes[key];
                let object =
                    io::network(client.verify_metadata(node.descriptor, node.bytes.clone()))
                        .await?;
                io::network(client.upload_metadata(object)).await?;
            }
        }
        let provider = RemoteObjectIo {
            inner: self.inner.clone(),
        };
        for (id, size, _) in &closure.files {
            let key = m::ObjectKey {
                kind: m::Kind::File,
                id: *id,
            };
            if !missing.contains(&key) {
                continue;
            }
            let descriptor = m::Descriptor {
                key,
                size_bytes: *size,
                format_version: m::FORMAT_VERSION,
            };
            let read = provider.read_file(m::FileId(id.0)).await.map_err(backend)?;
            let source = self
                .upload_snapshot(read, descriptor, deadline, &client)
                .await?;
            io::network(client.upload_file(source)).await?;
        }
        let root = self.hold(&client, request.head).await?;
        let root = self.current_hold(&client, root).await?;
        // Metadata is already durable locally; this proof also makes the remote
        // complete closure live before the public generation CAS is attempted.
        let ledger = self.inner.ledger.clone();
        let members = closure.descriptors();
        let files = closure.files;
        io::blocking(move || ledger.completed(root, &members, &files)).await?;
        let publication = match io::network(
            client.publish(pending.wire(&self.inner.config.remote.repository_id)?),
        )
        .await
        {
            Ok(publication) => publication,
            Err(error) => {
                if matches!(
                    error.code(),
                    tonic::Code::Aborted | tonic::Code::AlreadyExists
                ) {
                    let ledger = self.inner.ledger.clone();
                    let id = pending.id.clone();
                    io::blocking(move || ledger.discard_pending(&id)).await?;
                }
                return Err(error.into());
            }
        };
        let reference = publication
            .reference
            .context("native publication receipt omitted reference")?;
        let ledger = self.inner.ledger.clone();
        let finished = pending.clone();
        io::blocking(move || ledger.finish_publication(&finished, reference.generation)).await?;
        Ok(ConfirmedPublication {
            idempotency_key: pending.id,
            reference_name: pending.name,
            generation: reference.generation,
            head: request.head,
        })
    }
    async fn upload_snapshot(
        &self,
        read: jj_native_backend::FileRead,
        descriptor: m::Descriptor,
        deadline: tokio::time::Instant,
        client: &NativeClient,
    ) -> Result<tinytree_client::UploadSource> {
        ensure!(
            descriptor.size_bytes <= self.inner.config.remote.limits.max_file_bytes,
            "native upload file exceeds client bound"
        );
        let quota = Arc::new(
            tokio::time::timeout_at(
                deadline,
                self.inner.spool.clone().acquire_many_owned(u32::try_from(
                    descriptor
                        .size_bytes
                        .checked_mul(2)
                        .context("native double-spool size overflow")?,
                )?),
            )
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("native upload spool admission deadline exceeded")
            })??,
        );
        let parent = self.inner.staging.clone();
        let held = quota.clone();
        let staging_owner = self.inner.staging_owner.clone();
        let spool = io::blocking(move || {
            let file = tempfile::Builder::new()
                .prefix("native-publish-")
                .tempfile_in(parent)?;
            Ok(Arc::new(Spool {
                file: Mutex::new(file),
                _quota: held,
                _staging_owner: staging_owner,
            }))
        })
        .await?;
        let (actual, mut reader) = read.into_parts();
        ensure!(
            actual == descriptor,
            "native publication source descriptor mismatch"
        );
        let mut size = 0u64;
        let mut hash = blake3::Hasher::new();
        loop {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "native publication file spool deadline exceeded"
            );
            let mut bytes = vec![0; 65536];
            let n = reader.read(&mut bytes).await?;
            if n == 0 {
                break;
            }
            bytes.truncate(n);
            size = size
                .checked_add(n as u64)
                .context("native publication file size overflow")?;
            ensure!(
                size <= descriptor.size_bytes,
                "native publication source exceeds declared size"
            );
            hash.update(&bytes);
            let held = spool.clone();
            io::blocking(move || {
                use std::io::Write;
                held.file
                    .lock()
                    .map_err(|_| anyhow::anyhow!("native publication spool requires recovery"))?
                    .write_all(&bytes)?;
                Ok(())
            })
            .await?;
        }
        ensure!(
            size == descriptor.size_bytes && hash.finalize().as_bytes() == &descriptor.key.id.0,
            "native publication source failed complete verification"
        );
        let held = spool.clone();
        let path = io::blocking(move || {
            let file = held
                .file
                .lock()
                .map_err(|_| anyhow::anyhow!("native publication spool requires recovery"))?;
            file.as_file().sync_all()?;
            Ok(file.path().to_path_buf())
        })
        .await?;
        // Attach before admission/copy begins: cancellation can leave a client
        // blocking snapshot worker alive before an UploadSource is returned.
        let scoped_client = client.clone().with_lifetime_guard(quota.clone());
        let source =
            io::network(scoped_client.prepare_upload_file(&path, descriptor, &self.inner.staging))
                .await?;
        drop(spool);
        drop(quota);
        Ok(source)
    }
    /// Reconcile exactly one durable intent. A missing record is no confirmed
    /// outcome, never proof that an upstream operation did not commit.
    pub async fn publication(&self, id: &str) -> Result<Option<ConfirmedPublication>> {
        ensure!(
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid native publication key"
        );
        let deadline = self.inner.deadline();
        self.run(deadline, async {
            let ledger = self.inner.ledger.clone();
            let key = id.to_owned();
            let receipt = io::blocking(move || ledger.receipt(&key)).await?;
            if let Some(receipt) = receipt {
                ensure!(
                    receipt.request.id == id
                        && receipt.request.generation.checked_add(1) == Some(receipt.generation),
                    "native durable publication receipt is corrupt"
                );
                let head = m::CommitId(unhex(
                    receipt
                        .request
                        .commit
                        .as_deref()
                        .context("native publication receipt target missing")?,
                )?);
                return Ok(Some(ConfirmedPublication {
                    idempotency_key: receipt.request.id,
                    reference_name: receipt.request.name,
                    generation: receipt.generation,
                    head,
                }));
            }
            let ledger = self.inner.ledger.clone();
            let key = id.to_owned();
            let pending = io::blocking(move || ledger.pending_request(&key)).await?;
            let Some(request) = pending else {
                return Ok(None);
            };
            ensure!(
                request.id == id,
                "native durable publication intent is corrupt"
            );
            let head = m::CommitId(unhex(
                request
                    .commit
                    .as_deref()
                    .context("native pending publication target missing")?,
            )?);
            self.publish_inner(
                PublishRequest {
                    head,
                    reference_name: request.name,
                    expected_generation: request.generation,
                    idempotency_key: request.id,
                },
                deadline,
            )
            .await
            .map(Some)
        })
        .await
    }
    /// Rebuild disposable file-size/provenance hints exclusively from already
    /// admitted canonical local trees. Does not fetch payloads or renew holds.
    pub async fn rebuild_file_index(&self) -> Result<usize> {
        let deadline = self.inner.deadline();
        self.run(deadline, async {
            let ledger = self.inner.ledger.clone();
            let roots = io::blocking(move || ledger.roots()).await?;
            let mut claims =
                std::collections::HashMap::<m::ObjectId, crate::ledger::FileClaim>::new();
            for root in roots.into_iter().filter(|root| root.complete) {
                let closure =
                    Closure::collect(&self.inner, m::CommitId(unhex(&root.head)?), false).await?;
                for (id, size, tree) in closure.files {
                    if let Some(existing) = claims.get(&id) {
                        ensure!(
                            existing.size == size,
                            "conflicting native admitted file lengths"
                        );
                    } else {
                        claims.insert(
                            id,
                            crate::ledger::FileClaim {
                                size,
                                root: root.head.clone(),
                                tree: hex(&tree.0),
                            },
                        );
                    }
                    ensure!(
                        claims.len() <= self.inner.config.max_file_index_entries,
                        "native derived file index count exceeded"
                    );
                }
            }
            let count = claims.len();
            let ledger = self.inner.ledger.clone();
            io::blocking(move || ledger.replace_file_index(claims.into_iter().collect())).await?;
            Ok(count)
        })
        .await
    }
    pub async fn reconcile_pending(&self) -> Result<usize> {
        let deadline = self.inner.deadline();
        self.run(deadline, async {
            let ledger = self.inner.ledger.clone();
            let roots = io::blocking(move || ledger.roots()).await?;
            let mut completed = 0;
            for root in roots {
                if !root.complete
                    || root.request.is_some()
                    || root.binding != self.inner.ledger.binding.scope
                {
                    self.admit(m::CommitId(unhex(&root.head)?)).await?;
                    completed += 1;
                }
            }
            let ledger = self.inner.ledger.clone();
            let pending = io::blocking(move || ledger.pending()).await?;
            for request in pending {
                let head = m::CommitId(unhex(
                    request
                        .commit
                        .as_deref()
                        .context("native pending public target missing")?,
                )?);
                self.publish_inner(
                    PublishRequest {
                        head,
                        reference_name: request.name,
                        expected_generation: request.generation,
                        idempotency_key: request.id,
                    },
                    deadline,
                )
                .await?;
                completed += 1;
            }
            Ok(completed)
        })
        .await
    }
}
struct Spool {
    file: Mutex<tempfile::NamedTempFile>,
    _quota: Arc<tokio::sync::OwnedSemaphorePermit>,
    _staging_owner: Arc<crate::staging::Owner>,
}
