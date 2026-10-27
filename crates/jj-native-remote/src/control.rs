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
    _retained: (),
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
// Publication diagnostics contain only source-defined phases and typed categories.
// Never format the error or any request/provider value on this path.
fn publication_failure(phase: &'static str, error: &anyhow::Error) {
    let category = if let Some(status) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
    {
        match status.code() {
            tonic::Code::Ok => "rpc_ok_error",
            tonic::Code::Cancelled => "rpc_cancelled",
            tonic::Code::Unknown => "rpc_unknown",
            tonic::Code::InvalidArgument => "rpc_invalid_argument",
            tonic::Code::DeadlineExceeded => "rpc_deadline_exceeded",
            tonic::Code::NotFound => "rpc_not_found",
            tonic::Code::AlreadyExists => "rpc_already_exists",
            tonic::Code::PermissionDenied => "rpc_permission_denied",
            tonic::Code::ResourceExhausted => "rpc_resource_exhausted",
            tonic::Code::FailedPrecondition => "rpc_failed_precondition",
            tonic::Code::Aborted => "rpc_aborted",
            tonic::Code::OutOfRange => "rpc_out_of_range",
            tonic::Code::Unimplemented => "rpc_unimplemented",
            tonic::Code::Internal => "rpc_internal",
            tonic::Code::Unavailable => "rpc_unavailable",
            tonic::Code::DataLoss => "rpc_data_loss",
            tonic::Code::Unauthenticated => "rpc_unauthenticated",
        }
    } else if let Some(error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
    {
        match error.kind() {
            std::io::ErrorKind::NotFound => "io_not_found",
            std::io::ErrorKind::PermissionDenied => "io_permission_denied",
            std::io::ErrorKind::AlreadyExists => "io_already_exists",
            std::io::ErrorKind::InvalidInput => "io_invalid_input",
            std::io::ErrorKind::InvalidData => "io_invalid_data",
            std::io::ErrorKind::TimedOut => "io_timed_out",
            std::io::ErrorKind::Interrupted => "io_interrupted",
            std::io::ErrorKind::WouldBlock => "io_would_block",
            std::io::ErrorKind::UnexpectedEof => "io_unexpected_eof",
            std::io::ErrorKind::WriteZero => "io_write_zero",
            std::io::ErrorKind::BrokenPipe => "io_broken_pipe",
            std::io::ErrorKind::NotConnected => "io_not_connected",
            std::io::ErrorKind::ConnectionRefused => "io_connection_refused",
            std::io::ErrorKind::ConnectionReset => "io_connection_reset",
            std::io::ErrorKind::ConnectionAborted => "io_connection_aborted",
            _ => "io_other",
        }
    } else {
        "local_unclassified"
    };
    // Diagnostic I/O failure must not replace the original publication failure.
    let _ = std::io::Write::write_fmt(
        &mut std::io::stderr(),
        format_args!("native_publication_failure phase={phase} category={category}\n"),
    );
}

impl RemoteRepositoryControl {
    pub(crate) async fn physical_client(&self) -> Result<NativeClient> {
        let client = self.inner.connector.get().await?;
        Ok(match io::scope() {
            Some(scope) => client
                .with_operation_deadline(scope.deadline)
                .with_rpc_lifetime_guard(scope),
            None => client,
        })
    }
    /// Obtain the already configured SDK client for independently admitted host
    /// work. No control semaphore is retained by this client: callers may use
    /// the no-ref transfer helper while the client is alive. The original
    /// deadline bounds waiting; detached connector work retains its owner and
    /// participates in provider shutdown until it physically completes.
    pub async fn client_for_owned_work(
        &self,
        owner: Arc<dyn std::any::Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<NativeClient> {
        let inner = self.inner.clone();
        let work = inner.metadata_work.enter();
        let job = io::owned(async move {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "native host work deadline exceeded"
            );
            let client = inner.connector.get().await?;
            ensure!(
                tokio::time::Instant::now() < deadline,
                "native host work deadline exceeded"
            );
            Ok(client
                .with_operation_deadline(deadline)
                .with_rpc_lifetime_guard(Arc::new((owner, work, inner))))
        });
        tokio::time::timeout_at(deadline, job)
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("native host work deadline exceeded"))?
    }
    /// Admit immutable cloud roots under an independently owned host budget and
    /// one original deadline. This creates no native reference or retention
    /// hold. The inherited scope reaches cloud SDK and ledger blocking workers.
    pub async fn admit_retained_version_until(
        &self,
        selector: tinytree_client::RetainedVersionSelector,
        selected: tinytree_client::CloudRawDigest,
        owner: Arc<dyn std::any::Any + Send + Sync>,
        original: tokio::time::Instant,
    ) -> Result<crate::AdmittedCloudVersion> {
        ensure!(
            io::scope().is_none(),
            "cloud host admission requires an independent scope"
        );
        // run_owned acquires the sole control lease once. The cloud admission
        // method inherits this scope in its owned job; it never reacquires it.
        self.run_owned(
            original,
            owner.clone(),
            self.admit_retained_version(selector, selected, owner),
        )
        .await
    }
    /// Drain independently accepted host/connector work after stopping producers.
    pub async fn wait_for_owned_work(&self) {
        self.drain_control().await;
        // Never hold one lane while waiting for another: a source callback may
        // acquire foreground control while its transfer owner remains active.
        if let Ok(permit) = self.inner.background_transfer.clone().acquire_owned().await {
            drop(permit);
        }
        self.inner.metadata_work.wait_for_idle().await;
    }
    pub fn client_id(&self) -> [u8; 16] {
        self.inner.ledger.client
    }
    pub(crate) async fn refresh_scope_hold(&self, head: m::CommitId) -> Result<()> {
        let operation = async {
            let client = self.physical_client().await?;
            let root = self.hold(&client, head).await?;
            self.current_hold(&client, root).await?;
            Ok(())
        };
        if io::in_control_for(&self.inner.control) {
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
            let client = self.physical_client().await?;
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
        if io::in_control_for(&self.inner.control) {
            io::check_deadline()?;
            operation.await
        } else {
            self.run_owned(deadline, Arc::new(()), operation).await
        }
    }
    pub(crate) async fn run_owned<T>(
        &self,
        deadline: tokio::time::Instant,
        owner: Arc<dyn std::any::Any + Send + Sync>,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let deadline = io::bounded_deadline(deadline);
        let parent = io::scope();
        let deadline = parent
            .as_ref()
            .map_or(deadline, |p| deadline.min(p.deadline));
        if let Some(parent) = parent {
            io::check_deadline()?;
            if parent.is_control_for(&self.inner.control) {
                return io::control_scope(
                    io::ControlScope::with_owner(parent, owner, deadline),
                    operation,
                )
                .await;
            }
            ensure!(
                !parent.has_control(),
                tonic::Status::failed_precondition("foreign native control admission")
            );
            // Background scope ownership is retained by the new foreground
            // scope, but it must acquire this adapter's real control lease.
        }
        // Only admission is cancellable here. Once DirectObjectIo owns a job,
        // await its physical completion; the broker's owned request task keeps
        // this future alive even when the incoming RPC times out/disconnects.
        let permit = tokio::time::timeout_at(deadline, self.inner.control.clone().acquire_owned())
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("native control admission deadline exceeded")
            })??;
        let scope = Arc::new(io::ControlScope::new(
            permit,
            self.inner.control.clone(),
            deadline,
            owner,
        ));
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
        let client = self.physical_client().await?;
        let mut root = self.hold(&client, head).await?;
        root.pinned = true;
        let ledger = self.inner.ledger.clone();
        let pinned = root.clone();
        io::blocking(move || ledger.pin(pinned)).await?;
        let provider = RemoteObjectIo {
            inner: self.inner.clone(),
        };
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head.0),
        };
        provider.read_metadata(key).await.map_err(backend)?;
        let mut root = self.current_hold(&client, root).await?;
        root.ready = true;
        // Save the confirmed pin independently of complete local closure state.
        let ledger = self.inner.ledger.clone();
        let pinned = root.clone();
        io::blocking(move || ledger.pin(pinned)).await?;
        Ok(AdmittedHead {
            head,
            hold_name: root.hold_name,
            hold_generation: root
                .generation
                .context("native hold proof missing generation")?,
            metadata_objects: 1,
            public_reference: None,
            _retained: (),
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
            pinned: false,
            ready: false,
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
        let mut phase = "validate";
        let result = self
            .publish_inner_phase(request, deadline, &mut phase)
            .await;
        if let Err(error) = &result {
            publication_failure(phase, error);
        }
        result
    }
    async fn publish_inner_phase(
        &self,
        request: PublishRequest,
        deadline: tokio::time::Instant,
        phase: &mut &'static str,
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
        *phase = "save_pending";
        let ledger = self.inner.ledger.clone();
        let saved = pending.clone();
        io::blocking(move || ledger.save_pending(&saved)).await?;
        *phase = "connector";
        let client = self.physical_client().await?;
        *phase = "hydrate_closure";
        let closure = self.hydrate_closure(request.head).await?;
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
        *phase = "find_missing";
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
        *phase = "metadata_upload";
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
        *phase = "file_upload";
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
        *phase = "hold";
        let root = self.hold(&client, request.head).await?;
        let root = self.current_hold(&client, root).await?;
        *phase = "ledger_completed";
        // Metadata is already durable locally; this proof also makes the remote
        // complete closure live before the public generation CAS is attempted.
        let ledger = self.inner.ledger.clone();
        let members = closure.descriptors();
        let files = closure.files;
        io::blocking(move || ledger.completed(root, &members, &files)).await?;
        *phase = "publish_cas";
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
        *phase = "receipt_persist";
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
    pub(crate) async fn upload_snapshot(
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
    /// Explicit compatibility path for consumers requiring a complete copy catalog.
    /// This may fetch historical trees; ordinary admission and reads do not.
    pub(crate) async fn complete_metadata(&self) -> Result<()> {
        let operation = async {
            let ledger = self.inner.ledger.clone();
            let roots = io::blocking(move || ledger.roots()).await?;
            for root in roots
                .into_iter()
                .filter(|root| root.retained() && !root.complete)
            {
                let head = m::CommitId(unhex(&root.head)?);
                let client = self.physical_client().await?;
                let root = self.hold(&client, head).await?;
                let closure = self.hydrate_closure(head).await?;
                let root = self.current_hold(&client, root).await?;
                let members = closure.descriptors();
                let files = closure.files;
                let ledger = self.inner.ledger.clone();
                io::blocking(move || ledger.completed(root, &members, &files)).await?;
            }
            Ok(())
        };
        if io::in_control_for(&self.inner.control) {
            operation.await
        } else {
            self.run(self.inner.deadline(), operation).await
        }
    }
    async fn hydrate_closure(&self, head: m::CommitId) -> Result<Closure> {
        let _metadata = self.inner.metadata.clone().lock_owned().await;
        let closure = Closure::collect(&self.inner, head, true).await?;
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
                let descriptor = self
                    .inner
                    .local
                    .write_metadata_with_owner(
                        node.object.clone(),
                        io::scope()
                            .map(|v| v as Arc<dyn std::any::Any + Send + Sync>)
                            .unwrap_or_else(|| Arc::new(())),
                    )
                    .await
                    .map_err(backend)?;
                ensure!(
                    descriptor == node.descriptor,
                    "native metadata installation changed identity"
                );
            }
        }
        Ok(closure)
    }
    /// Rebuild hints from already verified resident trees, never hydrate history.
    pub async fn rebuild_file_index(&self) -> Result<usize> {
        let deadline = self.inner.deadline();
        self.run(deadline, async {
            let _metadata = self.inner.metadata.clone().lock_owned().await;
            let ledger = self.inner.ledger.clone();
            let authorities = io::blocking(move || ledger.retained_authorities()).await?;
            let mut claims =
                std::collections::HashMap::<m::ObjectId, crate::ledger::FileClaim>::new();
            for authority in authorities {
                let ledger = self.inner.ledger.clone();
                let head = authority.key();
                let members = io::blocking(move || ledger.verified_members(&head)).await?;
                for descriptor in members
                    .into_iter()
                    .filter(|descriptor| descriptor.key.kind == m::Kind::Tree)
                {
                    let verified = self
                        .inner
                        .local
                        .read_metadata(descriptor.key)
                        .await
                        .map_err(backend)?;
                    ensure!(
                        verified.descriptor() == descriptor,
                        "native verified tree descriptor changed"
                    );
                    let m::Object::Tree(tree) = verified.into_object() else {
                        anyhow::bail!("native file provenance is not a tree")
                    };
                    for entry in tree.entries {
                        if let m::TreeValue::File { id, size_bytes, .. } = entry.value {
                            let id = m::ObjectId(id.0);
                            if let Some(existing) = claims.get(&id) {
                                ensure!(
                                    existing.size == size_bytes,
                                    "conflicting admitted native file lengths"
                                );
                            } else {
                                claims.insert(
                                    id,
                                    crate::ledger::FileClaim {
                                        size: size_bytes,
                                        root: authority.key(),
                                        tree: hex(&descriptor.key.id.0),
                                    },
                                );
                            }
                            ensure!(
                                claims.len() <= self.inner.config.max_file_index_entries,
                                "native derived file index count exceeded"
                            );
                        }
                    }
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
                if !root.retained()
                    || !(root.ready || root.complete)
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
