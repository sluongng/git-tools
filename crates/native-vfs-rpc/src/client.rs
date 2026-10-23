use crate::{
    Limits, LocalEndpoint, proto as p,
    wire::{self, Binding, Input, Output, Reply, Unary},
};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use jj_lib::{
    object_id::ObjectId,
    op_store::OperationId,
    repo_path::RepoPathBuf,
    working_copy::{CheckoutStats, SnapshotOptions, SnapshotStats, UntrackedReason},
};
use jj_vfs::native::{
    NativeProjectionController, NativeProjectionMutation, NativeTree, ProjectionBinding,
    ProjectionConnector, ProjectionFailure, ProjectionOutcome, ProjectionReceipt, ProjectionState,
};
use native_vfs_controller::{PolicyInventory, evaluate_inventory, options_fingerprint};
use std::{
    future::Future,
    sync::{Arc, OnceLock},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Mutex, mpsc},
    time::Instant,
};
use tonic::{Request, Status, codec::CompressionEncoding, transport::Channel};
fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<std::io::Result<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(8)
                .enable_all()
                .thread_name("projection-rpc-io")
                .build()
        })
        .as_ref()
        .map_err(|_| anyhow::anyhow!("projection transport runtime unavailable"))
}
async fn run<T: Send + 'static>(
    future: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    struct Task<T>(tokio::task::JoinHandle<Result<T>>);
    impl<T> Drop for Task<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let mut task = Task(runtime()?.spawn(future));
    (&mut task.0)
        .await
        .context("projection transport task failed")?
}
#[derive(Clone)]
pub struct ClientConnector {
    endpoint: LocalEndpoint,
    mount: String,
    limits: Limits,
}
impl ClientConnector {
    pub fn new(endpoint: LocalEndpoint, mount: impl Into<String>, limits: Limits) -> Result<Self> {
        endpoint.validate()?;
        limits.validate()?;
        let mount = mount.into();
        wire::mount(&mount)?;
        Ok(Self {
            endpoint,
            mount,
            limits,
        })
    }
}
impl ProjectionConnector for ClientConnector {
    fn connect(&self, binding: &ProjectionBinding) -> Result<Arc<dyn NativeProjectionController>> {
        let config = self.clone();
        let expected = Binding::from(binding);
        let (channel, registration) = futures::executor::block_on(run(async move {
            let channel = native_broker::connect_local_channel(
                config.endpoint.clone(),
                &config.limits.transport,
            )
            .await?;
            let reply = unary(
                channel.clone(),
                Unary::Inspect {
                    mount: config.mount.clone(),
                    expected: None,
                },
                config.limits.clone(),
            )
            .await?;
            let Reply::Inspected {
                binding,
                incarnation,
                state,
            } = reply
            else {
                bail!("projection inspection response mismatch")
            };
            ensure!(
                binding == expected,
                "configured projection root/admin/repository binding mismatch"
            );
            wire::state(&state, &binding.repository)?;
            Ok((
                channel,
                wire::Registration {
                    binding,
                    incarnation,
                },
            ))
        }))?;
        Ok(Arc::new(RemoteController {
            channel,
            mount: self.mount.clone(),
            limits: self.limits.clone(),
            binding: binding.clone(),
            registration,
        }))
    }
}
struct RemoteController {
    channel: Channel,
    mount: String,
    limits: Limits,
    binding: ProjectionBinding,
    registration: wire::Registration,
}
fn client(channel: Channel, limits: &Limits) -> p::projection_client::ProjectionClient<Channel> {
    p::projection_client::ProjectionClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(limits.max_frame_bytes + 4096)
        .max_encoding_message_size(limits.max_frame_bytes + 4096)
}
async fn unary(channel: Channel, input: Unary, limits: Limits) -> Result<Reply> {
    let mut client = client(channel, &limits);
    let mut request = Request::new(wire::encode(&input, &limits)?);
    request.set_timeout(limits.operation_timeout);
    let response = tokio::time::timeout(limits.operation_timeout, async {
        match &input {
            Unary::Inspect { .. } => client.inspect(request).await,
            Unary::Receipt { .. } => client.receipt(request).await,
            Unary::Acknowledge { .. } => client.acknowledge(request).await,
        }
    })
    .await
    .context("projection operation timeout")??;
    Ok(wire::decode(response.into_inner(), &limits)?)
}
#[async_trait]
impl NativeProjectionController for RemoteController {
    fn inspect(&self) -> Result<ProjectionState> {
        let channel = self.channel.clone();
        let mount = self.mount.clone();
        let limits = self.limits.clone();
        let expected = self.registration.clone();
        futures::executor::block_on(run(async move {
            let Reply::Inspected {
                binding,
                incarnation,
                state,
            } = unary(
                channel,
                Unary::Inspect {
                    mount,
                    expected: Some(expected.clone()),
                },
                limits,
            )
            .await?
            else {
                bail!("projection inspection response mismatch")
            };
            ensure!(
                binding == expected.binding && incarnation == expected.incarnation,
                "projection binding changed"
            );
            wire::state(&state, &binding.repository)?;
            Ok(state)
        }))
    }
    async fn begin(&self, expected: &ProjectionState) -> Result<Box<dyn NativeProjectionMutation>> {
        let started = Instant::now();
        let begin_deadline = started + self.limits.operation_timeout;
        let session_deadline = started + self.limits.session_lifetime;
        wire::state(expected, &self.binding.repository)?;
        let channel = self.channel.clone();
        let mount = self.mount.clone();
        let limits = self.limits.clone();
        let expected = expected.clone();
        let current = expected.clone();
        let registration = self.registration.clone();
        // One absolute begin budget includes bridge queueing, channel reconnect,
        // response headers, and the Began frame; headers never renew it.
        let session = run(async move {
            Session::open(
                channel,
                mount,
                registration,
                expected,
                limits,
                begin_deadline,
                session_deadline,
            )
            .await
        })
        .await?;
        Ok(Box::new(RemoteMutation {
            session,
            sequence: 1,
            repository: self.binding.repository.clone(),
            current: Some(current),
        }))
    }
    async fn receipt(&self, request_id: &str) -> Result<ProjectionOutcome> {
        wire::request_id(request_id)?;
        let input = Unary::Receipt {
            mount: self.mount.clone(),
            registration: self.registration.clone(),
            request_id: request_id.into(),
        };
        let channel = self.channel.clone();
        let limits = self.limits.clone();
        let id = request_id.to_owned();
        let repository = self.binding.repository.clone();
        run(async move {
            Ok(match unary(channel, input, limits).await? {
                Reply::Applied { receipt } => {
                    wire::receipt(&receipt, &repository)?;
                    ensure!(
                        receipt.request_id == id,
                        "projection receipt intent mismatch"
                    );
                    ProjectionOutcome::Applied(receipt)
                }
                Reply::NotApplied => ProjectionOutcome::NotApplied,
                Reply::Unknown => ProjectionOutcome::Unknown,
                _ => bail!("projection receipt response mismatch"),
            })
        })
        .await
    }
    async fn acknowledge(
        &self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        wire::state(state, &self.binding.repository)?;
        if let Some(receipt) = receipt {
            wire::receipt(receipt, &self.binding.repository)?;
        }
        let input = Unary::Acknowledge {
            mount: self.mount.clone(),
            registration: self.registration.clone(),
            state: state.clone(),
            operation: operation.as_bytes().into(),
            receipt: receipt.cloned(),
        };
        let channel = self.channel.clone();
        let limits = self.limits.clone();
        run(async move {
            ensure!(
                matches!(unary(channel, input, limits).await?, Reply::Acknowledged),
                "projection acknowledgement response mismatch"
            );
            Ok(())
        })
        .await
    }
}
struct Session {
    close: Arc<SessionClose>,
    output: Arc<Mutex<mpsc::Receiver<Result<p::Envelope, Status>>>>,
    limits: Limits,
    deadline: Instant,
    session_deadline: Instant,
    worker: tokio::task::JoinHandle<()>,
}
struct SessionClose {
    input: std::sync::Mutex<Option<mpsc::Sender<p::Envelope>>>,
    abort: tokio::task::AbortHandle,
}
impl SessionClose {
    fn close(&self) {
        if let Ok(mut input) = self.input.lock() {
            input.take();
        }
        self.abort.abort();
    }
}
struct ActionGuard {
    close: Arc<SessionClose>,
    success: bool,
}
impl Drop for ActionGuard {
    fn drop(&mut self) {
        if !self.success {
            self.close.close();
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.close.close();
        self.worker.abort();
    }
}
impl Session {
    async fn open(
        channel: Channel,
        mount: String,
        registration: wire::Registration,
        expected: ProjectionState,
        limits: Limits,
        begin_deadline: Instant,
        session_deadline: Instant,
    ) -> Result<Self> {
        ensure!(
            Instant::now() < begin_deadline,
            "projection begin deadline expired before admission"
        );
        let (send, receive) = mpsc::channel(8);
        let (output, events) = mpsc::channel(2);
        let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receive));
        request.set_timeout(session_deadline.saturating_duration_since(Instant::now()));
        // The first frame must be available before tonic waits for response
        // headers: server attestation/session admission requires that begin.
        send.send(wire::encode(
            &Input::Begin {
                mount,
                registration,
                expected: expected.clone(),
            },
            &limits,
        )?)
        .await?;
        ensure!(
            Instant::now() < begin_deadline,
            "projection begin deadline expired before headers"
        );
        let mut client = client(channel, &limits);
        let mut body = tokio::time::timeout_at(begin_deadline, client.session(request))
            .await
            .context("projection session begin timeout")??
            .into_inner();
        let worker_limits = limits.clone();
        let worker = tokio::spawn(async move {
            let deadline = session_deadline;
            loop {
                let next = tokio::time::timeout_at(
                    deadline.min(Instant::now() + worker_limits.session_idle_timeout),
                    body.message(),
                )
                .await;
                let event = match next {
                    Ok(Ok(Some(frame))) => Ok(frame),
                    Ok(Ok(None)) => break,
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(Status::deadline_exceeded(
                        "projection session response timeout",
                    )),
                };
                let terminal = event.is_err();
                if tokio::time::timeout_at(
                    deadline.min(Instant::now() + worker_limits.session_idle_timeout),
                    output.send(event),
                )
                .await
                .is_err()
                {
                    break;
                }
                if terminal {
                    break;
                }
            }
        });
        let mut session = Self {
            close: Arc::new(SessionClose {
                input: std::sync::Mutex::new(Some(send)),
                abort: worker.abort_handle(),
            }),
            output: Arc::new(Mutex::new(events)),
            deadline: begin_deadline,
            session_deadline,
            limits,
            worker,
        };
        let Output::Began { state } = session.next().await? else {
            bail!("projection begin response mismatch")
        };
        ensure!(state == expected, "projection admitted wrong baseline");
        Ok(session)
    }
    async fn send(&self, frame: Input) -> Result<()> {
        let encoded = wire::encode(&frame, &self.limits)?;
        let sender = self
            .close
            .input
            .lock()
            .map_err(|_| anyhow::anyhow!("projection session closed"))?
            .as_ref()
            .cloned()
            .context("projection session closed")?;
        let deadline = self.deadline;
        run(async move {
            ensure!(
                Instant::now() < deadline,
                "projection action expired before frame admission"
            );
            tokio::time::timeout_at(deadline, sender.send(encoded))
                .await
                .context("projection request admission timeout")?
                .map_err(|_| anyhow::anyhow!("projection session is closed"))
        })
        .await
    }
    async fn next_frame(&mut self) -> Result<p::Envelope> {
        let output = self.output.clone();
        let deadline = self.deadline;
        run(async move {
            ensure!(
                Instant::now() < deadline,
                "projection action deadline exceeded"
            );
            tokio::time::timeout_at(deadline, async {
                output
                    .lock()
                    .await
                    .recv()
                    .await
                    .context("projection session ended before result")?
                    .map_err(anyhow::Error::from)
            })
            .await
            .context("projection action deadline exceeded")?
        })
        .await
    }
    async fn next(&mut self) -> Result<Output> {
        let frame = self.next_frame().await?;
        Ok(wire::decode(frame, &self.limits)?)
    }
    fn action(&self) -> ActionGuard {
        ActionGuard {
            close: self.close.clone(),
            success: false,
        }
    }
    fn start_action(&mut self) {
        self.deadline = self
            .session_deadline
            .min(Instant::now() + self.limits.operation_timeout);
    }
}
struct RemoteMutation {
    session: Session,
    sequence: u64,
    repository: String,
    current: Option<ProjectionState>,
}
fn ambiguous(error: impl Into<anyhow::Error>) -> ProjectionFailure {
    ProjectionFailure::Ambiguous(error.into())
}
impl RemoteMutation {
    fn failure(&mut self, sequence: u64, rejected: bool, message: String) -> ProjectionFailure {
        if sequence != self.sequence {
            return ambiguous(anyhow::anyhow!("projection response sequence mismatch"));
        }
        self.sequence += 1;
        if rejected {
            ProjectionFailure::Rejected(anyhow::anyhow!(message))
        } else {
            ambiguous(anyhow::anyhow!(message))
        }
    }
    fn accepted(&mut self, sequence: u64, id: &str, receipt: &ProjectionReceipt) -> Result<()> {
        ensure!(
            sequence == self.sequence,
            "projection response sequence mismatch"
        );
        wire::receipt(receipt, &self.repository)?;
        ensure!(
            receipt.request_id == id,
            "projection response intent mismatch"
        );
        if let Some(current) = &self.current {
            ensure!(
                receipt.before == *current || receipt.after == *current,
                "projection receipt baseline mismatch"
            );
        }
        self.current = Some(receipt.after.clone());
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("projection sequence exhausted")?;
        Ok(())
    }
}
impl RemoteMutation {
    async fn snapshot_inner(
        &mut self,
        id: &str,
        options: &SnapshotOptions<'_>,
    ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
        self.session.start_action();
        wire::request_id(id).map_err(|e| ProjectionFailure::Rejected(e.into()))?;
        self.session
            .send(Input::Snapshot {
                sequence: self.sequence,
                request_id: id.into(),
                fingerprint: options_fingerprint(options),
            })
            .await
            .map_err(ambiguous)?;
        let mut inventory = PolicyInventory {
            candidates: Vec::new(),
            ignore_files: Vec::new(),
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-projection-inventory-v1\0");
        let mut bytes = 0usize;
        let mut ignores = false;
        loop {
            let frame = self.session.next_frame().await.map_err(ambiguous)?;
            bytes = bytes
                .checked_add(frame.payload.len())
                .ok_or_else(|| ambiguous(anyhow::anyhow!("projection inventory byte overflow")))?;
            if bytes > self.session.limits.max_policy_bytes + self.session.limits.max_frame_bytes {
                return Err(ambiguous(anyhow::anyhow!(
                    "projection inventory bytes exceeded"
                )));
            }
            let event: Output =
                wire::decode(frame.clone(), &self.session.limits).map_err(ambiguous)?;
            match event {
                Output::Candidate { entry } if !ignores => {
                    if inventory.candidates.len() >= self.session.limits.max_policy_paths
                        || inventory
                            .candidates
                            .last()
                            .is_some_and(|old| old.path >= entry.path)
                    {
                        return Err(ambiguous(anyhow::anyhow!(
                            "invalid projection candidate inventory"
                        )));
                    }
                    RepoPathBuf::from_internal_string(entry.path.clone()).map_err(ambiguous)?;
                    wire::hash_frame(&mut hash, &frame);
                    inventory.candidates.push(entry);
                }
                Output::Ignore { entry } => {
                    ignores = true;
                    if inventory.ignore_files.len() >= self.session.limits.max_policy_paths
                        || inventory
                            .ignore_files
                            .last()
                            .is_some_and(|old| old.directory >= entry.directory)
                    {
                        return Err(ambiguous(anyhow::anyhow!(
                            "invalid projection ignore inventory"
                        )));
                    }
                    RepoPathBuf::from_internal_string(entry.directory.clone())
                        .map_err(ambiguous)?;
                    wire::hash_frame(&mut hash, &frame);
                    inventory.ignore_files.push(entry);
                }
                Output::InventoryComplete {
                    candidates,
                    ignores,
                    digest,
                } => {
                    if candidates != inventory.candidates.len() as u64
                        || ignores != inventory.ignore_files.len() as u64
                        || digest != *hash.finalize().as_bytes()
                    {
                        return Err(ambiguous(anyhow::anyhow!(
                            "projection inventory completion mismatch"
                        )));
                    }
                    break;
                }
                Output::Snapshot { sequence, receipt } => {
                    self.accepted(sequence, id, &receipt).map_err(ambiguous)?;
                    return Ok((receipt, SnapshotStats::default()));
                }
                Output::Failure {
                    sequence,
                    rejected,
                    message,
                } => return Err(self.failure(sequence, rejected, message)),
                _ => {
                    return Err(ambiguous(anyhow::anyhow!(
                        "invalid projection inventory frame"
                    )));
                }
            }
        }
        let decisions = match evaluate_inventory(options, &inventory) {
            Ok(v) => v,
            Err(error) => {
                self.session
                    .send(Input::PolicyAbort)
                    .await
                    .map_err(ambiguous)?;
                return match self.session.next().await.map_err(ambiguous)? {
                    Output::Failure {
                        sequence,
                        rejected: true,
                        message: _,
                    } if sequence == self.sequence => {
                        self.sequence += 1;
                        Err(ProjectionFailure::Rejected(error))
                    }
                    Output::Failure {
                        sequence,
                        rejected,
                        message,
                    } => Err(self.failure(sequence, rejected, message)),
                    _ => Err(ambiguous(anyhow::anyhow!(
                        "projection policy abort was not confirmed"
                    ))),
                };
            }
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-projection-decisions-v1\0");
        let count = decisions.len() as u64;
        for (index, decision) in decisions.into_iter().enumerate() {
            let frame = Input::Decision {
                index: index as u64,
                decision,
            };
            wire::hash_frame(
                &mut hash,
                &wire::encode(&frame, &self.session.limits).map_err(ambiguous)?,
            );
            self.session.send(frame).await.map_err(ambiguous)?;
        }
        self.session
            .send(Input::DecisionsComplete {
                count,
                digest: *hash.finalize().as_bytes(),
            })
            .await
            .map_err(ambiguous)?;
        let mut stats = SnapshotStats::default();
        loop {
            match self.session.next().await.map_err(ambiguous)? {
                Output::Untracked {
                    path,
                    max_size,
                    size,
                } => {
                    if stats.untracked_paths.len() >= self.session.limits.max_policy_paths {
                        return Err(ambiguous(anyhow::anyhow!("projection stats exceeded")));
                    }
                    let path = RepoPathBuf::from_internal_string(path).map_err(ambiguous)?;
                    ensure_unique_stat(&mut stats, path, max_size, size).map_err(ambiguous)?;
                }
                Output::Snapshot { sequence, receipt } => {
                    self.accepted(sequence, id, &receipt).map_err(ambiguous)?;
                    return Ok((receipt, stats));
                }
                Output::Failure {
                    sequence,
                    rejected,
                    message,
                } => return Err(self.failure(sequence, rejected, message)),
                _ => {
                    return Err(ambiguous(anyhow::anyhow!(
                        "projection snapshot result mismatch"
                    )));
                }
            }
        }
    }
    async fn check_out_inner(
        &mut self,
        id: &str,
        tree: &NativeTree,
    ) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure> {
        self.session.start_action();
        wire::request_id(id).map_err(|e| ProjectionFailure::Rejected(e.into()))?;
        tree.validate().map_err(ProjectionFailure::Rejected)?;
        self.session
            .send(Input::Checkout {
                sequence: self.sequence,
                request_id: id.into(),
                tree: tree.clone(),
            })
            .await
            .map_err(ambiguous)?;
        match self.session.next().await.map_err(ambiguous)? {
            Output::Checkout {
                sequence,
                receipt,
                updated,
                added,
                removed,
                skipped,
            } => {
                self.accepted(sequence, id, &receipt).map_err(ambiguous)?;
                if receipt.after.tree != *tree {
                    return Err(ambiguous(anyhow::anyhow!(
                        "projection checkout result tree mismatch"
                    )));
                }
                Ok((
                    receipt,
                    CheckoutStats {
                        updated_files: updated,
                        added_files: added,
                        removed_files: removed,
                        skipped_files: skipped,
                    },
                ))
            }
            Output::Failure {
                sequence,
                rejected,
                message,
            } => Err(self.failure(sequence, rejected, message)),
            _ => Err(ambiguous(anyhow::anyhow!(
                "projection checkout result mismatch"
            ))),
        }
    }
    async fn reset_inner(
        &mut self,
        id: &str,
        tree: &NativeTree,
        recover: bool,
    ) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
        self.session.start_action();
        wire::request_id(id).map_err(|e| ProjectionFailure::Rejected(e.into()))?;
        tree.validate().map_err(ProjectionFailure::Rejected)?;
        self.session
            .send(Input::Reset {
                sequence: self.sequence,
                request_id: id.into(),
                tree: tree.clone(),
                recover,
            })
            .await
            .map_err(ambiguous)?;
        match self.session.next().await.map_err(ambiguous)? {
            Output::Reset { sequence, receipt } => {
                self.accepted(sequence, id, &receipt).map_err(ambiguous)?;
                if receipt.after.tree != *tree {
                    return Err(ambiguous(anyhow::anyhow!(
                        "projection reset result tree mismatch"
                    )));
                }
                Ok(receipt)
            }
            Output::Failure {
                sequence,
                rejected,
                message,
            } => Err(self.failure(sequence, rejected, message)),
            _ => Err(ambiguous(anyhow::anyhow!(
                "projection reset result mismatch"
            ))),
        }
    }
    async fn finish_inner(
        &mut self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        self.session.start_action();
        wire::state(state, &self.repository)?;
        if let Some(receipt) = receipt {
            wire::receipt(receipt, &self.repository)?;
        }
        self.session
            .send(Input::Finish {
                sequence: self.sequence,
                state: state.clone(),
                operation: operation.as_bytes().into(),
                receipt: receipt.cloned(),
            })
            .await?;
        let Output::Finished { sequence } = self.session.next().await? else {
            bail!("projection finish response mismatch")
        };
        ensure!(
            sequence == self.sequence,
            "projection finish sequence mismatch"
        );
        self.sequence += 1;
        Ok(())
    }
}
fn ensure_unique_stat(
    stats: &mut SnapshotStats,
    path: RepoPathBuf,
    max_size: Option<u64>,
    size: u64,
) -> Result<()> {
    let reason = if let Some(max_size) = max_size {
        UntrackedReason::FileTooLarge { size, max_size }
    } else {
        UntrackedReason::FileNotAutoTracked
    };
    ensure!(
        stats.untracked_paths.insert(path, reason).is_none(),
        "duplicate projection stat path"
    );
    Ok(())
}

#[async_trait]
impl NativeProjectionMutation for RemoteMutation {
    async fn snapshot(
        &mut self,
        id: &str,
        options: &SnapshotOptions<'_>,
    ) -> std::result::Result<(ProjectionReceipt, SnapshotStats), ProjectionFailure> {
        let mut guard = self.session.action();
        let result = self.snapshot_inner(id, options).await;
        guard.success = result.is_ok();
        result
    }
    async fn check_out(
        &mut self,
        id: &str,
        tree: &NativeTree,
    ) -> std::result::Result<(ProjectionReceipt, CheckoutStats), ProjectionFailure> {
        let mut guard = self.session.action();
        let result = self.check_out_inner(id, tree).await;
        guard.success = result.is_ok();
        result
    }
    async fn reset(
        &mut self,
        id: &str,
        tree: &NativeTree,
        recover: bool,
    ) -> std::result::Result<ProjectionReceipt, ProjectionFailure> {
        let mut guard = self.session.action();
        let result = self.reset_inner(id, tree, recover).await;
        guard.success = result.is_ok();
        result
    }
    async fn finish(
        &mut self,
        state: &ProjectionState,
        operation: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        let mut guard = self.session.action();
        let result = self.finish_inner(state, operation, receipt).await;
        guard.success = result.is_ok();
        result
    }
}
