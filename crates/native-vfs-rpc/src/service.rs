use crate::{
    Limits, proto as p,
    wire::{self, Input, Output, Reply, Unary},
};
use async_trait::async_trait;
use futures::Stream;
use jj_lib::working_copy::UntrackedReason;
use jj_vfs::native::{
    NativeProjectionController, NativeProjectionMutation, ProjectionBinding, ProjectionConnector,
    ProjectionFailure, ProjectionOutcome,
};
use native_vfs_controller::{Controller, PolicyDecision, PolicyInventory, SnapshotPolicy};
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use tonic::{
    Request, Response, Status, body::Body, codec::CompressionEncoding, server::NamedService,
};
use tower::{Layer, Service};
type OutputStream = Pin<Box<dyn Stream<Item = Result<p::Envelope, Status>> + Send>>;
#[derive(Clone)]
pub struct ProjectionService {
    inner: Arc<Inner>,
}
struct Inner {
    limits: Limits,
    mounts: RwLock<HashMap<String, Arc<Mount>>>,
    calls: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    closing: AtomicBool,
    command_host: RwLock<Option<Arc<dyn crate::CommandHost>>>,
}
struct Mount {
    controller: Arc<Controller>,
    binding: ProjectionBinding,
    incarnation: [u8; 16],
    leases: Arc<Semaphore>,
    closing: AtomicBool,
}
struct MountLease {
    _permit: OwnedSemaphorePermit,
    mount: Arc<Mount>,
}
struct Work {
    _memory: OwnedSemaphorePermit,
    _call: OwnedSemaphorePermit,
    deadline: Instant,
    command: std::sync::Mutex<Option<crate::CommandJob>>,
}
impl ProjectionService {
    pub fn new(limits: Limits) -> Result<Self, Status> {
        limits.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                calls: Arc::new(Semaphore::new(limits.max_inflight)),
                memory: Arc::new(Semaphore::new(limits.max_memory_bytes)),
                limits,
                mounts: RwLock::new(HashMap::new()),
                closing: AtomicBool::new(false),
                command_host: RwLock::new(None),
            }),
        })
    }
    /// Install once before serving. A configured host makes user mutation
    /// requests command-scoped; protocol fixtures without a host stay explicit.
    pub fn set_command_host(&self, host: Arc<dyn crate::CommandHost>) -> Result<(), Status> {
        let mut slot = self
            .inner
            .command_host
            .write()
            .map_err(|_| Status::internal("command host unavailable"))?;
        if slot.is_some() {
            return Err(Status::already_exists("command host already configured"));
        }
        *slot = Some(host);
        Ok(())
    }
    /// Host-only configuration. Incoming requests never choose paths or mounts.
    pub fn register(
        &self,
        id: impl Into<String>,
        controller: Arc<Controller>,
        binding: ProjectionBinding,
    ) -> Result<(), Status> {
        let id = id.into();
        wire::mount(&id)?;
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("projection service is draining"));
        }
        // Attest the exact configured root, external admin state and repository
        // through the real controller, not merely client-provided path strings.
        controller
            .connect(&binding)
            .map_err(|_| Status::failed_precondition("projection binding is not attested"))?;
        let state = controller
            .inspect()
            .map_err(|_| Status::failed_precondition("native projection is unavailable"))?;
        wire::state(&state, &binding.repository)?;
        let mut mounts = self
            .inner
            .mounts
            .write()
            .map_err(|_| Status::internal("projection registry unavailable"))?;
        if mounts.contains_key(&id) {
            return Err(Status::already_exists(
                "projection identity already configured",
            ));
        }
        if mounts.len() >= self.inner.limits.max_mounts {
            return Err(Status::resource_exhausted(
                "projection registry limit exceeded",
            ));
        }
        mounts.insert(
            id,
            Arc::new(Mount {
                controller,
                binding,
                incarnation: *uuid::Uuid::new_v4().as_bytes(),
                leases: Arc::new(Semaphore::new(self.inner.limits.max_inflight)),
                closing: AtomicBool::new(false),
            }),
        );
        Ok(())
    }
    /// Explicit host lifecycle only; waits for sessions and owned operations.
    pub async fn unregister(&self, id: &str) -> Result<(), Status> {
        let mount = self.lookup(id)?;
        mount.closing.store(true, Ordering::Release);
        let _all = mount
            .leases
            .clone()
            .acquire_many_owned(self.inner.limits.max_inflight as u32)
            .await
            .map_err(|_| Status::unavailable("projection admission closed"))?;
        let mut mounts = self
            .inner
            .mounts
            .write()
            .map_err(|_| Status::internal("projection registry unavailable"))?;
        if mounts
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, &mount))
        {
            mounts.remove(id);
        }
        Ok(())
    }
    pub async fn drain(&self) -> Result<(), Status> {
        self.inner.closing.store(true, Ordering::Release);
        let _all = self
            .inner
            .calls
            .clone()
            .acquire_many_owned(self.inner.limits.max_inflight as u32)
            .await
            .map_err(|_| Status::unavailable("projection admission closed"))?;
        Ok(())
    }
    fn lookup(&self, id: &str) -> Result<Arc<Mount>, Status> {
        wire::mount(id)?;
        self.inner
            .mounts
            .read()
            .map_err(|_| Status::internal("projection registry unavailable"))?
            .get(id)
            .cloned()
            .ok_or_else(|| Status::not_found("projection identity is not configured"))
    }
    async fn lease(&self, id: &str) -> Result<MountLease, Status> {
        let mount = self.lookup(id)?;
        let permit = mount
            .leases
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("projection admission closed"))?;
        if mount.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("projection is draining"));
        }
        Ok(MountLease {
            _permit: permit,
            mount,
        })
    }
    async fn lease_bound(
        &self,
        id: &str,
        registration: &wire::Registration,
    ) -> Result<MountLease, Status> {
        let lease = self.lease(id).await?;
        if lease.mount.incarnation != registration.incarnation
            || wire::Binding::from(&lease.mount.binding) != registration.binding
        {
            return Err(Status::failed_precondition(
                "projection registration binding or incarnation changed",
            ));
        }
        Ok(lease)
    }
    fn command_scope<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(Arc<Work>, Option<[u8; 32]>), Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("projection admission scope missing"))?;
        let all = request.metadata().get_all_bin("native-command-bin");
        let mut values = all.iter();
        let token = match values.next() {
            Some(value) => Some(
                value
                    .to_bytes()
                    .map_err(|_| Status::invalid_argument("invalid native command token"))?
                    .as_ref()
                    .try_into()
                    .map_err(|_| Status::invalid_argument("invalid native command token width"))?,
            ),
            None => None,
        };
        if values.next().is_some() {
            return Err(Status::invalid_argument("duplicate native command token"));
        }
        Ok((work, token))
    }
    fn bind_command(
        &self,
        scope: &(Arc<Work>, Option<[u8; 32]>),
        mount: &str,
    ) -> Result<Option<crate::CommandJob>, Status> {
        let host = self
            .inner
            .command_host
            .read()
            .map_err(|_| Status::internal("command host unavailable"))?
            .clone();
        match (host, scope.1) {
            (Some(host), Some(token)) => {
                let guard = host.resolve(mount, token)?;
                *scope
                    .0
                    .command
                    .lock()
                    .map_err(|_| Status::internal("command guard unavailable"))? = Some(guard);
                Ok(Some(scope.0.clone()))
            }
            (Some(_), None) => Err(Status::failed_precondition(
                "native projection requires an admitted command",
            )),
            (None, Some(_)) => Err(Status::failed_precondition(
                "native command host is not configured",
            )),
            (None, None) => Ok(None),
        }
    }
    pub fn rpc_service(&self) -> RpcService {
        let raw = p::projection_server::ProjectionServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(self.inner.limits.max_frame_bytes + 4096)
            .max_encoding_message_size(self.inner.limits.max_frame_bytes + 4096);
        let gate = Gate {
            service: raw,
            shared: self.inner.clone(),
        };
        let timed = grpc_transport::DeadlineLayer::new(
            self.inner.limits.session_lifetime.as_millis() as u64,
            self.inner.limits.session_idle_timeout.as_millis() as u64,
            self.inner.limits.max_frame_bytes + 4096,
        )
        .layer(gate);
        let mapped = tower::ServiceBuilder::new()
            .map_response(|response: http::Response<_>| response.map(Body::new))
            .service(timed);
        RpcService {
            inner: tower::util::BoxCloneSyncService::new(mapped),
        }
    }
}
#[derive(Clone)]
pub struct RpcService {
    inner: tower::util::BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Infallible>,
}
impl NamedService for RpcService {
    const NAME: &'static str = "native.vfs.v1.Projection";
}
impl Service<http::Request<Body>> for RpcService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        self.inner.call(request)
    }
}
#[derive(Clone)]
struct Gate<S> {
    service: S,
    shared: Arc<Inner>,
}
impl<S> Service<http::Request<Body>> for Gate<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let shared = self.shared.clone();
        let session = request.uri().path().ends_with("/Session");
        let budget = if session {
            shared.limits.session_lifetime
        } else {
            shared.limits.operation_timeout
        };
        let budget = grpc_budget(request.headers(), budget);
        let mut service = self.service.clone();
        Box::pin(async move {
            let budget = match budget {
                Ok(v) => v,
                Err(error) => return Ok(error.into_http()),
            };
            let deadline = Instant::now() + budget;
            let result = tokio::time::timeout_at(deadline, async {
                if shared.closing.load(Ordering::Acquire) {
                    return Err(Status::unavailable("projection service is draining"));
                }
                let call = shared
                    .calls
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("projection admission closed"))?;
                let charge = if session {
                    shared.limits.memory_charge()
                } else {
                    shared.limits.max_frame_bytes * 8
                };
                let memory = shared
                    .memory
                    .clone()
                    .acquire_many_owned(charge as u32)
                    .await
                    .map_err(|_| Status::unavailable("projection memory admission closed"))?;
                if shared.closing.load(Ordering::Acquire) {
                    return Err(Status::unavailable("projection service is draining"));
                }
                let work = Arc::new(Work {
                    _memory: memory,
                    _call: call,
                    deadline,
                    command: std::sync::Mutex::new(None),
                });
                let mut request = request.map(|body| {
                    Body::new(InboundBody::new(
                        body,
                        deadline,
                        shared.limits.session_idle_timeout,
                    ))
                });
                request.extensions_mut().insert(work.clone());
                // Owned task keeps admission charged through actual provider
                // completion, even if HTTP cancellation drops this waiter.
                tokio::spawn(async move {
                    let mut response = service.call(request).await.unwrap();
                    response
                        .extensions_mut()
                        .insert(grpc_transport::ResponseBudget::new(work));
                    response
                })
                .await
                .map_err(|_| Status::internal("projection request worker failed"))
            })
            .await;
            Ok(match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => error.into_http(),
                Err(_) => {
                    Status::deadline_exceeded("projection request deadline exceeded").into_http()
                }
            })
        })
    }
}
#[tonic::async_trait]
impl p::projection_server::Projection for ProjectionService {
    async fn command(
        &self,
        request: Request<p::Envelope>,
    ) -> Result<Response<p::Envelope>, Status> {
        use crate::command::{CommandReply, CommandRequest};
        let host = self
            .inner
            .command_host
            .read()
            .map_err(|_| Status::internal("command host unavailable"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("native command host is not configured"))?;
        let input: CommandRequest = wire::decode_command(request.into_inner(), &self.inner.limits)?;
        let result = match input {
            CommandRequest::Prepare {
                version: 1,
                mount,
                replay,
            } => {
                let _lease = self.lease(&mount).await?;
                CommandReply::Prepared(host.prepare(&mount, replay).await?)
            }
            CommandRequest::Begin {
                version: 1,
                mount,
                replay,
                preparation,
            } => {
                let _lease = self.lease(&mount).await?;
                CommandReply::Began(host.begin(&mount, replay, preparation).await?)
            }
            CommandRequest::SelectPrivateBackup {
                version: 1,
                mount,
                token,
            } => {
                let _lease = self.lease(&mount).await?;
                host.select_private_backup(&mount, token).await?;
                CommandReply::SelectedPrivateBackup
            }
            CommandRequest::FinishPrivateBackup {
                version: 1,
                mount,
                token,
            } => {
                let _lease = self.lease(&mount).await?;
                let receipt = host.finish_private_backup(&mount, token).await?;
                receipt
                    .validate()
                    .map_err(|_| Status::internal("invalid private backup receipt"))?;
                CommandReply::PrivateBackup(receipt)
            }
            CommandRequest::FinishDirty {
                version: 1,
                mount,
                token,
            } => {
                let _lease = self.lease(&mount).await?;
                host.finish_dirty(&mount, token).await?;
                CommandReply::DrainedDirty
            }
            _ => return Err(Status::invalid_argument("native command protocol version")),
        };
        Ok(Response::new(wire::encode(&result, &self.inner.limits)?))
    }
    async fn inspect(
        &self,
        request: Request<p::Envelope>,
    ) -> Result<Response<p::Envelope>, Status> {
        let command_scope = self.command_scope(&request)?;
        let Unary::Inspect { mount, expected } =
            wire::decode(request.into_inner(), &self.inner.limits)?
        else {
            return Err(Status::invalid_argument("expected projection inspection"));
        };
        self.bind_command(&command_scope, &mount)?;
        let lease = match expected {
            Some(expected) => self.lease_bound(&mount, &expected).await?,
            None => self.lease(&mount).await?,
        };
        if lease.mount.controller.requires_reconciliation() {
            lease
                .mount
                .controller
                .reconcile_pending()
                .await
                .map_err(|_| {
                    Status::unavailable("projection outcome reconciliation unavailable")
                })?;
        }
        let state = lease
            .mount
            .controller
            .inspect()
            .map_err(|_| Status::failed_precondition("native projection unavailable"))?;
        wire::state(&state, &lease.mount.binding.repository)?;
        Ok(Response::new(wire::encode(
            &Reply::Inspected {
                binding: (&lease.mount.binding).into(),
                incarnation: lease.mount.incarnation,
                state,
            },
            &self.inner.limits,
        )?))
    }
    async fn receipt(
        &self,
        request: Request<p::Envelope>,
    ) -> Result<Response<p::Envelope>, Status> {
        let command_scope = self.command_scope(&request)?;
        let Unary::Receipt {
            mount,
            registration,
            request_id,
        } = wire::decode(request.into_inner(), &self.inner.limits)?
        else {
            return Err(Status::invalid_argument(
                "expected projection receipt lookup",
            ));
        };
        wire::request_id(&request_id)?;
        self.bind_command(&command_scope, &mount)?;
        let lease = self.lease_bound(&mount, &registration).await?;
        let outcome = lease
            .mount
            .controller
            .receipt(&request_id)
            .await
            .map_err(|_| Status::unavailable("projection receipt unavailable"))?;
        let reply = match outcome {
            ProjectionOutcome::Applied(receipt) => {
                wire::receipt(&receipt, &lease.mount.binding.repository)?;
                Reply::Applied { receipt }
            }
            ProjectionOutcome::NotApplied => Reply::NotApplied,
            ProjectionOutcome::Unknown => Reply::Unknown,
        };
        Ok(Response::new(wire::encode(&reply, &self.inner.limits)?))
    }
    async fn acknowledge(
        &self,
        request: Request<p::Envelope>,
    ) -> Result<Response<p::Envelope>, Status> {
        let command_scope = self.command_scope(&request)?;
        let Unary::Acknowledge {
            mount,
            registration,
            state,
            operation,
            receipt,
        } = wire::decode(request.into_inner(), &self.inner.limits)?
        else {
            return Err(Status::invalid_argument(
                "expected projection acknowledgement",
            ));
        };
        self.bind_command(&command_scope, &mount)?;
        let lease = self.lease_bound(&mount, &registration).await?;
        wire::state(&state, &lease.mount.binding.repository)?;
        if let Some(receipt) = &receipt {
            wire::receipt(receipt, &lease.mount.binding.repository)?;
        }
        lease
            .mount
            .controller
            .acknowledge(&state, &wire::operation(operation)?, receipt.as_ref())
            .await
            .map_err(|_| Status::unavailable("projection acknowledgement requires recovery"))?;
        Ok(Response::new(wire::encode(
            &Reply::Acknowledged,
            &self.inner.limits,
        )?))
    }
    type SessionStream = OutputStream;
    async fn session(
        &self,
        request: Request<tonic::Streaming<p::Envelope>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let command_scope = self.command_scope(&request)?;
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("projection admission scope missing"))?;
        let mut body = request.into_inner();
        let cfg = self.inner.limits.clone();
        let first = tokio::time::timeout_at(
            work.deadline.min(Instant::now() + cfg.session_idle_timeout),
            body.message(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("projection session begin timeout"))??
        .ok_or_else(|| Status::invalid_argument("projection session has no begin"))?;
        let Input::Begin {
            mount,
            registration,
            expected,
        } = wire::decode(first, &cfg)?
        else {
            return Err(Status::invalid_argument(
                "projection session must begin with binding",
            ));
        };
        let physical_owner = self.bind_command(&command_scope, &mount)?;
        let lease = self.lease_bound(&mount, &registration).await?;
        wire::state(&expected, &lease.mount.binding.repository)?;
        let mut mutation = lease
            .mount
            .controller
            .begin_session(&expected)
            .await
            .map_err(|_| Status::failed_precondition("cannot begin native projection mutation"))?;
        if let Some(owner) = physical_owner {
            mutation.set_physical_owner(owner);
        }
        let (send, receive) = mpsc::channel(2);
        let (incoming, input) = mpsc::channel(1);
        let revoked = Arc::new(AtomicBool::new(false));
        let reader_revoked = revoked.clone();
        let reader_cfg = cfg.clone();
        let deadline = work.deadline;
        let reader = tokio::spawn(async move {
            loop {
                let next = tokio::time::timeout_at(
                    deadline.min(Instant::now() + reader_cfg.session_idle_timeout),
                    body.message(),
                )
                .await;
                let frame = match next {
                    Ok(Ok(Some(frame))) => frame,
                    _ => break,
                };
                let frame = match wire::decode::<Input>(frame, &reader_cfg) {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = tokio::time::timeout_at(
                            deadline.min(Instant::now() + reader_cfg.session_idle_timeout),
                            incoming.send(Err(error)),
                        )
                        .await;
                        break;
                    }
                };
                if tokio::time::timeout_at(
                    deadline.min(Instant::now() + reader_cfg.session_idle_timeout),
                    incoming.send(Ok(frame)),
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            reader_revoked.store(true, Ordering::Release);
        });
        let session_cfg = cfg.clone();
        tokio::spawn(async move {
            // Never abort an action future: this owner keeps mutation fencing,
            // mounted-provider leases and admission until real work completes.
            let _work = work;
            let _lease = lease;
            let mut mutation = mutation;
            let input = Arc::new(Mutex::new(input));
            let result = session_loop(
                &mut mutation,
                &expected,
                input,
                send.clone(),
                revoked.clone(),
                &session_cfg,
                deadline,
            )
            .await;
            revoked.store(true, Ordering::Release);
            reader.abort();
            // Keep mutation, mounted-provider lease and command/resource Work
            // until real source work has stopped, not merely its logical wait.
            // One existing admitted worker/session retries an unavailable
            // barrier; an unprovable stop stays owned for explicit recovery.
            while mutation.drain_physical().await.is_err() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if let Err(error) = result {
                let _ =
                    tokio::time::timeout(session_cfg.session_idle_timeout, send.send(Err(error)))
                        .await;
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receive),
        )))
    }
}
async fn send(
    send: &mpsc::Sender<Result<p::Envelope, Status>>,
    frame: Output,
    cfg: &Limits,
    deadline: Instant,
) -> Result<(), Status> {
    let frame = wire::encode(&frame, cfg)?;
    tokio::time::timeout_at(
        deadline.min(Instant::now() + cfg.session_idle_timeout),
        send.send(Ok(frame)),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("projection response consumer stalled"))?
    .map_err(|_| Status::cancelled("projection response consumer closed"))
}
async fn receive(
    input: &Arc<Mutex<mpsc::Receiver<Result<Input, Status>>>>,
    cfg: &Limits,
    deadline: Instant,
) -> Result<Input, Status> {
    tokio::time::timeout_at(
        deadline.min(Instant::now() + cfg.session_idle_timeout),
        input.lock().await.recv(),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("projection input stalled"))?
    .ok_or_else(|| Status::cancelled("projection session closed"))?
}
async fn session_loop(
    mutation: &mut native_vfs_controller::Mutation,
    expected: &jj_vfs::native::ProjectionState,
    input: Arc<Mutex<mpsc::Receiver<Result<Input, Status>>>>,
    output: mpsc::Sender<Result<p::Envelope, Status>>,
    revoked: Arc<AtomicBool>,
    cfg: &Limits,
    session_deadline: Instant,
) -> Result<(), Status> {
    send(
        &output,
        Output::Began {
            state: expected.clone(),
        },
        cfg,
        session_deadline,
    )
    .await?;
    let mut sequence = 1u64;
    loop {
        if sequence > cfg.max_actions_per_session {
            return Err(Status::resource_exhausted(
                "projection session action limit exceeded",
            ));
        }
        let frame = receive(&input, cfg, session_deadline).await?;
        if revoked.load(Ordering::Acquire) {
            return Err(Status::cancelled("projection session admission revoked"));
        }
        let deadline = session_deadline.min(Instant::now() + cfg.operation_timeout);
        // Physical futures drain under this immutable per-action scope. The
        // controller checks it before admitting each later logical stage.
        mutation.set_admission_scope(native_vfs_controller::AdmissionScope::new(
            deadline,
            revoked.clone(),
        ));
        let result = match frame {
            Input::Snapshot {
                sequence: actual,
                request_id,
                fingerprint,
            } if actual == sequence => {
                wire::request_id(&request_id)?;
                let policy = RemotePolicy {
                    fingerprint,
                    input: input.clone(),
                    output: output.clone(),
                    cfg: cfg.clone(),
                    deadline,
                };
                match mutation.snapshot_with_policy(&request_id, &policy).await {
                    Ok((receipt, stats)) => {
                        if !stats.invalid_utf8_paths.is_empty() {
                            return Err(Status::data_loss(
                                "projection returned unsupported invalid path stats",
                            ));
                        }
                        if stats.untracked_paths.len() > cfg.max_policy_paths {
                            return Err(Status::resource_exhausted(
                                "projection stats exceed limit",
                            ));
                        }
                        for (path, reason) in stats.untracked_paths {
                            let (size, max_size) = match reason {
                                UntrackedReason::FileNotAutoTracked => (0, None),
                                UntrackedReason::FileTooLarge { size, max_size } => {
                                    (size, Some(max_size))
                                }
                            };
                            send(
                                &output,
                                Output::Untracked {
                                    path: path.as_internal_file_string().into(),
                                    max_size,
                                    size,
                                },
                                cfg,
                                deadline,
                            )
                            .await?;
                        }
                        Ok(Output::Snapshot { sequence, receipt })
                    }
                    Err(error) => Err(error),
                }
            }
            Input::Checkout {
                sequence: actual,
                request_id,
                tree,
            } if actual == sequence => {
                wire::request_id(&request_id)?;
                tree.validate()
                    .map_err(|_| Status::invalid_argument("invalid checkout tree"))?;
                mutation
                    .check_out(&request_id, &tree)
                    .await
                    .map(|(receipt, stats)| Output::Checkout {
                        sequence,
                        receipt,
                        updated: stats.updated_files,
                        added: stats.added_files,
                        removed: stats.removed_files,
                        skipped: stats.skipped_files,
                    })
            }
            Input::Reset {
                sequence: actual,
                request_id,
                tree,
                recover,
            } if actual == sequence => {
                wire::request_id(&request_id)?;
                tree.validate()
                    .map_err(|_| Status::invalid_argument("invalid reset tree"))?;
                mutation
                    .reset(&request_id, &tree, recover)
                    .await
                    .map(|receipt| Output::Reset { sequence, receipt })
            }
            Input::Finish {
                sequence: actual,
                state,
                operation,
                receipt,
            } if actual == sequence => {
                wire::state(&state, &expected.repository)?;
                if let Some(receipt) = &receipt {
                    wire::receipt(receipt, &expected.repository)?;
                }
                mutation
                    .finish(&state, &wire::operation(operation)?, receipt.as_ref())
                    .await
                    .map_err(|_| {
                        Status::unavailable("projection finish requires acknowledgement recovery")
                    })?;
                send(&output, Output::Finished { sequence }, cfg, deadline).await?;
                return Ok(());
            }
            _ => return Err(Status::invalid_argument("projection action order mismatch")),
        };
        let frame = match result {
            Ok(frame) => frame,
            Err(error) => {
                let rejected = matches!(&error, ProjectionFailure::Rejected(_));
                Output::Failure {
                    sequence,
                    rejected,
                    message: error.to_string().chars().take(1024).collect(),
                }
            }
        };
        send(&output, frame, cfg, deadline).await?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("projection action sequence exhausted"))?;
    }
}
struct RemotePolicy {
    fingerprint: [u8; 32],
    input: Arc<Mutex<mpsc::Receiver<Result<Input, Status>>>>,
    output: mpsc::Sender<Result<p::Envelope, Status>>,
    cfg: Limits,
    deadline: Instant,
}
#[async_trait]
impl SnapshotPolicy for RemotePolicy {
    fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
    async fn evaluate(&self, inventory: &PolicyInventory) -> anyhow::Result<Vec<PolicyDecision>> {
        anyhow::ensure!(
            inventory.candidates.len() <= self.cfg.max_policy_paths,
            "projection policy path count exceeded"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-projection-inventory-v1\0");
        let mut bytes = 0usize;
        for frame in inventory
            .candidates
            .iter()
            .map(|entry| Output::Candidate {
                entry: entry.clone(),
            })
            .chain(inventory.ignore_files.iter().map(|entry| Output::Ignore {
                entry: entry.clone(),
            }))
        {
            let encoded = wire::encode(&frame, &self.cfg)?;
            bytes = bytes
                .checked_add(encoded.payload.len())
                .ok_or_else(|| anyhow::anyhow!("projection policy byte overflow"))?;
            anyhow::ensure!(
                bytes <= self.cfg.max_policy_bytes,
                "projection policy bytes exceeded"
            );
            wire::hash_frame(&mut hash, &encoded);
            tokio::time::timeout_at(self.deadline, self.output.send(Ok(encoded))).await??;
        }
        send(
            &self.output,
            Output::InventoryComplete {
                candidates: inventory.candidates.len() as u64,
                ignores: inventory.ignore_files.len() as u64,
                digest: *hash.finalize().as_bytes(),
            },
            &self.cfg,
            self.deadline,
        )
        .await?;
        let mut decisions = Vec::with_capacity(inventory.candidates.len());
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-projection-decisions-v1\0");
        for index in 0..inventory.candidates.len() {
            let frame = receive(&self.input, &self.cfg, self.deadline).await?;
            if matches!(frame, Input::PolicyAbort) {
                anyhow::bail!("client snapshot policy could not be evaluated");
            }
            let Input::Decision {
                index: actual,
                decision,
            } = frame
            else {
                anyhow::bail!("expected ordered projection policy decision")
            };
            anyhow::ensure!(
                actual == index as u64,
                "projection policy decision order mismatch"
            );
            wire::hash_frame(
                &mut hash,
                &wire::encode(
                    &Input::Decision {
                        index: actual,
                        decision: decision.clone(),
                    },
                    &self.cfg,
                )?,
            );
            decisions.push(decision);
        }
        let completion = receive(&self.input, &self.cfg, self.deadline).await?;
        if matches!(completion, Input::PolicyAbort) {
            anyhow::bail!("client snapshot policy could not be evaluated");
        }
        let Input::DecisionsComplete { count, digest } = completion else {
            anyhow::bail!("projection decisions incomplete")
        };
        anyhow::ensure!(
            count == decisions.len() as u64 && digest == *hash.finalize().as_bytes(),
            "projection decision completion mismatch"
        );
        Ok(decisions)
    }
}
fn grpc_budget(headers: &http::HeaderMap, maximum: Duration) -> Result<Duration, Status> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(maximum);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid projection timeout"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Status::invalid_argument("invalid projection timeout"));
    }
    let (number, unit) = value.split_at(value.len() - 1);
    if !number.bytes().all(|v| v.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid projection timeout"));
    }
    let number: u64 = number
        .parse()
        .map_err(|_| Status::invalid_argument("invalid projection timeout"))?;
    let value = match unit {
        "H" => Duration::from_secs(number.saturating_mul(3600)),
        "M" => Duration::from_secs(number.saturating_mul(60)),
        "S" => Duration::from_secs(number),
        "m" => Duration::from_millis(number),
        "u" => Duration::from_micros(number),
        "n" => Duration::from_nanos(number),
        _ => return Err(Status::invalid_argument("invalid projection timeout")),
    };
    Ok(value.min(maximum))
}
struct InboundBody {
    body: Body,
    timer: Pin<Box<tokio::time::Sleep>>,
    deadline: Instant,
    idle: Duration,
    done: bool,
}
impl InboundBody {
    fn new(body: Body, deadline: Instant, idle: Duration) -> Self {
        Self {
            body,
            timer: Box::pin(tokio::time::sleep_until(
                deadline.min(Instant::now() + idle),
            )),
            deadline,
            idle,
            done: false,
        }
    }
}
impl http_body::Body for InboundBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<bytes::Bytes>, Status>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this.timer.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "projection input body deadline exceeded",
            ))));
        }
        let result = Pin::new(&mut this.body).poll_frame(cx);
        if result.is_ready() {
            this.timer
                .as_mut()
                .reset(this.deadline.min(Instant::now() + this.idle));
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.done || self.body.is_end_stream()
    }
}
