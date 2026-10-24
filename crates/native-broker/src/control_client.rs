use crate::command::CommandChannel;
use crate::{
    AdmittedCommit, CommitId, Config, ConfirmedPublication, PublishRequest, ReferenceSnapshot,
    control, proto as p,
};
use std::{future::Future, path::Path, sync::Arc};
use tokio::{sync::Semaphore, time::Instant};
use tonic::{Request, Status, codec::CompressionEncoding};
#[derive(Clone)]
pub struct BrokerControlClient {
    inner: Arc<Inner>,
}
#[derive(Clone)]
struct Inner {
    channel: CommandChannel,
    store: String,
    config: Config,
    calls: Arc<Semaphore>,
    memory: Arc<Semaphore>,
}
impl std::fmt::Debug for BrokerControlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerControlClient")
            .finish_non_exhaustive()
    }
}
impl BrokerControlClient {
    /// Bind subsequent local RPCs to a live host command. Runtime only: shared
    /// channel/pools and configured provider identity remain unchanged.
    pub fn with_command_token(mut self, token: [u8; 32]) -> Self {
        let mut inner = (*self.inner).clone();
        inner.channel = inner.channel.with_command_token(token);
        self.inner = Arc::new(inner);
        self
    }

    /// Connect without requiring a caller Tokio runtime. No remote endpoint,
    /// credentials, object-store path or per-workspace generation is inferred.
    pub async fn connect(
        socket: impl AsRef<Path>,
        store: impl Into<String>,
        config: Config,
    ) -> Result<Self, Status> {
        Self::connect_local(
            crate::LocalEndpoint::UnixSocket(socket.as_ref().to_owned()),
            store,
            config,
        )
        .await
    }
    /// Connect through the configured native endpoint without a caller Tokio runtime.
    pub async fn connect_local(
        endpoint: crate::LocalEndpoint,
        store: impl Into<String>,
        config: Config,
    ) -> Result<Self, Status> {
        let store = store.into();
        crate::client::run_control(async move {
            config.validate()?;
            crate::wire::id(&store)?;
            let channel = crate::transport::connect_local_channel(endpoint, &config).await?;
            let channel = CommandChannel::new(channel);
            Ok(Self {
                inner: Arc::new(Inner {
                    channel,
                    store,
                    calls: Arc::new(Semaphore::new(config.max_inflight)),
                    memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                    config,
                }),
            })
        })
        .await
    }
    async fn execute<T, F, Fut>(&self, operation: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(
                p::repository_control_client::RepositoryControlClient<CommandChannel>,
                Instant,
                String,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<T, Status>> + Send + 'static,
    {
        let inner = self.inner.clone();
        crate::client::run_control(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                let _call = inner
                    .calls
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("broker client closed"))?;
                let _memory = inner
                    .memory
                    .clone()
                    .acquire_many_owned(16 * 1024)
                    .await
                    .map_err(|_| Status::unavailable("broker client closed"))?;
                let client = p::repository_control_client::RepositoryControlClient::new(
                    inner.channel.clone(),
                )
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
                .max_decoding_message_size(4096)
                .max_encoding_message_size(4096);
                operation(client, deadline, inner.store.clone())
                    .await
                    .map_err(control::status)
            })
            .await
        })
        .await
    }
    pub async fn observe_reference(&self, name: &str) -> Result<ReferenceSnapshot, Status> {
        control::reference_name(name)?;
        let name = name.to_owned();
        self.execute(move |mut client, deadline, store| async move {
            let value = client
                .observe_reference(request(
                    p::ObserveReferenceRequest {
                        store_id: store,
                        name: name.clone(),
                    },
                    deadline,
                )?)
                .await?
                .into_inner();
            if value.name != name || value.generation == 0 && value.head.is_some() {
                return Err(Status::data_loss("invalid broker reference observation"));
            }
            Ok(ReferenceSnapshot {
                name,
                generation: value.generation,
                head: value.head.map(admitted).transpose()?,
            })
        })
        .await
    }
    pub async fn admit_commit(&self, head: CommitId) -> Result<AdmittedCommit, Status> {
        control::commit(head)?;
        self.execute(move |mut client, deadline, store| async move {
            let value = admitted(
                client
                    .admit_commit(request(
                        p::AdmitCommitRequest {
                            store_id: store,
                            head: head.0.to_vec(),
                        },
                        deadline,
                    )?)
                    .await?
                    .into_inner(),
            )?;
            if value.head != head {
                return Err(Status::data_loss("broker admitted another commit"));
            }
            Ok(value)
        })
        .await
    }
    pub async fn publish(&self, input: PublishRequest) -> Result<ConfirmedPublication, Status> {
        control::reference_name(&input.name)?;
        control::key(&input.idempotency_key)?;
        control::commit(input.head)?;
        let expected = input
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| Status::invalid_argument("native reference generation exhausted"))?;
        self.execute(move |mut client, deadline, store| async move {
            let value = client
                .publish(request(
                    p::PublishRequest {
                        store_id: store,
                        name: input.name.clone(),
                        head: input.head.0.to_vec(),
                        expected_generation: Some(input.expected_generation),
                        idempotency_key: input.idempotency_key.clone(),
                    },
                    deadline,
                )?)
                .await?
                .into_inner();
            let value = publication(value)?;
            if value.idempotency_key != input.idempotency_key
                || value.name != input.name
                || value.head != input.head
                || value.generation != expected
            {
                return Err(Status::data_loss(
                    "broker publication does not match the original intent",
                ));
            }
            Ok(value)
        })
        .await
    }
    /// None is no confirmed receipt, not evidence that upstream did not commit.
    /// Compare a returned receipt against the workspace's durable original intent.
    pub async fn publication(&self, key: &str) -> Result<Option<ConfirmedPublication>, Status> {
        control::key(key)?;
        let key = key.to_owned();
        self.execute(move |mut client, deadline, store| async move {
            let value = client
                .publication(request(
                    p::PublicationRequest {
                        store_id: store,
                        idempotency_key: key.clone(),
                    },
                    deadline,
                )?)
                .await?
                .into_inner()
                .publication
                .map(publication)
                .transpose()?;
            if value.as_ref().is_some_and(|v| v.idempotency_key != key) {
                return Err(Status::data_loss("broker publication key mismatch"));
            }
            Ok(value)
        })
        .await
    }
}
fn request<T>(body: T, deadline: Instant) -> Result<Request<T>, Status> {
    let mut request = Request::new(body);
    request.set_timeout(
        deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| Status::deadline_exceeded("broker control deadline exceeded"))?,
    );
    Ok(request)
}
pub(crate) fn head(bytes: &[u8]) -> Result<CommitId, Status> {
    let head = CommitId(
        bytes
            .try_into()
            .map_err(|_| Status::data_loss("invalid native commit identity"))?,
    );
    control::commit(head).map_err(|_| Status::data_loss("invalid native commit identity"))?;
    Ok(head)
}
fn admitted(value: p::AdmittedCommit) -> Result<AdmittedCommit, Status> {
    Ok(AdmittedCommit {
        head: head(&value.head)?,
        metadata_objects: value.metadata_objects,
    })
}
fn publication(value: p::ConfirmedPublication) -> Result<ConfirmedPublication, Status> {
    control::reference_name(&value.name)
        .map_err(|_| Status::data_loss("invalid native publication reference"))?;
    control::key(&value.idempotency_key)
        .map_err(|_| Status::data_loss("invalid native publication key"))?;
    if value.generation == 0 {
        return Err(Status::data_loss("invalid native publication generation"));
    }
    Ok(ConfirmedPublication {
        idempotency_key: value.idempotency_key,
        name: value.name,
        generation: value.generation,
        head: head(&value.head)?,
    })
}
