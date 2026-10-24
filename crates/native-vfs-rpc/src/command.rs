//! Local command fences. Finishing drains dirty state only: this protocol does
//! not authorize collection, publish a Clean checkpoint or acknowledge backup.
use crate::{Limits, LocalEndpoint, client, wire};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
};
use tonic::{Request, Status};

pub type CommandJob = Arc<dyn Any + Send + Sync>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandPreparation {
    pub incarnation: [u8; 16],
    pub nonce: [u8; 32],
    pub expected_epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandStarted {
    pub token: [u8; 32],
    pub epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum CommandRequest {
    Prepare {
        version: u32,
        mount: String,
        replay: [u8; 32],
    },
    Begin {
        version: u32,
        mount: String,
        replay: [u8; 32],
        preparation: CommandPreparation,
    },
    FinishDirty {
        version: u32,
        mount: String,
        token: [u8; 32],
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum CommandReply {
    Prepared(CommandPreparation),
    Began(CommandStarted),
    DrainedDirty,
}
#[async_trait]
pub trait CommandHost: Send + Sync {
    async fn prepare(&self, mount: &str, replay: [u8; 32]) -> Result<CommandPreparation, Status>;
    async fn begin(
        &self,
        mount: &str,
        replay: [u8; 32],
        preparation: CommandPreparation,
    ) -> Result<CommandStarted, Status>;
    async fn finish_dirty(&self, mount: &str, token: [u8; 32]) -> Result<(), Status>;
    fn resolve(&self, mount: &str, token: [u8; 32]) -> Result<CommandJob, Status>;
}
/// This sibling namespace does not change the initializer's fresh-admin layout.
/// Callers pass only a privately registered admin path, never an RPC path field.
pub fn command_lock_path(admin: &Path, mount: &str) -> Result<PathBuf, Status> {
    wire::mount(mount)?;
    let parent = admin
        .parent()
        .ok_or_else(|| Status::invalid_argument("invalid command administration"))?;
    Ok(parent.join("command-leases").join(format!("{mount}.lock")))
}
#[derive(Clone)]
pub struct CommandClient {
    endpoint: LocalEndpoint,
    mount: String,
    limits: Limits,
}
impl CommandClient {
    pub fn new(
        endpoint: LocalEndpoint,
        mount: impl Into<String>,
        limits: Limits,
    ) -> anyhow::Result<Self> {
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
    async fn call(&self, input: CommandRequest) -> anyhow::Result<CommandReply> {
        let config = self.clone();
        let deadline = tokio::time::Instant::now() + config.limits.operation_timeout;
        client::run(async move {
            tokio::time::timeout_at(deadline, async move {
                let channel =
                    native_broker::connect_local_channel(config.endpoint, &config.limits.transport)
                        .await?;
                let mut rpc = client::client(channel, &config.limits);
                let mut request = Request::new(wire::encode(&input, &config.limits)?);
                request
                    .set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
                let response = rpc.command(request).await?;
                Ok(wire::decode_command(response.into_inner(), &config.limits)?)
            })
            .await
            .map_err(|_| anyhow::anyhow!("native command operation timed out"))?
        })
        .await
    }
    pub async fn prepare(&self, replay: [u8; 32]) -> anyhow::Result<CommandPreparation> {
        match self
            .call(CommandRequest::Prepare {
                version: 1,
                mount: self.mount.clone(),
                replay,
            })
            .await?
        {
            CommandReply::Prepared(value)
                if value.incarnation != [0; 16] && value.nonce != [0; 32] =>
            {
                Ok(value)
            }
            _ => anyhow::bail!("native command preparation response mismatch"),
        }
    }
    /// Reuse this exact preparation/replay for an active attempt's retries.
    /// Preparation must happen BEFORE acquiring the local writer kernel lock.
    pub async fn begin(
        &self,
        replay: [u8; 32],
        preparation: CommandPreparation,
    ) -> anyhow::Result<CommandStarted> {
        let epoch = preparation
            .expected_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("native command epoch exhausted"))?;
        match self
            .call(CommandRequest::Begin {
                version: 1,
                mount: self.mount.clone(),
                replay,
                preparation,
            })
            .await?
        {
            CommandReply::Began(value) if value.token != [0; 32] && value.epoch == epoch => {
                Ok(value)
            }
            _ => anyhow::bail!("native command begin response mismatch"),
        }
    }
    pub async fn finish_dirty(&self, token: [u8; 32]) -> anyhow::Result<()> {
        match self
            .call(CommandRequest::FinishDirty {
                version: 1,
                mount: self.mount.clone(),
                token,
            })
            .await?
        {
            CommandReply::DrainedDirty => Ok(()),
            _ => anyhow::bail!("native command drain response mismatch"),
        }
    }
}
