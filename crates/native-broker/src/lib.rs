//! Private daemon-owned native object I/O. Native references and remote storage
//! remain in Tinytree; per-workspace jj operation/index/working-copy state stays local.
mod client;
mod config;
mod control;
mod control_client;
mod local;
mod query;
mod query_client;
mod query_service;
pub use control::{
    AdmittedCommit, ConfirmedPublication, ControlResources, NativeRepositoryControl,
    PublishRequest, ReferenceSnapshot,
};
pub use control_client::BrokerControlClient;
pub use query::*;
pub use query_client::BrokerQueryClient;
pub use query_service::QueryRpcService;
pub use tinytree_model::CommitId;
pub use tinytree_proto::query_v1 as query_protocol;
pub use tonic::{Code as ControlCode, Status as ControlStatus};
mod service;
mod transport;
mod wire;
pub use client::BrokerObjectIo;
pub use config::Config;
pub use service::{BrokerService, ControlRpcService, RpcService};
#[cfg(unix)]
pub use transport::serve_unix;
pub use transport::{LocalEndpoint, connect_local_channel, serve_local, serve_local_router};
pub mod proto {
    tonic::include_proto!("native.broker.v1");
}
