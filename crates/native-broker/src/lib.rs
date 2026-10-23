//! Private daemon-owned native object I/O. Native references and remote storage
//! remain in Tinytree; per-workspace jj operation/index/working-copy state stays local.
mod client;
mod config;
mod service;
mod transport;
mod wire;
pub use client::BrokerObjectIo;
pub use config::Config;
pub use service::{BrokerService, RpcService};
#[cfg(unix)]
pub use transport::serve_unix;
pub mod proto {
    tonic::include_proto!("native.broker.v1");
}
