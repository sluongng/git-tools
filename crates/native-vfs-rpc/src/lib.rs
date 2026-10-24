//! Private native projection control. The mounted controller remains the sole
//! namespace authority; this transport never falls back to materialized files.
mod bounded_json;
mod client;
mod command;
pub use command::{
    CommandClient, CommandHost, CommandJob, CommandPreparation, CommandStarted, command_lock_path,
};
mod service;
pub use client::ClientConnector;
mod wire;
pub use native_broker::LocalEndpoint;
pub use service::{ProjectionService, RpcService};
pub mod proto {
    tonic::include_proto!("native.vfs.v1");
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_inflight: usize,
    pub max_memory_bytes: usize,
    pub max_mounts: usize,
    pub max_policy_paths: usize,
    pub max_actions_per_session: u64,
    pub max_policy_bytes: usize,
    pub operation_timeout: std::time::Duration,
    pub session_lifetime: std::time::Duration,
    pub session_idle_timeout: std::time::Duration,
    pub transport: native_broker::Config,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 2 * 1024 * 1024,
            max_inflight: 16,
            max_memory_bytes: 512 * 1024 * 1024,
            max_mounts: 1024,
            max_policy_paths: 100_000,
            max_actions_per_session: 1024,
            max_policy_bytes: 16 * 1024 * 1024,
            operation_timeout: std::time::Duration::from_secs(120),
            session_lifetime: std::time::Duration::from_secs(3600),
            session_idle_timeout: std::time::Duration::from_secs(120),
            transport: native_broker::Config::default(),
        }
    }
}
impl Limits {
    pub(crate) fn memory_charge(&self) -> usize {
        // One queued decoded input plus the current decoder. Each input is
        // structurally capped before allocating: at most three native trees
        // (finish state + receipt), bounded strings, and fixed-size identifiers.
        // The small-frame bound includes Vec capacity growth for empty labels.
        let decoded = self.max_frame_bytes.saturating_mul(16).min(2 * 1024 * 1024);
        self.max_frame_bytes
            .saturating_mul(8)
            .saturating_add(decoded.saturating_mul(2))
            .saturating_add(self.max_policy_bytes.saturating_mul(8))
    }
    pub fn validate(&self) -> Result<(), tonic::Status> {
        let valid = self.max_frame_bytes >= 4096
            && self.max_frame_bytes <= 4 * 1024 * 1024
            && self.max_inflight > 0
            && self.max_inflight <= 1024
            && self.max_memory_bytes >= self.memory_charge()
            && self.max_memory_bytes <= 1024 * 1024 * 1024
            && self.max_mounts > 0
            && self.max_mounts <= 4096
            && self.max_actions_per_session > 0
            && self.max_actions_per_session <= 1_000_000
            && self.max_policy_paths > 0
            && self.max_policy_paths <= 1_000_000
            && self.max_policy_bytes > 0
            && self.max_policy_bytes <= 64 * 1024 * 1024
            && !self.operation_timeout.is_zero()
            && self.operation_timeout <= std::time::Duration::from_secs(3600)
            && self.session_lifetime >= self.operation_timeout
            && self.session_lifetime <= std::time::Duration::from_secs(86400)
            && !self.session_idle_timeout.is_zero()
            && self.session_idle_timeout <= self.session_lifetime;
        if !valid {
            return Err(tonic::Status::invalid_argument(
                "invalid native projection RPC limits",
            ));
        }
        self.transport.validate()
    }
}

#[cfg(all(test, feature = "protocol-test-support", unix))]
mod tests;
