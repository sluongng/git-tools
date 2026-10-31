//! Checks run operator-trusted source code; an immutable mount is not a sandbox.
//! Receipts describe one physical attempt, not a client-supplied verdict.
mod bep;
mod config;
mod process;
mod runner;
mod prepared_source;
pub mod candidate_http;
pub mod source;
pub mod snapshot_linux;
mod remote;

pub use config::{Binary, Config, HttpsSnapshotConfig, Mode, Profile};
pub use runner::{inspect_recovery, RecoveryInspection, RunningCheck, recover, run, start, run_leased, start_leased};
pub use remote::{RemoteReport, run_remote, recover_remote};

pub use tinytree_forge_core::check::{
    Evidence, MAX_RECEIPT_BYTES, MAX_REQUEST_BYTES, Outcome, Receipt, Request,
};
pub(crate) use tinytree_forge_core::check::{hex_id, name};

#[cfg(test)]
mod tests;
