//! Checks run operator-trusted source code; an immutable mount is not a sandbox.
//! Receipts describe one physical attempt, not a client-supplied verdict.
mod bep;
mod config;
mod process;
mod runner;

pub use config::{Binary, Config, Mode, Profile};
pub use runner::{RunningCheck, recover, run, start};

pub use tinytree_forge_core::check::{
    Evidence, MAX_RECEIPT_BYTES, MAX_REQUEST_BYTES, Outcome, Receipt, Request,
};
pub(crate) use tinytree_forge_core::check::{hex_id, name};

#[cfg(test)]
mod tests;
