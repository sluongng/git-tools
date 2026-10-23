//! Jujutsu working-copy adapters for daemon-owned lazy projections.
//! Native Tinytree workspaces never select jj's materialized working copy.
#[cfg(unix)]
mod git;
#[cfg(unix)]
pub use git::*;
pub mod native;
