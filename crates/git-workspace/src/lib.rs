//! Git objects, immutable workspace trees, and cached metadata.
pub mod backend;
pub mod cas_digest;
pub mod filesystem;
mod github_sizes;
pub mod prefetch;
pub mod storage;
