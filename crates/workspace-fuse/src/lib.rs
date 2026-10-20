//! FUSE adapters and command-line support for Git-backed workspaces.
pub use workspace_core::{backend, cas_digest, prefetch, storage};
pub use workspace_overlay::{overlay, worktree};
pub mod blake3_xattr;
pub mod filesystem;
pub mod writable;

pub mod host;
