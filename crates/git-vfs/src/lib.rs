//! FUSE adapters and command-line support for Git-backed workspaces.
pub use git_overlay::{overlay, worktree};
pub use git_workspace::{backend, cas_digest, prefetch, storage};
pub mod blake3_xattr;
pub mod filesystem;
pub mod writable;
