//! Persistent mutable workspace state, independent of a FUSE mount.
pub use workspace_core::{backend, filesystem, storage};
pub mod overlay;
pub mod worktree;
