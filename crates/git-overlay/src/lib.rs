//! Persistent mutable workspace state, independent of a FUSE mount.
pub use git_workspace::{backend, filesystem, storage};
pub mod overlay;
pub mod worktree;
