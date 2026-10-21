//! Lazy writable Linux output projection. Only payload reads hydrate CAS data;
//! namespace operations and publication never fetch regular-file contents.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{FuseMount, start, start_with_retirement_budget};
