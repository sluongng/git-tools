//! Lazy writable Linux output projection. Only payload reads hydrate CAS data;
//! namespace operations and publication never fetch regular-file contents.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{
    FuseMount, FuseSourceCapture, StartupFence, start, start_native_source,
    start_native_source_gated, start_with_retirement_budget,
};
