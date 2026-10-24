//! Lazy writable Linux output projection. Only payload reads hydrate CAS data;
//! namespace operations and publication never fetch regular-file contents.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{
    FuseMount, FuseSourceCapture, MetadataDiagnostics, NativeSourceOptions, StartupFence, start,
    start_native_source, start_native_source_gated, start_native_source_gated_with_options,
    start_native_source_with_options, start_with_retirement_budget,
};
