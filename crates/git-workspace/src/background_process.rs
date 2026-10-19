//! Platform-native process-tree cancellation for background Git operations.
#[cfg(unix)]
#[path = "background_process_unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "background_process_windows.rs"]
mod platform;
pub use platform::BackgroundCancellation;
pub(super) use platform::NativeChild;
