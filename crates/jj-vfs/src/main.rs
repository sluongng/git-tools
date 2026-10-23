//! The legacy Git projection command remains Unix-specific. Native Tinytree
//! working-copy integration lives in the portable library, not an eager fallback.
#[cfg(unix)]
mod git_main;

#[cfg(unix)]
fn main() -> std::process::ExitCode {
    git_main::run()
}

#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("git-jj requires a supported Unix Git VFS mount");
    std::process::ExitCode::FAILURE
}
