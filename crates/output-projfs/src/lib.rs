//! Windows ProjFS frontend for immutable CAS output generations.
//!
//! Local edits and deletion tombstones remain authoritative in NTFS. Publishing
//! a new remote generation never silently discards a dirty native file.
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{Metrics, ProjfsMount, start};

/// Validate representable Win32 output components. Unlike a Git workspace,
/// output projections do not reserve `.git` or short-name lookalikes.
pub fn validate_component(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty() && name != "." && name != "..",
        "invalid Windows name"
    );
    anyhow::ensure!(
        !name.ends_with(['.', ' ']),
        "Windows name ends in dot or space"
    );
    anyhow::ensure!(
        !name.chars().any(|c| c < ' ' || "<>:\"/\\|?*".contains(c)),
        "invalid Windows name"
    );
    anyhow::ensure!(
        name.encode_utf16().count() <= 255,
        "Windows component too long"
    );
    let stem = name
        .split('.')
        .next()
        .unwrap()
        .trim_end_matches(' ')
        .to_uppercase();
    anyhow::ensure!(
        !matches!(
            stem.as_str(),
            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
        ) && !(stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
            && !["COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"].contains(&stem.as_str()),
        "reserved device name"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn output_names_reject_native_aliases_not_git_control_paths() {
        for name in [
            "nul.txt", "CON .txt", "COM¹", "a:", "a.", "a ", "a/b", "", "..",
        ] {
            assert!(super::validate_component(name).is_err(), "{name}");
        }
        for name in [".git", "git~1", "BUILD.bazel", "résumé"] {
            super::validate_component(name).unwrap();
        }
    }
}
