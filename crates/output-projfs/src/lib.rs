//! Windows ProjFS frontend for immutable CAS output generations.
//!
//! Local edits and deletion tombstones remain authoritative in NTFS. Publishing
//! a new remote generation never silently discards a dirty native file.
pub mod identity;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{
    CapturedSourceFile, CapturedSourceReader, Metrics, NativeCaptureError, NativeCaptureLimits,
    NativeSourceCapture, NativeSourceContent, NativeSourceEntry, ProjfsMount, SourceRootGuard,
    start,
};

/// Read-only binding check for a recognized persisted source projection.
/// This does not start, repair, adopt, or mutate a root. Callers needing lifetime
/// protection retain `SourceRootGuard` instead of discarding this check's guard.
pub fn validate_existing_binding(
    root: &std::path::Path,
    state: &std::path::Path,
    tree_identity: &str,
) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        let _guard = SourceRootGuard::open(root, state, tree_identity)?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = (root, state, tree_identity);
        anyhow::bail!("ProjFS source binding requires Windows")
    }
}

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
    use crate::identity::{Content, MAX_ORIGINS, Origin, validate_origins};
    use output_core::Artifact;
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

    #[test]
    fn immutable_locators_preserve_full_origin_and_reject_ambiguous_identity() {
        let origin = Origin {
            key: [7; 16],
            backend: "fixture-scope".into(),
            instance: "builds/main".into(),
        };
        for digest_function in [1, 9] {
            let artifact = Artifact {
                backend: origin.backend.clone(),
                instance: origin.instance.clone(),
                digest_function,
                hash: "ab".repeat(32),
                size: 8193,
            };
            let content = Content::remote(42, 3, &origin, &artifact).unwrap();
            let (provider, encoded) = content.encode().unwrap();
            let restored = Content::decode(&provider, &encoded).unwrap();
            assert_eq!(restored, content);
            assert_eq!(
                restored.artifact(std::slice::from_ref(&origin)).unwrap(),
                artifact
            );
            assert!(
                restored.artifact(&[]).is_err(),
                "unknown origin cannot select a default backend"
            );
            let mut wrong_instance = artifact.clone();
            wrong_instance.instance = "builds/other".into();
            assert!(Content::remote(42, 3, &origin, &wrong_instance).is_err());
            let mut reserved = encoded;
            reserved[127] = 1;
            assert!(Content::decode(&provider, &reserved).is_err());
            let mut unsupported = encoded;
            unsupported[40..44].copy_from_slice(&2i32.to_le_bytes());
            assert!(Content::decode(&provider, &unsupported).is_err());
            let mut oversized = encoded;
            oversized[44..52].copy_from_slice(&u64::MAX.to_le_bytes());
            assert!(Content::decode(&provider, &oversized).is_err());
            let mut unknown_provider = provider;
            unknown_provider[127] = 1;
            assert!(Content::decode(&unknown_provider, &encoded).is_err());
        }
        let legacy = Content::Legacy {
            node: 17,
            generation: 9,
        };
        let (provider, encoded) = legacy.encode().unwrap();
        assert_eq!(Content::decode(&provider, &encoded).unwrap(), legacy);
        assert!(legacy.artifact(std::slice::from_ref(&origin)).is_err());
        assert!(validate_origins(&[origin.clone(), origin.clone()]).is_err());
        let mut too_long = origin.clone();
        too_long.instance = "x".repeat(crate::identity::MAX_ORIGIN_TEXT_BYTES + 1);
        assert!(validate_origins(&[too_long]).is_err());
        let origins: Vec<_> = (0..=MAX_ORIGINS)
            .map(|index| {
                let mut key = [0; 16];
                key[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
                Origin {
                    key,
                    backend: "fixture".into(),
                    instance: format!("instance-{index}"),
                }
            })
            .collect();
        validate_origins(&origins[..MAX_ORIGINS]).unwrap();
        assert!(validate_origins(&origins).is_err());
    }
}
