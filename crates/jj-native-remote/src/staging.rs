//! Versioned disposable staging, independent of authoritative object GC.
use anyhow::{Result, ensure};
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};
const MARKER: &str = "staging.format";
const FORMAT: &[u8] = b"tinytree-native-staging-1\n";
pub(crate) struct Plan {
    fresh: bool,
    orphans: Vec<PathBuf>,
}
pub(crate) fn preflight(path: &Path, max_files: usize) -> Result<Plan> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Plan {
                fresh: true,
                orphans: Vec::new(),
            });
        }
        Err(error) => return Err(error.into()),
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "native staging root must be a real directory"
        ),
    }
    if std::fs::read_dir(path)?.next().is_none() {
        return Ok(Plan {
            fresh: true,
            orphans: Vec::new(),
        });
    }
    let marker = std::fs::symlink_metadata(path.join(MARKER))
        .map_err(|_| anyhow::anyhow!("unrecognized native staging directory"))?;
    ensure!(
        marker.is_file()
            && !marker.file_type().is_symlink()
            && marker.len() == FORMAT.len() as u64
            && std::fs::read(path.join(MARKER))? == FORMAT,
        "unrecognized native staging format"
    );
    let owner = std::fs::symlink_metadata(path.join("owner"))?;
    ensure!(
        owner.is_file() && !owner.file_type().is_symlink(),
        "native staging owner file missing"
    );
    let mut orphans = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid native staging filename"))?;
        if matches!(name, "staging.format" | "owner") {
            continue;
        }
        ensure!(
            orphans.len() < max_files,
            "native staging orphan scan exceeds budget"
        );
        ensure!(
            name.starts_with("tinytree-") || name.starts_with("native-publish-"),
            "unrecognized native staging contents"
        );
        let metadata = std::fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "invalid native staging orphan type"
        );
        orphans.push(PathBuf::from(entry.file_name()));
    }
    Ok(Plan {
        fresh: false,
        orphans,
    })
}
pub(crate) struct Owner {
    _file: File,
    _directory: private_fs::PrivateDir,
}
pub(crate) fn open(path: &Path, plan: Plan) -> Result<Arc<Owner>> {
    let directory = crate::private_directory(path)?;
    let path = path.canonicalize()?;
    let owner = OpenOptions::new()
        .read(true)
        .write(true)
        .create(plan.fresh)
        .truncate(false)
        .open(path.join("owner"))?;
    fs2::FileExt::try_lock_exclusive(&owner)
        .map_err(|_| anyhow::anyhow!("native staging directory already owned"))?;
    if plan.fresh {
        directory.replace(MARKER, FORMAT)?;
    }
    // Only verified owned disposable names were accepted during preflight; a
    // surviving owner lock prevents cleanup racing live client copy workers.
    for orphan in plan.orphans {
        std::fs::remove_file(path.join(orphan))?;
    }
    #[cfg(unix)]
    File::open(&path)?.sync_all()?;
    Ok(Arc::new(Owner {
        _file: owner,
        _directory: directory,
    }))
}
