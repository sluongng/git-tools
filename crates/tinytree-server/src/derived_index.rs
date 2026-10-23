//! Explicit activation of rebuildable index generations. Older schema directories
//! and unselected generations are never opened, renamed, migrated, or deleted.
use crate::QueryConfig;
use private_fs::PrivateDir;
use std::{io, path::Path, sync::Arc};
use tinytree_index::{Index, STORAGE_SCHEMA};

pub(crate) fn open(storage: &Path, config: &QueryConfig) -> tinytree_index::Result<Arc<Index>> {
    // Lexical absolutization does not resolve symlinks. PrivateDir validates and
    // retains the inspected directory chain, including all new fixed children.
    let storage = PrivateDir::open_existing(std::path::absolute(storage)?)?;
    let version = PrivateDir::create(
        storage
            .path()
            .join(format!("derived-index-v{STORAGE_SCHEMA}")),
    )?;
    let generation = PrivateDir::create(
        version
            .path()
            .join(format!("generation-{}", config.index_rebuild_generation)),
    )?;
    let mut count = 0;
    let mut lock = false;
    let mut data = false;
    for entry in std::fs::read_dir(generation.path())? {
        let entry = entry?;
        let kind = entry.file_type()?;
        count += 1;
        if entry.file_name() == "index.lock" && kind.is_file() {
            lock = true;
        } else if entry.file_name() == "index.fjall" && kind.is_dir() {
            data = true;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unrecognized derived index generation",
            )
            .into());
        }
    }
    if count != 0 && !(count == 2 && lock && data) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incomplete derived index generation",
        )
        .into());
    }
    let _engine = if count != 0 {
        // Fjall treats a missing version file as fresh storage, even when its
        // directory already contains unrelated files. Validate identity before
        // allowing recovery to open or create anything in an existing engine.
        Some(known_engine(&generation.path().join("index.fjall"))?)
    } else {
        None
    };
    Index::open(generation.path(), &config.index_limits())
}

#[cfg(unix)]
fn known_engine(path: &Path) -> io::Result<std::fs::File> {
    use std::{
        io::Read,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
    };
    // The private generation encloses this child. Fjall creates its own directory
    // with the process umask, so a normal 0755 child is valid; do not chmod it or
    // weaken PrivateDir's owner-private leaf contract.
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe derived index engine directory",
        ));
    }
    let metadata = std::fs::symlink_metadata(path.join("version"))?;
    if !metadata.is_file() || metadata.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unrecognized derived index engine",
        ));
    }
    // SAFETY: the retained directory owns a live fd; the child name is a static
    // NUL-terminated string. On success ownership of the returned fd moves once.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"version".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let marker = unsafe { std::fs::File::from_raw_fd(fd) };
    let metadata = marker.metadata()?;
    if !metadata.is_file()
        || metadata.len() != 4
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe derived index engine marker",
        ));
    }
    let mut bytes = Vec::with_capacity(5);
    marker.take(5).read_to_end(&mut bytes)?;
    if bytes != b"FJL\x03" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unrecognized derived index engine",
        ));
    }
    Ok(directory)
}
#[cfg(windows)]
fn known_engine(path: &Path) -> io::Result<PrivateDir> {
    let directory = PrivateDir::open_existing(path)?;
    let metadata = std::fs::symlink_metadata(directory.path().join("version"))?;
    if !metadata.is_file() || metadata.len() != 4 || directory.read("version", 4)? != b"FJL\x03" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unrecognized derived index engine",
        ));
    }
    Ok(directory)
}
#[cfg(not(any(unix, windows)))]
fn known_engine(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "derived index engine path validation unavailable",
    ))
}
