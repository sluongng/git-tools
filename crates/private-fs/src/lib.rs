//! Owner-private storage. A successful check never changes an existing ACL or
//! permission mode. Keep `PrivateDir` alive while operations use its path; its
//! handles retain the inspected directory chain (and prohibit Windows rename).
use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
pub mod windows;
#[cfg(windows)]
pub use windows::TrustedDirectory as WindowsTrustedDirectory;

/// Validated, owner-private directory and retained handles to every ancestor.
/// Paths are local, absolute, bounded, and free of symlink/reparse traversal.
/// The security boundary is the current OS user, not hostile same-user code.
pub struct PrivateDir {
    path: PathBuf,
    _chain: Vec<File>,
}
impl std::fmt::Debug for PrivateDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateDir").finish_non_exhaustive()
    }
}
impl PrivateDir {
    pub fn open_existing(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        normalized(path)?;
        #[cfg(unix)]
        let chain = unix::open_chain(path, true)?;
        #[cfg(windows)]
        let chain = windows::open_chain(path, true)?;
        #[cfg(not(any(unix, windows)))]
        let chain = return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private storage unsupported",
        ));
        Ok(Self {
            path: path.to_owned(),
            _chain: chain,
        })
    }
    /// Create one child under an existing trusted directory. Existing paths are
    /// validated, never repaired. Ancestors are not recursively created.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        normalized(path)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| invalid("private directory requires a parent"))?;
        #[cfg(unix)]
        let _parent = unix::open_chain(parent, false)?;
        #[cfg(windows)]
        let _parent = windows::open_chain(parent, false)?;
        #[cfg(unix)]
        let result = {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().mode(0o700).create(path)
        };
        #[cfg(windows)]
        let result = windows::create_directory(path);
        #[cfg(not(any(unix, windows)))]
        let result = Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private storage unsupported",
        ));
        match result {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        Self::open_existing(path)
    }
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bound and validate a daemon-owned child tree before deleting any of it.
    /// This rejects symlinks and special files; it never follows them. The caller
    /// must separately own the lifecycle and prove this is disposable state.
    pub fn check_child_tree(
        &self,
        name: impl AsRef<OsStr>,
        max_entries: usize,
        max_depth: usize,
    ) -> io::Result<()> {
        let name = name.as_ref();
        single_name(name)?;
        let child = Self::open_existing(self.path.join(name))?;
        self.check_disposable_mount(child._chain.last().expect("directory chain"))?;
        let mut remaining = max_entries;
        child.walk_disposable(max_depth, &mut remaining, false)
    }
    /// Idempotently remove a bounded, prevalidated private child tree. Not for
    /// user data or shared stores. Retain lifecycle locks across this operation.
    pub fn remove_child_tree(
        &self,
        name: impl AsRef<OsStr>,
        max_entries: usize,
        max_depth: usize,
    ) -> io::Result<()> {
        let name = name.as_ref();
        single_name(name)?;
        match std::fs::symlink_metadata(self.path.join(name)) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
            Ok(_) => {}
        }
        self.check_child_tree(name, max_entries, max_depth)?;
        let child = Self::open_existing(self.path.join(name))?;
        let mut remaining = max_entries;
        child.walk_disposable(max_depth, &mut remaining, true)?;
        drop(child);
        std::fs::remove_dir(self.path.join(name))?;
        self.sync_directory()
    }
    fn walk_disposable(&self, depth: usize, remaining: &mut usize, remove: bool) -> io::Result<()> {
        if depth == 0 {
            return Err(invalid("disposable tree depth limit exceeded"));
        }
        for item in std::fs::read_dir(&self.path)? {
            let item = item?;
            *remaining = remaining
                .checked_sub(1)
                .ok_or_else(|| invalid("disposable tree entry limit exceeded"))?;
            let kind = item.file_type()?;
            if kind.is_dir() {
                let path = item.path();
                // Database internals may use 0755 inside our private ancestor.
                // Still reject symlinks and writable/untrusted ancestors.
                #[cfg(unix)]
                let chain = unix::open_chain(&path, false)?;
                #[cfg(windows)]
                let chain = windows::open_chain(&path, false)?;
                #[cfg(not(any(unix, windows)))]
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "private cleanup unsupported",
                ));
                let child = Self {
                    path,
                    _chain: chain,
                };
                self.check_disposable_mount(child._chain.last().expect("directory chain"))?;
                child.walk_disposable(depth - 1, remaining, remove)?;
                drop(child);
                if remove {
                    std::fs::remove_dir(item.path())?;
                }
            } else if kind.is_file() {
                let file = Self::open_regular(&item.path(), u64::MAX, false)?;
                self.check_disposable_mount(&file)?;
                drop(file);
                if remove {
                    std::fs::remove_file(item.path())?;
                }
            } else {
                return Err(denied("disposable tree contains a symlink or special file"));
            }
        }
        if remove {
            self.sync_directory()?;
        }
        Ok(())
    }
    fn check_disposable_mount(&self, file: &File) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mount = |file: &File| -> io::Result<u64> {
                let mut stat: libc::statx = unsafe { std::mem::zeroed() };
                // Empty-path statx addresses the retained nofollow descriptor,
                // detecting bind mounts as well as different-device mounts.
                if unsafe {
                    libc::statx(
                        file.as_raw_fd(),
                        c"".as_ptr(),
                        libc::AT_EMPTY_PATH,
                        libc::STATX_MNT_ID,
                        &mut stat,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
                if stat.stx_mask & libc::STATX_MNT_ID == 0 {
                    return Err(denied("mount identity unavailable for private cleanup"));
                }
                Ok(stat.stx_mnt_id)
            };
            if mount(self._chain.last().expect("directory chain"))? != mount(file)? {
                return Err(denied("private cleanup cannot cross a mount boundary"));
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            use std::os::unix::fs::MetadataExt;
            if self
                ._chain
                .last()
                .expect("directory chain")
                .metadata()?
                .dev()
                != file.metadata()?.dev()
            {
                return Err(denied("private cleanup cannot cross a filesystem boundary"));
            }
        }
        Ok(())
    }
    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        self._chain.last().expect("directory chain").sync_all()?;
        Ok(())
    }

    /// Open/create a persistent owner-private lock file without following a
    /// symlink or reparse point. Callers apply their OS locking primitive and
    /// keep both this directory guard and returned file alive.
    pub fn open_lock(&self, name: impl AsRef<OsStr>) -> io::Result<File> {
        let name = name.as_ref();
        single_name(name)?;
        #[cfg(windows)]
        let file = windows::open_private_lock(&self.path.join(name))?;
        #[cfg(not(windows))]
        let file = {
            let mut options = OpenOptions::new();
            options.read(true).write(true).create(true).truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
            }
            options.open(self.path.join(name))?
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(invalid("lock is not a regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                return Err(denied("lock file must be owner-private"));
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::{fs::MetadataExt, io::AsRawHandle};
            if metadata.file_attributes()
                & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
                != 0
            {
                return Err(denied("lock cannot be a reparse point"));
            }
            unsafe { windows::verify_private_child_handle(file.as_raw_handle()) }?;
        }
        file.sync_all()?;
        #[cfg(unix)]
        self._chain.last().expect("directory chain").sync_all()?;
        Ok(file)
    }

    /// Read one regular, non-reparse child without unbounded allocation.
    pub fn read(&self, name: impl AsRef<OsStr>, max_bytes: usize) -> io::Result<Vec<u8>> {
        let name = name.as_ref();
        single_name(name)?;
        Self::read_regular(&self.path.join(name), max_bytes)
    }

    /// Read beneath this already owner-private directory without requiring
    /// jj-created inner directories to have owner-only modes themselves. Every
    /// parent remains no-follow, trusted-owner and non-unsafe-writable; the
    /// inspected chain stays owned through the bounded regular-file read.
    /// This never adopts a child as PrivateDir or changes permissions.
    pub fn read_descendant(
        &self,
        relative: impl AsRef<Path>,
        max_bytes: usize,
    ) -> io::Result<Vec<u8>> {
        let relative = relative.as_ref();
        if relative.as_os_str().len() > 4096 {
            return Err(invalid("private descendant path too long"));
        }
        let mut count = 0usize;
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(invalid(
                    "private descendant requires relative normal components",
                ));
            };
            single_name(name)?;
            count += 1;
            if count > 16 {
                return Err(invalid("private descendant path too deep"));
            }
        }
        if count == 0 {
            return Err(invalid("empty private descendant path"));
        }
        let path = self.path.join(relative);
        let parent = path
            .parent()
            .ok_or_else(|| invalid("private descendant has no parent"))?;
        #[cfg(unix)]
        let _chain = unix::open_chain(parent, false)?;
        #[cfg(windows)]
        let _chain = windows::open_chain(parent, false)?;
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private descendant reads unsupported",
        ));
        Self::read_regular(&path, max_bytes)
    }

    /// Create one fresh regular file beneath this private anchor. Known inner
    /// directories may use ordinary trusted-owner modes; no directory is adopted
    /// or repaired. Existing files (including symlinks) are never replaced.
    /// Keep the inspected chain through file SyncAll (and Unix parent SyncAll).
    pub fn create_descendant_file(
        &self,
        relative: impl AsRef<Path>,
        bytes: &[u8],
    ) -> io::Result<()> {
        let relative = relative.as_ref();
        if relative.as_os_str().len() > 4096 {
            return Err(invalid("private descendant path too long"));
        }
        let mut count = 0usize;
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(invalid(
                    "private descendant requires relative normal components",
                ));
            };
            single_name(name)?;
            count += 1;
            if count > 16 {
                return Err(invalid("private descendant path too deep"));
            }
        }
        if count == 0 {
            return Err(invalid("empty private descendant path"));
        }
        let path = self.path.join(relative);
        let parent = path
            .parent()
            .ok_or_else(|| invalid("private descendant has no parent"))?;
        #[cfg(unix)]
        let chain = unix::open_chain(parent, false)?;
        #[cfg(windows)]
        let _chain = windows::open_chain(parent, false)?;
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "private descendant writes unsupported",
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
            );
        }
        let mut file = options.open(path)?;
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            unsafe { windows::verify_private_child_handle(file.as_raw_handle()) }?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        #[cfg(unix)]
        chain.last().expect("directory chain").sync_all()?;
        Ok(())
    }

    fn read_regular(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
        if max_bytes == 0 || max_bytes > 64 * 1024 * 1024 {
            return Err(invalid("private file read limit must be within 1..64 MiB"));
        }
        let file = Self::open_regular(path, max_bytes as u64, false)?;
        let mut bytes = Vec::new();
        file.take(max_bytes as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > max_bytes {
            return Err(invalid("private file exceeds read limit"));
        }
        Ok(bytes)
    }

    /// Open one existing owner-private regular child read-only without following
    /// symlinks/reparse points or creating anything. Retain this directory guard
    /// while using the returned file. `max_bytes` bounds its current size only;
    /// callers must separately bound all later reads and handle mutation.
    pub fn open_existing_file(&self, name: impl AsRef<OsStr>, max_bytes: u64) -> io::Result<File> {
        let name = name.as_ref();
        single_name(name)?;
        Self::open_regular(&self.path.join(name), max_bytes, true)
    }

    fn open_regular(path: &Path, max_bytes: u64, owner_private: bool) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
            );
        }
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > max_bytes {
            return Err(invalid("private file is not regular or exceeds read limit"));
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if metadata.file_attributes()
                & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
                != 0
            {
                return Err(denied("private file cannot be a reparse point"));
            }
        }
        if owner_private {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                    return Err(denied("file must be owner-private"));
                }
            }
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle;
                unsafe { windows::verify_private_child_handle(file.as_raw_handle()) }?;
            }
        }
        Ok(file)
    }

    /// Fsync a new file, then replace a single child atomically. Unix additionally
    /// fsyncs the parent; Windows requests MOVEFILE_WRITE_THROUGH. The latter is
    /// the documented rename request, not a power-loss guarantee for every disk.
    pub fn replace(&self, name: impl AsRef<OsStr>, bytes: &[u8]) -> io::Result<()> {
        let name = name.as_ref();
        single_name(name)?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let (temporary, mut file) = (0..128)
            .find_map(|_| {
                let n = NEXT.fetch_add(1, Ordering::Relaxed);
                let path = self
                    .path
                    .join(format!(".private-{}-{n}.tmp", std::process::id()));
                match OpenOptions::new().write(true).create_new(true).open(&path) {
                    Ok(file) => Some(Ok((path, file))),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .unwrap_or_else(|| {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "temporary names exhausted",
                ))
            })?;
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            #[cfg(windows)]
            windows::replace(&temporary, &self.path.join(name))?;
            #[cfg(not(windows))]
            std::fs::rename(&temporary, self.path.join(name))?;
            #[cfg(unix)]
            self._chain.last().expect("directory chain").sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        result
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
fn normalized(path: &Path) -> io::Result<()> {
    if !path.is_absolute()
        || path.as_os_str().len() > 4096
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid(
            "private storage path must be bounded, absolute and normalized",
        ));
    }
    #[cfg(windows)]
    {
        use std::path::Prefix;
        if !matches!(path.components().next(), Some(Component::Prefix(p))
            if matches!(p.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_)))
        {
            return Err(invalid("private storage requires a local drive path"));
        }
    }
    Ok(())
}
fn single_name(name: &OsStr) -> io::Result<()> {
    let mut parts = Path::new(name).components();
    if !matches!(parts.next(), Some(Component::Normal(_)))
        || parts.next().is_some()
        || name.is_empty()
        || name.len() > 255
    {
        return Err(invalid("publication target must be one bounded filename"));
    }
    #[cfg(windows)]
    {
        let text = name
            .to_str()
            .ok_or_else(|| invalid("private filename must be valid Unicode"))?;
        let stem = text.split('.').next().unwrap_or("").to_uppercase();
        if text.ends_with(['.', ' '])
            || text.chars().any(|c| c < ' ' || "<>:\"/\\|?*".contains(c))
            || matches!(
                stem.as_str(),
                "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
            )
            || (stem.len() == 4
                && (stem.starts_with("COM") || stem.starts_with("LPT"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9'))
            || ["COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"].contains(&stem.as_str())
        {
            return Err(invalid("unsupported private filename"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
