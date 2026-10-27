use super::*;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub(super) fn open_chain(path: &Path, private_leaf: bool) -> io::Result<Vec<File>> {
    let mut cursor = PathBuf::new();
    let mut chain = Vec::new();
    for component in path.components() {
        cursor.push(component.as_os_str());
        let flags = libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        // Ancestors are retained only for identity/ownership validation. Do not
        // request directory enumeration outside the private storage subtree.
        // The final handle stays readable for directory fsync and cleanup.
        #[cfg(target_os = "linux")]
        let flags = flags | if cursor == path { 0 } else { libc::O_PATH };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(flags)
            .open(&cursor)?;
        validate_directory(&file, private_leaf && cursor == path)?;
        chain.push(file);
    }
    if chain.is_empty() {
        return Err(invalid("empty private storage path"));
    }
    Ok(chain)
}

fn validate_directory(file: &File, private_leaf: bool) -> io::Result<()> {
    let uid = unsafe { libc::geteuid() };
    let m = file.metadata()?;
    let mode = m.mode();
    if !m.is_dir()
        || (m.uid() != uid && m.uid() != 0)
        || (mode & 0o022 != 0 && !(m.uid() == 0 && mode & 0o1000 != 0))
    {
        return Err(denied("untrusted private storage ancestor"));
    }
    if private_leaf && (m.uid() != uid || mode & 0o077 != 0) {
        return Err(denied("private storage must be owner-only"));
    }
    Ok(())
}

pub(super) fn create_all_durable(path: &Path) -> io::Result<Vec<File>> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let mut anchor = path;
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(anchor) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(
                    anchor
                        .file_name()
                        .ok_or_else(|| invalid("directory anchor missing"))?,
                );
                anchor = anchor
                    .parent()
                    .ok_or_else(|| invalid("directory anchor missing"))?;
            }
            Err(error) => return Err(error),
        }
    }
    // A trusted existing anchor need not itself be owner-private. Its ancestors
    // stay metadata-only on Linux; only this readable anchor leaf is fsynced.
    let mut chain = open_chain(anchor, missing.is_empty())?;
    let anchor_index = chain.len() - 1;
    for name in missing.into_iter().rev() {
        let name =
            CString::new(name.as_bytes()).map_err(|_| invalid("directory name contains NUL"))?;
        let parent = chain.last().expect("directory anchor").as_raw_fd();
        if unsafe { libc::mkdirat(parent, name.as_ptr(), 0o700) } != 0 {
            let error = io::Error::last_os_error();
            // A raced existing entry must pass the same nofollow/private checks
            // below. Never chmod, traverse a link, or accept an untrusted entry.
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        let fd = unsafe {
            libc::openat(
                parent,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        validate_directory(&file, true)?;
        chain.push(file);
    }
    // Retained handles name the directories actually created/validated. Refuse
    // a renamed/replaced path rather than returning a guard for another tree.
    let current = open_chain(path, true)?;
    if current.len() != chain.len() {
        return Err(denied("private directory path changed during creation"));
    }
    for (retained, current) in chain.iter().zip(&current) {
        let a = retained.metadata()?;
        let b = current.metadata()?;
        if (a.dev(), a.ino()) != (b.dev(), b.ino()) {
            return Err(denied("private directory path changed during creation"));
        }
    }
    for directory in chain[anchor_index..].iter().rev() {
        directory.sync_all()?;
    }
    Ok(chain)
}
