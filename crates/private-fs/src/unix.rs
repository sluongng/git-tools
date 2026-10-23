use super::*;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub(super) fn open_chain(path: &Path, private_leaf: bool) -> io::Result<Vec<File>> {
    let uid = unsafe { libc::geteuid() };
    let mut cursor = PathBuf::new();
    let mut chain = Vec::new();
    for component in path.components() {
        cursor.push(component.as_os_str());
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&cursor)?;
        let m = file.metadata()?;
        let mode = m.mode();
        if !m.is_dir()
            || (m.uid() != uid && m.uid() != 0)
            || (mode & 0o022 != 0 && !(m.uid() == 0 && mode & 0o1000 != 0))
        {
            return Err(denied("untrusted private storage ancestor"));
        }
        if private_leaf && cursor == path && (m.uid() != uid || mode & 0o077 != 0) {
            return Err(denied("private storage must be owner-only"));
        }
        chain.push(file);
    }
    if chain.is_empty() {
        return Err(invalid("empty private storage path"));
    }
    Ok(chain)
}
