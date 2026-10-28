//! Fixed layout capability for trusted external-check embeddings. The Store
//! owns provenance/layout, NOT the attempt journal's state machine. An embedding
//! must retain its journal gate through dispatch, uncertain-state reconciliation
//! and any Store expiry/root-release decision, even when checking is disabled.
use super::*;
use private_fs::PrivateDir;

const DIRECTORY: &str = "external-checks";
const HEADER: &str = ".header.json";
const HEADER_BYTES: &[u8] = b"{\"format\":\"tinytree-external-checks\",\"version\":1}\n";
const HEADER_MAX: u64 = 128;
const LOCK: &str = "journal.lock";
const CHILDREN: [&str; 2] = ["jobs", "binaries"];

fn private_dir(path: &Path) -> Result<PrivateDir> {
    PrivateDir::open_existing(path)
        .map_err(|_| Error::Corrupt("external check directory must be real and owner-private"))
}
fn validate(root: &Path, required: bool) -> Result<Option<PathBuf>> {
    let path = root.join(DIRECTORY);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound && !required => return Ok(None),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Corrupt("external check layout missing"));
        }
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let _root = private_dir(root)?;
    let directory = private_dir(&path)?;
    let mut header = directory
        .open_existing_file(HEADER, HEADER_MAX)
        .map_err(|_| Error::Corrupt("external check header must be bounded and owner-private"))?;
    let mut bytes = Vec::new();
    Read::take(&mut header, HEADER_MAX + 1).read_to_end(&mut bytes)?;
    if bytes != HEADER_BYTES {
        return Err(Error::Corrupt("external check header format"));
    }
    let lock = directory.open_existing_file(LOCK, 0).map_err(|_| {
        Error::Corrupt("external check journal lock must be empty and owner-private")
    })?;
    drop(lock);
    let mut entries = 0usize;
    for entry in fs::read_dir(&path)? {
        let entry = entry?;
        entries += 1;
        if entries > 4
            || ![HEADER, LOCK, CHILDREN[0], CHILDREN[1]]
                .iter()
                .any(|name| entry.file_name() == *name)
        {
            return Err(Error::Corrupt("unexpected external check layout entry"));
        }
    }
    if entries != 4 {
        return Err(Error::Corrupt("external check layout incomplete"));
    }
    for name in CHILDREN {
        let child = private_dir(&path.join(name))?;
        // No dispatch can precede feature seven. A complete empty setup may
        // survive an uncertain activation, but pre-feature journal data cannot.
        if !required && fs::read_dir(child.path())?.next().transpose()?.is_some() {
            return Err(Error::Corrupt("external check data precedes Store feature"));
        }
    }
    Ok(Some(path))
}

/// Inspect before any Store open-time layout initialization. Unknown/partial
/// fixed namespaces are refused rather than adopted or silently repaired.
pub(super) fn preflight(root: &Path, feature: u8) -> Result<()> {
    validate(root, feature >= 7).map(|_| ())
}

fn temporary(root: &Path, prefix: &str) -> Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o700));
    }
    let temporary = builder.tempdir_in(root)?;
    private_dir(temporary.path())?;
    Ok(temporary)
}
#[cfg(unix)]
fn sync_layout_directory(path: &Path) -> Result<()> {
    sync_dir(path)
}
#[cfg(windows)]
fn sync_layout_directory(_: &Path) -> Result<()> {
    Ok(())
} // Publication uses MOVEFILE_WRITE_THROUGH.
#[cfg(not(any(unix, windows)))]
fn sync_layout_directory(_: &Path) -> Result<()> {
    Err(Error::Invalid("external check layout unsupported"))
}

/// Atomic, same-parent publication MUST NOT replace an existing namespace.
fn publish_directory(from: &Path, to: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::unix::ffi::OsStrExt;
        let from = std::ffi::CString::new(from.as_os_str().as_bytes())
            .map_err(|_| Error::Invalid("external check setup path"))?;
        let to = std::ffi::CString::new(to.as_os_str().as_bytes())
            .map_err(|_| Error::Invalid("external check setup path"))?;
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
        if result != 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};
        let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
        let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
        if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (from, to);
        return Err(Error::Invalid(
            "atomic external check layout publication unsupported",
        ));
    }
    Ok(())
}
// Open and validate without following links, then sync that exact file. Windows
// needs write access for FlushFileBuffers; ReOpenFile preserves the validated
// object identity without a second pathname lookup or create/truncate flags.
fn sync_existing_file(directory: &PrivateDir, name: &str, max_bytes: u64) -> Result<()> {
    let file = directory.open_existing_file(name, max_bytes)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsRawHandle, FromRawHandle};
        use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
            ReOpenFile,
        };
        // SAFETY: file owns a validated regular-file handle. ReOpenFile returns
        // a distinct owned handle to that same object; errors never become File.
        let handle = unsafe {
            ReOpenFile(
                file.as_raw_handle(),
                GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_FLAG_OPEN_REPARSE_POINT,
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error().into());
        }
        let writable = unsafe { File::from_raw_handle(handle) };
        writable.sync_all()?;
    }
    #[cfg(not(windows))]
    file.sync_all()?;
    Ok(())
}
fn ensure_prior_layout(root: &Path) -> Result<()> {
    let cloud = root.join("cloud");
    match fs::symlink_metadata(&cloud) {
        Ok(m) if m.is_dir() => {
            // Earlier feature-one/two crash prefixes need not have established
            // this namespace's durability. Repeat both barriers before feature7.
            sync_layout_directory(&cloud)?;
            sync_layout_directory(root)?;
            return Ok(());
        }
        Ok(_) => return Err(Error::Corrupt("cloud payload namespace type")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let temporary = temporary(root, ".external-cloud-init-")?;
    sync_layout_directory(temporary.path())?;
    publish_directory(temporary.path(), &cloud)?;
    sync_layout_directory(root)?;
    Ok(())
}

impl Inner {
    pub(super) fn external_checks_active(&self) -> Result<bool> {
        Ok(store_feature(
            self.db
                .read_tx()
                .get(&self.spaces.meta, b"format")?
                .as_deref(),
        )? >= 7)
    }
}
impl Store {
    /// The fixed, validated Store-owned journal directory, if activated. This
    /// cannot be redirected or hidden by operator checker configuration.
    /// Attempt loading/validation and the journal gate remain embedding duties.
    pub fn external_checks_directory(&self) -> Result<Option<PathBuf>> {
        let _gate = self.inner.lock()?;
        let active = self.inner.external_checks_active()?;
        let directory = validate(&self.inner.root, active)?;
        Ok(if active { directory } else { None })
    }
    /// Durably establish private layout/header, THEN publish feature seven.
    /// Call before ANY external dispatch. Returning a directory proves layout
    /// activation only, not a check result, cleanup or permission to retire roots.
    /// The trusted embedding must own journal.lock and its shared in-process gate
    /// through dispatch/recovery/expiry decisions; this method does not hold them.
    pub fn enable_external_checks(&self) -> Result<PathBuf> {
        let gate = self.inner.lock()?;
        self.enable_external_checks_locked(&gate)
    }
    /// The caller already owns the healthy physical Store gate and its arena.
    pub(super) fn enable_external_checks_locked(
        &self,
        _gate: &RuntimeGuard<'_>,
    ) -> Result<PathBuf> {
        let active = self.inner.external_checks_active()?;
        let root = &self.inner.root;
        let _root = private_dir(root)?;
        let existing = validate(root, active)?;
        if active {
            return existing.ok_or(Error::Corrupt("external check layout missing"));
        }
        if existing.is_none() {
            let temporary = temporary(root, ".external-checks-init-")?;
            {
                let directory = private_dir(temporary.path())?;
                for name in CHILDREN {
                    let child = PrivateDir::create(directory.path().join(name))?;
                    sync_layout_directory(child.path())?;
                }
                directory.create_descendant_file(HEADER, HEADER_BYTES)?;
                directory.create_descendant_file(LOCK, b"")?;
                sync_layout_directory(directory.path())?;
            }
            publish_directory(temporary.path(), &root.join(DIRECTORY))?;
            sync_layout_directory(root)?;
        } else {
            // Repeat barriers for a complete pre-feature setup recovered after
            // an uncertain activation; never guess that an earlier sync worked.
            let directory = private_dir(&root.join(DIRECTORY))?;
            sync_existing_file(&directory, HEADER, HEADER_MAX)?;
            sync_existing_file(&directory, LOCK, 0)?;
            for name in CHILDREN {
                sync_layout_directory(&directory.path().join(name))?;
            }
            sync_layout_directory(directory.path())?;
            sync_layout_directory(root)?;
        }
        ensure_prior_layout(root)?;
        let path =
            validate(root, false)?.ok_or(Error::Corrupt("external check layout disappeared"))?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        advance_feature(&mut tx, &self.inner.spaces.meta, 7)?;
        self.inner.commit(tx)?;
        Ok(path)
    }
}
