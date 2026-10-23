//! The source identity marker is an explicit native control file, never a
//! generic core-local sparse placeholder. Kernel handles protect its bytes and
//! binding; unsupported/unpreventable administration changes fail capture closed.
use super::source_capture::{Oplock, tag};
use super::*;
use private_fs::windows::UserSecurity;
use std::io;

pub(super) struct SourceControl {
    root: PathBuf,
    _ancestors: private_fs::WindowsTrustedDirectory,
    _root_file: File,
    directory: File,
    marker: Mutex<File>,
    directory_identity: Fingerprint,
    marker_identity: Fingerprint,
    expected: Vec<u8>,
    fence: Oplock,
    failed: AtomicBool,
}
fn protected(path: &str) -> bool {
    path.split(['/', '\\']).next().is_some_and(|part| {
        part.split(':')
            .next()
            .unwrap_or(part)
            .trim_end_matches(['.', ' '])
            .eq_ignore_ascii_case(".jj")
    })
}
impl SourceControl {
    pub(super) fn is_path(&self, path: &str) -> bool {
        protected(path)
    }
    pub(super) fn check_open(&self) -> Result<()> {
        ensure!(
            !self.failed.load(Ordering::Acquire),
            "native source administration changed"
        );
        self.fence.validate()
    }
    pub(super) fn invalidate(&self) {
        self.failed.store(true, Ordering::Release);
    }
    pub(super) fn prepare(root: &Path, expected: Vec<u8>, fresh: bool) -> Result<Self> {
        ensure!(
            !expected.is_empty() && expected.len() <= 4096,
            "invalid native source control marker"
        );
        let ancestors = private_fs::WindowsTrustedDirectory::open(
            root.parent().context("native source root has no parent")?,
        )?;
        // The native source owner must provision this root privately. A generic
        // output root must never acquire source-control meaning accidentally.
        let root_file = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(root)?;
        unsafe { private_fs::windows::verify_private_handle(root_file.as_raw_handle()) }?;
        let root_tag = tag(&root_file)?;
        ensure!(
            root_tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
                || root_tag.ReparseTag
                    == windows_sys::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS,
            "native source root has a foreign reparse tag"
        );
        let directory_path = root.join(".jj");
        let marker_path = directory_path.join("native-workspace");
        match std::fs::symlink_metadata(&directory_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound && fresh => {
                let security = UserSecurity::new()?;
                if unsafe {
                    CreateDirectoryW(wide(&directory_path).as_ptr(), &security.attributes())
                } == 0
                {
                    return Err(io::Error::last_os_error().into());
                }
            }
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let directory = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&directory_path)?;
        ensure!(
            directory.metadata()?.is_dir()
                && directory.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            "native source administration must be a real directory"
        );
        unsafe { private_fs::windows::verify_private_handle(directory.as_raw_handle()) }?;
        if !marker_path.try_exists()? {
            ensure!(
                fresh,
                "recognized source marker is missing; refusing automatic repair"
            );
            ensure!(
                std::fs::read_dir(&directory_path)?.next().is_none(),
                "foreign source administration contents"
            );
            let security = UserSecurity::new()?;
            let handle = unsafe {
                CreateFileW(
                    wide(&marker_path).as_ptr(),
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                    0,
                    &security.attributes(),
                    CREATE_NEW,
                    FILE_ATTRIBUTE_NORMAL,
                    ptr::null_mut(),
                )
            };
            ensure!(
                handle != INVALID_HANDLE_VALUE,
                "cannot create native source control marker"
            );
            use std::os::windows::io::FromRawHandle;
            let mut file = unsafe { File::from_raw_handle(handle) };
            file.write_all(&expected)?;
            file.sync_all()?;
        }
        let marker = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&marker_path)?;
        ensure!(
            marker.metadata()?.is_file()
                && marker.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            "native source marker must be a real file"
        );
        unsafe { private_fs::windows::verify_private_handle(marker.as_raw_handle()) }?;
        let fence = Oplock::open(&marker_path, false)?;
        let control = Self {
            root: root.to_owned(),
            _ancestors: ancestors,
            _root_file: root_file,
            directory_identity: file_fingerprint(&directory)?,
            marker_identity: file_fingerprint(&marker)?,
            directory,
            marker: Mutex::new(marker),
            expected,
            fence,
            failed: AtomicBool::new(false),
        };
        control.verify()?;
        Ok(control)
    }
    pub(super) fn verify(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            bail!("native source administration changed");
        }
        let result = (|| {
            self.fence.validate()?;
            let directory_path = self.root.join(".jj");
            ensure!(
                native_fingerprint(&directory_path)?.as_ref() == Some(&self.directory_identity),
                "native source administration identity changed"
            );
            ensure!(
                self.directory.metadata()?.is_dir()
                    && self.directory.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT
                        == 0,
                "native source administration changed type"
            );
            let mut names = std::fs::read_dir(&directory_path)?;
            let entry = names.next().context("native source marker missing")??;
            ensure!(
                entry.file_name() == "native-workspace" && names.next().is_none(),
                "unexpected native source administration contents"
            );
            ensure!(
                native_fingerprint(&entry.path())?.as_ref() == Some(&self.marker_identity),
                "native source marker identity changed"
            );
            let mut file = self.marker.lock().unwrap();
            ensure!(
                file.metadata()?.len() == self.expected.len() as u64,
                "native source marker size changed"
            );
            file.seek(SeekFrom::Start(0))?;
            let mut bytes = Vec::new();
            (&mut *file).take(4097).read_to_end(&mut bytes)?;
            ensure!(
                bytes == self.expected,
                "native source marker contents changed"
            );
            self.fence.validate()?;
            Ok(())
        })();
        if result.is_err() {
            self.invalidate();
        }
        result
    }
}
