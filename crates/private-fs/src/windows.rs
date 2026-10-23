//! Windows security primitives for local owner-private directories and IPC.
//! DACL checks are conservative: unknown grant ACEs fail closed; denial ACEs do
//! not make an otherwise excessive grant acceptable. No existing ACL is repaired.
use super::*;
use std::{
    ffi::c_void,
    mem::{size_of, zeroed},
    os::windows::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    },
    ptr::null_mut,
};
use windows_sys::Win32::{
    Foundation::{HANDLE, LocalFree},
    Security::{
        Authorization::{ConvertStringSidToSidW, GetSecurityInfo, SE_FILE_OBJECT},
        *,
    },
    Storage::FileSystem::{
        CreateDirectoryW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        FILE_SHARE_WRITE, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    },
    System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE, SECURITY_DESCRIPTOR_REVISION,
    },
    System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
    },
};

fn bool_result(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut text: Vec<_> = value.encode_wide().collect();
    if text.contains(&0) {
        return Err(invalid("NUL in native path"));
    }
    text.push(0);
    Ok(text)
}
#[derive(Clone)]
struct Sid(Vec<u32>);
impl Sid {
    fn as_ptr(&self) -> PSID {
        self.0.as_ptr().cast_mut().cast()
    }
    unsafe fn copy(raw: PSID) -> io::Result<Self> {
        if raw.is_null() || unsafe { IsValidSid(raw) } == 0 {
            return Err(denied("invalid security identity"));
        }
        let size = unsafe { GetLengthSid(raw) };
        if size == 0 || size > SECURITY_MAX_SID_SIZE {
            return Err(denied("invalid security identity size"));
        }
        let sid = Self(vec![0; (size as usize).div_ceil(size_of::<u32>())]);
        bool_result(unsafe { CopySid(size, sid.as_ptr(), raw) })?;
        Ok(sid)
    }
    unsafe fn equal(&self, raw: PSID) -> bool {
        !raw.is_null()
            && unsafe { IsValidSid(raw) } != 0
            && unsafe { EqualSid(self.as_ptr(), raw) } != 0
    }
    fn well_known(kind: WELL_KNOWN_SID_TYPE) -> io::Result<Self> {
        let mut size = SECURITY_MAX_SID_SIZE;
        let sid = Self(vec![0; (size as usize).div_ceil(size_of::<u32>())]);
        bool_result(unsafe { CreateWellKnownSid(kind, null_mut(), sid.as_ptr(), &mut size) })?;
        Ok(sid)
    }
    fn from_text(text: &str) -> io::Result<Self> {
        let text = wide(OsStr::new(text))?;
        let mut sid = null_mut();
        bool_result(unsafe { ConvertStringSidToSidW(text.as_ptr(), &mut sid) })?;
        let result = unsafe { Self::copy(sid) };
        unsafe {
            LocalFree(sid);
        }
        result
    }
}
fn token_user(process: HANDLE) -> io::Result<Sid> {
    let mut token = null_mut();
    bool_result(unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) })?;
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    let mut size = 0;
    unsafe {
        GetTokenInformation(token.as_raw_handle(), TokenUser, null_mut(), 0, &mut size);
    }
    if size < size_of::<TOKEN_USER>() as u32 || size > 64 * 1024 {
        return Err(denied("invalid process token identity size"));
    }
    // TOKEN_USER and its variable SID storage require pointer alignment.
    let mut bytes = vec![0usize; (size as usize).div_ceil(size_of::<usize>())];
    bool_result(unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            bytes.as_mut_ptr().cast(),
            size,
            &mut size,
        )
    })?;
    let user = unsafe { &*bytes.as_ptr().cast::<TOKEN_USER>() };
    unsafe { Sid::copy(user.User.Sid) }
}
/// Current process user SID. Debug intentionally reveals no identity.
#[derive(Clone)]
pub struct CurrentUser {
    sid: Sid,
}
impl CurrentUser {
    pub fn get() -> io::Result<Self> {
        Ok(Self {
            sid: token_user(unsafe { GetCurrentProcess() })?,
        })
    }
    /// A supplementary peer check, not a substitute for an authenticated object
    /// handle/DACL. Process identifiers alone are not capabilities.
    pub fn same_process_user(&self, process_id: u32) -> io::Result<bool> {
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        let other = token_user(handle.as_raw_handle())?;
        Ok(unsafe { self.sid.equal(other.as_ptr()) })
    }
}
/// Owner-only, protected security descriptor. Keep this value alive while its
/// `attributes()` result is passed to a native object-creation call.
pub struct UserSecurity {
    user: CurrentUser,
    _acl: Vec<u32>,
    descriptor: Box<SECURITY_DESCRIPTOR>,
}
impl UserSecurity {
    pub fn new() -> io::Result<Self> {
        let user = CurrentUser::get()?;
        let sid_size = unsafe { GetLengthSid(user.sid.as_ptr()) } as usize;
        let acl_size =
            size_of::<ACL>() + size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>() + sid_size;
        let mut acl = vec![0u32; acl_size.div_ceil(size_of::<u32>())];
        let raw_acl = acl.as_mut_ptr().cast::<ACL>();
        bool_result(unsafe { InitializeAcl(raw_acl, acl_size as u32, ACL_REVISION) })?;
        bool_result(unsafe {
            AddAccessAllowedAceEx(
                raw_acl,
                ACL_REVISION,
                OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
                FILE_ALL_ACCESS,
                user.sid.as_ptr(),
            )
        })?;
        let mut descriptor = Box::<SECURITY_DESCRIPTOR>::new(unsafe { zeroed() });
        let raw = (&mut *descriptor as *mut SECURITY_DESCRIPTOR).cast::<c_void>();
        bool_result(unsafe { InitializeSecurityDescriptor(raw, SECURITY_DESCRIPTOR_REVISION) })?;
        bool_result(unsafe { SetSecurityDescriptorOwner(raw, user.sid.as_ptr(), 0) })?;
        bool_result(unsafe { SetSecurityDescriptorDacl(raw, 1, raw_acl, 0) })?;
        bool_result(unsafe {
            SetSecurityDescriptorControl(raw, SE_DACL_PROTECTED, SE_DACL_PROTECTED)
        })?;
        Ok(Self {
            user,
            _acl: acl,
            descriptor,
        })
    }
    pub fn attributes(&self) -> SECURITY_ATTRIBUTES {
        // Owner SID and ACL backing allocations stay at stable heap addresses.
        let _ = &self.user;
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: (&*self.descriptor as *const SECURITY_DESCRIPTOR)
                .cast_mut()
                .cast(),
            bInheritHandle: 0,
        }
    }
}
struct SecurityInfo {
    descriptor: PSECURITY_DESCRIPTOR,
    owner: PSID,
    acl: *mut ACL,
}
impl Drop for SecurityInfo {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.descriptor);
        }
    }
}
impl SecurityInfo {
    unsafe fn get(handle: HANDLE) -> io::Result<Self> {
        let mut value = Self {
            descriptor: null_mut(),
            owner: null_mut(),
            acl: null_mut(),
        };
        let code = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut value.owner,
                null_mut(),
                &mut value.acl,
                null_mut(),
                &mut value.descriptor,
            )
        };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        if value.descriptor.is_null()
            || value.acl.is_null()
            || unsafe { IsValidSecurityDescriptor(value.descriptor) } == 0
            || unsafe { IsValidAcl(value.acl) } == 0
        {
            return Err(denied("missing or invalid private storage ACL"));
        }
        Ok(value)
    }
    fn protected(&self) -> io::Result<bool> {
        let mut control = 0;
        let mut revision = 0;
        bool_result(unsafe {
            GetSecurityDescriptorControl(self.descriptor, &mut control, &mut revision)
        })?;
        Ok(control & SE_DACL_PROTECTED != 0)
    }
    fn validate(
        &self,
        user: &CurrentUser,
        private_leaf: bool,
        trusted: &[Sid],
        require_protected: bool,
    ) -> io::Result<()> {
        if !unsafe { user.sid.equal(self.owner) }
            && (private_leaf || !trusted.iter().any(|sid| unsafe { sid.equal(self.owner) }))
        {
            return Err(denied("untrusted private storage owner"));
        }
        if require_protected && !self.protected()? {
            return Err(denied("private storage ACL must be protected"));
        }
        let count = unsafe { (*self.acl).AceCount };
        let mut owner_access = false;
        for index in 0..u32::from(count) {
            let mut raw: *mut c_void = null_mut();
            bool_result(unsafe { GetAce(self.acl, index, &mut raw) })?;
            let header = unsafe { &*raw.cast::<ACE_HEADER>() };
            let inherit_only = u32::from(header.AceFlags) & INHERIT_ONLY_ACE != 0;
            // A protected leaf must be private for both itself AND children
            // created with inherited security (temporary/state files). Skipping
            // inherit-only grants here would expose those children by name.
            // Ancestors do not contribute inherited ACEs to our protected leaf.
            if inherit_only && !private_leaf {
                continue;
            }
            if u32::from(header.AceType) == ACCESS_DENIED_ACE_TYPE {
                continue;
            }
            if u32::from(header.AceType) != ACCESS_ALLOWED_ACE_TYPE
                || usize::from(header.AceSize) < size_of::<ACCESS_ALLOWED_ACE>()
            {
                return Err(denied("unsupported private storage grant ACE"));
            }
            let ace = unsafe { &*raw.cast::<ACCESS_ALLOWED_ACE>() };
            let sid = (&ace.SidStart as *const u32).cast_mut().cast::<c_void>();
            // IsValidAcl validates ACE boundaries; bound the SID before APIs read it.
            let sid_offset = size_of::<ACCESS_ALLOWED_ACE>() - size_of::<u32>();
            let max_sid = usize::from(header.AceSize).saturating_sub(sid_offset);
            if max_sid < 8 {
                return Err(denied("invalid grant identity"));
            }
            let sub_authorities = unsafe { *sid.cast::<u8>().add(1) } as usize;
            if 8 + 4 * sub_authorities > max_sid || unsafe { IsValidSid(sid) } == 0 {
                return Err(denied("invalid grant identity"));
            }
            let own = unsafe { user.sid.equal(sid) };
            if own && !inherit_only && ace.Mask & FILE_ALL_ACCESS == FILE_ALL_ACCESS {
                owner_access = true;
            }
            if own || (!private_leaf && trusted.iter().any(|s| unsafe { s.equal(sid) })) {
                continue;
            }
            // Read/list/traverse and adding *new* subdirectories are allowed on
            // ancestors. They cannot redirect an existing retained directory.
            // Existing directory mutation, deletion or ACL takeover is not.
            const MUTATE: u32 = 0x0001_0000
                | 0x0004_0000
                | 0x0008_0000
                | 0x0000_0040
                | 0x0000_0002
                | 0x0000_0010
                | 0x0000_0100
                | 0x4000_0000
                | 0x1000_0000;
            if private_leaf || ace.Mask & MUTATE != 0 {
                return Err(denied("private storage grants access to another principal"));
            }
        }
        if private_leaf && !owner_access {
            return Err(denied("private storage lacks owner control"));
        }
        Ok(())
    }
}
/// Validate a live file or named-pipe handle as current-user owned and private.
///
/// # Safety
/// `handle` must remain a valid open file/pipe handle for the entire call. This
/// function does not take ownership or close it.
pub unsafe fn verify_private_handle(handle: HANDLE) -> io::Result<()> {
    let user = CurrentUser::get()?;
    unsafe { SecurityInfo::get(handle) }?.validate(&user, true, &[], true)
}
/// Validate an inherited private child file. Its protected containing
/// `PrivateDir` must remain held; inherited user-only DACLs need not themselves
/// carry SE_DACL_PROTECTED.
///
/// # Safety
/// `handle` must remain valid and its protected parent must remain owned.
pub unsafe fn verify_private_child_handle(handle: HANDLE) -> io::Result<()> {
    let user = CurrentUser::get()?;
    let info = unsafe { SecurityInfo::get(handle) }?;
    info.validate(&user, true, &[], false)
}
fn trusted_os_sids() -> io::Result<Vec<Sid>> {
    Ok(vec![
        Sid::well_known(WinLocalSystemSid)?,
        Sid::well_known(WinBuiltinAdministratorsSid)?,
        // Stable NT SERVICE\\TrustedInstaller service SID, not a local account.
        Sid::from_text("S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464")?,
    ])
}
pub(super) fn open_chain(path: &Path, private_leaf: bool) -> io::Result<Vec<File>> {
    let user = CurrentUser::get()?;
    let trusted = trusted_os_sids()?;
    let mut chain = Vec::new();
    let mut cursor = PathBuf::new();
    for part in path.components() {
        cursor.push(part);
        if matches!(part, Component::Prefix(_)) {
            continue;
        }
        let file = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&cursor)?;
        let m = file.metadata()?;
        if !m.is_dir() || m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(denied("private storage cannot traverse a reparse point"));
        }
        unsafe { SecurityInfo::get(file.as_raw_handle()) }?.validate(
            &user,
            private_leaf && cursor == path,
            &trusted,
            private_leaf && cursor == path,
        )?;
        chain.push(file);
    }
    if chain.is_empty() {
        return Err(invalid("empty private storage chain"));
    }
    Ok(chain)
}
pub(super) fn create_directory(path: &Path) -> io::Result<()> {
    let security = UserSecurity::new()?;
    let path = wide(path.as_os_str())?;
    bool_result(unsafe { CreateDirectoryW(path.as_ptr(), &security.attributes()) })
}
pub(super) fn replace(from: &Path, to: &Path) -> io::Result<()> {
    let from = wide(from.as_os_str())?;
    let to = wide(to.as_os_str())?;
    bool_result(unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    })
}

/// Delete only the file currently named by `path` when its kernel identity
/// matches `expected`. Retain a no-share-delete candidate handle through the
/// comparison and handle-based deletion; a replacement file is never removed.
pub fn remove_if_same(path: &Path, expected: &File) -> io::Result<bool> {
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, DELETE, FILE_DISPOSITION_INFO, FILE_READ_ATTRIBUTES,
        FileDispositionInfo, GetFileInformationByHandle, SetFileInformationByHandle,
    };
    let candidate = match OpenOptions::new()
        .access_mode(DELETE | FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let identity = |file: &File| -> io::Result<(u32, u32, u32)> {
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        bool_result(unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) })?;
        if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 || !file.metadata()?.is_file()
        {
            return Err(denied(
                "temporary cleanup cannot target a reparse point or directory",
            ));
        }
        Ok((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    };
    if identity(&candidate)? != identity(expected)? {
        return Ok(false);
    }
    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    bool_result(unsafe {
        SetFileInformationByHandle(
            candidate.as_raw_handle(),
            FileDispositionInfo,
            (&disposition as *const FILE_DISPOSITION_INFO).cast(),
            size_of::<FILE_DISPOSITION_INFO>() as u32,
        )
    })?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::Security::Authorization::SetNamedSecurityInfoW;

    #[test]
    fn another_principals_read_grant_and_null_dacl_are_not_private() {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("private");
        let private = PrivateDir::create(&path).unwrap();
        let restore = UserSecurity::new().unwrap();
        let user = CurrentUser::get().unwrap();
        let world = Sid::well_known(WinWorldSid).unwrap();
        let mut storage = vec![0u32; 256];
        let acl = storage.as_mut_ptr().cast::<ACL>();
        bool_result(unsafe { InitializeAcl(acl, (storage.len() * 4) as u32, ACL_REVISION) })
            .unwrap();
        bool_result(unsafe {
            AddAccessAllowedAceEx(acl, ACL_REVISION, 0, FILE_ALL_ACCESS, user.sid.as_ptr())
        })
        .unwrap();
        bool_result(unsafe {
            AddAccessAllowedAceEx(acl, ACL_REVISION, 0, 0x0002_0000, world.as_ptr())
        })
        .unwrap();
        let mut name = wide(path.as_os_str()).unwrap();
        let set = |acl| {
            let code = unsafe {
                SetNamedSecurityInfoW(
                    name.as_mut_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    acl,
                    null_mut(),
                )
            };
            assert_eq!(code, 0);
        };
        let mut set = set;
        set(acl);
        assert!(PrivateDir::open_existing(&path).is_err());
        set(null_mut());
        assert!(PrivateDir::open_existing(&path).is_err());
        set(restore._acl.as_ptr().cast_mut().cast());
        assert!(PrivateDir::open_existing(&path).is_ok());
        drop(private);
    }

    #[test]
    fn inherited_everyone_grant_is_rejected_before_child_publication() {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("private");
        let private = PrivateDir::create(&path).unwrap();
        let restore = UserSecurity::new().unwrap();
        let user = CurrentUser::get().unwrap();
        let world = Sid::well_known(WinWorldSid).unwrap();
        let mut storage = vec![0u32; 256];
        let acl = storage.as_mut_ptr().cast::<ACL>();
        bool_result(unsafe { InitializeAcl(acl, (storage.len() * 4) as u32, ACL_REVISION) })
            .unwrap();
        bool_result(unsafe {
            AddAccessAllowedAceEx(acl, ACL_REVISION, 0, FILE_ALL_ACCESS, user.sid.as_ptr())
        })
        .unwrap();
        bool_result(unsafe {
            AddAccessAllowedAceEx(
                acl,
                ACL_REVISION,
                OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | INHERIT_ONLY_ACE,
                0x1000_0000,
                world.as_ptr(),
            )
        })
        .unwrap();
        let mut name = wide(path.as_os_str()).unwrap();
        let mut set = |acl| {
            let result = unsafe {
                SetNamedSecurityInfoW(
                    name.as_mut_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    acl,
                    null_mut(),
                )
            };
            assert_eq!(result, 0);
        };
        set(acl);
        drop(private);
        // Exercise the public constructor and publication workflow, not just
        // the ACL helper. Before the fix this wrote an Everyone-readable child.
        let result = PrivateDir::open_existing(&path)
            .and_then(|directory| directory.replace("state.json", b"private state"));
        assert!(result.is_err());
        assert!(PrivateDir::create(&path).is_err());
        assert!(!path.join("state.json").exists());
        assert!(std::fs::read_dir(&path).unwrap().next().is_none());
        set(restore._acl.as_ptr().cast_mut().cast());
        assert!(PrivateDir::open_existing(&path).is_ok());
    }

    #[test]
    fn cleanup_compares_kernel_identity_and_does_not_delete_replacement() {
        let parent = tempfile::tempdir().unwrap();
        let private = PrivateDir::create(parent.path().join("private")).unwrap();
        let path = private.path().join("temporary");
        let original = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        std::fs::rename(&path, private.path().join("old")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(!remove_if_same(&path, &original).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        assert!(remove_if_same(&private.path().join("old"), &original).unwrap());
        drop(original);
        assert!(!private.path().join("old").exists());
    }
}

pub(super) fn open_private_lock(path: &Path) -> io::Result<File> {
    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        Storage::FileSystem::{CreateFileW, FILE_GENERIC_READ, FILE_GENERIC_WRITE, OPEN_ALWAYS},
    };
    let security = UserSecurity::new()?;
    let path = wide(path.as_os_str())?;
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &security.attributes(),
            OPEN_ALWAYS,
            FILE_FLAG_OPEN_REPARSE_POINT,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Read-only ownership of a trusted, non-reparse directory chain. Unlike a
/// `PrivateDir`, its final directory need not be owner-private. It exposes no
/// file access or creation operations; native providers use it only to retain
/// trusted ancestors while validating a specially tagged leaf separately.
pub struct TrustedDirectory {
    _chain: Vec<File>,
}
impl TrustedDirectory {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        normalized(path)?;
        Ok(Self {
            _chain: open_chain(path, false)?,
        })
    }
}
