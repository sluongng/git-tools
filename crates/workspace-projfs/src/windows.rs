//! Native Windows callbacks. All callbacks are synchronous and unwind-contained.
use crate::{Entry, Namespace};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{File, OpenOptions},
    os::windows::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    ptr,
    sync::Mutex,
};
use windows_sys::{
    Win32::{
        Foundation::*,
        Storage::{FileSystem::*, ProjectedFileSystem::*},
        System::Com::CoCreateGuid,
    },
    core::{GUID, HRESULT},
};
use workspace_core::{
    backend::{BackgroundCancellation, GitRepo, SizeFallback, SizeWarmOptions},
    filesystem::{Kind, Snapshot},
};

fn wide(value: impl AsRef<OsStr>) -> Vec<u16> {
    value.as_ref().encode_wide().chain(Some(0)).collect()
}
fn hr(code: u32) -> HRESULT {
    if code == 0 {
        0
    } else {
        (code & 0xffff | 0x80070000) as i32
    }
}
fn check(result: HRESULT, operation: &str) -> Result<()> {
    ensure!(result >= 0, "{operation}: HRESULT {:#010x}", result as u32);
    Ok(())
}
unsafe fn string(pointer: *const u16) -> String {
    if pointer.is_null() {
        return String::new();
    }
    let mut len = 0;
    while unsafe { *pointer.add(len) } != 0 {
        len += 1;
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(pointer, len) })
}
fn guid_key(guid: &GUID) -> [u8; 16] {
    let mut key = [0; 16];
    key[..4].copy_from_slice(&guid.data1.to_le_bytes());
    key[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    key[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    key[8..].copy_from_slice(&guid.data4);
    key
}
fn guid(bytes: [u8; 16]) -> GUID {
    GUID {
        data1: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        data2: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
        data3: u16::from_le_bytes(bytes[6..8].try_into().unwrap()),
        data4: bytes[8..].try_into().unwrap(),
    }
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    root: PathBuf,
    repository: PathBuf,
    commit: String,
    instance: [u8; 16],
}
struct Enumeration {
    entries: Vec<Entry>,
    next: usize,
    expression: Option<Vec<u16>>,
}
struct Provider {
    namespace: Namespace,
    repo: Mutex<GitRepo>,
    enumerations: Mutex<HashMap<[u8; 16], Enumeration>>,
    timestamp: i64,
    lookup_order: Vec<(Vec<u16>, usize)>,
    children: HashMap<u64, Vec<usize>>,
}

impl Provider {
    // ProjFS uses its own Windows collation, not Rust's Unicode case fold.
    fn lookup(&self, path: &str) -> Option<&Entry> {
        let wanted = wide(path);
        self.lookup_order
            .binary_search_by(|(name, _)| {
                unsafe { PrjFileNameCompare(name.as_ptr(), wanted.as_ptr()) }.cmp(&0)
            })
            .ok()
            .map(|index| &self.namespace.entries[self.lookup_order[index].1])
    }
    fn symlink_target(&self, entry: &Entry) -> Result<String> {
        let bytes = self
            .repo
            .lock()
            .map_err(|_| anyhow::anyhow!("repository lock poisoned"))?
            .read_blob(entry.oid.as_deref().context("symlink without object ID")?)?;
        let target = String::from_utf8(bytes).context("symlink target must be UTF-8 on Windows")?;
        ensure!(
            !target.is_empty() && !target.starts_with('/') && !target.contains(['\0', '\\', ':']),
            "only representable relative Windows symlinks are supported"
        );
        Ok(target)
    }
    fn symlink_info(&self, entry: &Entry) -> Result<(Vec<u16>, bool)> {
        let original = self.symlink_target(entry)?;
        let mut current = entry;
        let mut visited = std::collections::HashSet::new();
        while current.kind == Kind::Symlink {
            ensure!(
                visited.insert(current.inode) && visited.len() <= 40,
                "cyclic or over-depth symlink"
            );
            let target = self.symlink_target(current)?;
            let mut parts: Vec<_> = current
                .parent
                .split('\\')
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect();
            let components: Vec<_> = target.split('/').filter(|part| !part.is_empty()).collect();
            for (index, part) in components.iter().copied().enumerate() {
                match part {
                    "" | "." => {}
                    ".." => {
                        ensure!(parts.pop().is_some(), "symlink leaves projected snapshot");
                    }
                    part => {
                        crate::validate_component(part)?;
                        parts.push(part.to_owned());
                        if index + 1 < components.len() {
                            let intermediate = self
                                .lookup(&parts.join("\\"))
                                .context("missing symlink target component")?;
                            ensure!(
                                intermediate.kind.is_dir(),
                                "symlink targets through non-directory or intermediate-symlink components are unsupported"
                            );
                        }
                    }
                }
            }
            if parts.is_empty() {
                return Ok((wide(original.replace('/', "\\")), true));
            }
            current = self
                .lookup(&parts.join("\\"))
                .context("dangling or external symlink target is unsupported")?;
            if target.ends_with('/') {
                ensure!(
                    current.kind.is_dir(),
                    "trailing slash requires a direct directory target"
                );
            }
        }
        Ok((wide(original.replace('/', "\\")), current.kind.is_dir()))
    }
    fn info(&self, entry: &Entry) -> Result<PRJ_FILE_BASIC_INFO> {
        let size = self.namespace.size(
            entry,
            &mut *self
                .repo
                .lock()
                .map_err(|_| anyhow::anyhow!("repository lock poisoned"))?,
        )?;
        let directory = if entry.kind == Kind::Symlink {
            self.symlink_info(entry)?.1
        } else {
            entry.kind.is_dir()
        };
        Ok(PRJ_FILE_BASIC_INFO {
            IsDirectory: directory,
            FileSize: i64::try_from(size)?,
            CreationTime: self.timestamp,
            LastAccessTime: self.timestamp,
            LastWriteTime: self.timestamp,
            ChangeTime: self.timestamp,
            FileAttributes: if directory {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_ARCHIVE
            } | if entry.kind == Kind::Symlink {
                FILE_ATTRIBUTE_REPARSE_POINT
            } else {
                0
            },
        })
    }
}

/// Running provider. Drop stops callbacks before freeing their context.
pub struct Projection {
    context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    _provider: Box<Provider>,
    _lock: File,
    cancellation: BackgroundCancellation,
}
impl Drop for Projection {
    fn drop(&mut self) {
        self.cancellation.cancel();
        unsafe { PrjStopVirtualizing(self.context) };
    }
}

/// Start (or resume) a fixed snapshot. State must live outside the virtualization
/// root. A matching manifest is mandatory before accepting a nonempty root.
pub fn start(
    repository: &Path,
    revision: &str,
    root: &Path,
    state: &Path,
    git: &Path,
) -> Result<Projection> {
    start_with_size_options(
        repository,
        revision,
        root,
        state,
        git,
        &SizeWarmOptions::default(),
    )
}

pub fn start_with_size_options(
    repository: &Path,
    revision: &str,
    root: &Path,
    state: &Path,
    git: &Path,
    sizes: &SizeWarmOptions,
) -> Result<Projection> {
    std::fs::create_dir_all(state)?;
    std::fs::create_dir_all(root)?;
    let state = state.canonicalize()?;
    let root = root.canonicalize()?;
    let repository = repository.canonicalize()?;
    ensure!(
        !state.starts_with(&root) && !repository.starts_with(&root),
        "state and repository must be outside projection root"
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(state.join("provider.lock"))
        .context("another provider owns this state")?;
    let mut repo = GitRepo::open(&repository, git, "origin", SizeFallback::Error)?;
    let (_, cancellation) = repo.background_handle()?;
    let snapshot = Snapshot::load(&repo, revision)?;
    let namespace = Namespace::from_snapshot(&snapshot)?;
    // ProjFS enumeration requires exact lengths. Preseed metadata in one batch
    // (GitHub tree API for github.com), never hydrate file content for stat.
    repo.warm_snapshot_sizes(&snapshot, sizes)?;
    // Reject collisions according to actual Windows comparison as well.
    let mut names: Vec<_> = namespace
        .entries
        .iter()
        .map(|entry| wide(&entry.path))
        .collect();
    names.sort_by(|a, b| unsafe { PrjFileNameCompare(a.as_ptr(), b.as_ptr()) }.cmp(&0));
    ensure!(
        !names
            .windows(2)
            .any(|pair| unsafe { PrjFileNameCompare(pair[0].as_ptr(), pair[1].as_ptr()) } == 0),
        "Git tree has Windows-colliding names"
    );
    let manifest_path = state.join("instance.json");
    let manifest = if manifest_path.exists() {
        let manifest: Manifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        ensure!(
            manifest.version == 1
                && manifest.root == root
                && manifest.repository == repository
                && manifest.commit == snapshot.commit,
            "existing projection belongs to another root, repository or commit; use a new root and state (never discard local edits)"
        );
        manifest
    } else {
        ensure!(
            std::fs::symlink_metadata(&root)?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            "existing reparse-point root requires its original state directory"
        );
        ensure!(
            std::fs::read_dir(&root)?.next().is_none(),
            "new projection root must be empty"
        );
        let mut instance = GUID::default();
        check(unsafe { CoCreateGuid(&mut instance) }, "create instance ID")?;
        let manifest = Manifest {
            version: 1,
            root: root.clone(),
            repository,
            commit: snapshot.commit.clone(),
            instance: guid_key(&instance),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&manifest_path)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
        file.sync_all()?;
        manifest
    };
    // Marking an already marked root is unnecessary and may reject its contents.
    let root_name = wide(root.as_os_str());
    let mut root_state = 0;
    let state_result = unsafe { PrjGetOnDiskFileState(root_name.as_ptr(), &mut root_state) };
    if state_result < 0 || root_state & PRJ_FILE_STATE_PLACEHOLDER == 0 {
        check(
            unsafe {
                PrjMarkDirectoryAsPlaceholder(
                    root_name.as_ptr(),
                    ptr::null(),
                    ptr::null(),
                    &guid(manifest.instance),
                )
            },
            "mark ProjFS root (enable Client-ProjFS first)",
        )?;
    }
    let timestamp = snapshot
        .timestamp()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut lookup_order: Vec<_> = namespace
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (wide(&entry.path), index))
        .collect();
    lookup_order.sort_by(|a, b| unsafe { PrjFileNameCompare(a.0.as_ptr(), b.0.as_ptr()) }.cmp(&0));
    let mut children: HashMap<u64, Vec<usize>> = HashMap::new();
    for (index, entry) in namespace.entries.iter().enumerate() {
        children.entry(entry.parent_inode).or_default().push(index);
    }
    let provider = Box::new(Provider {
        lookup_order,
        children,
        namespace,
        repo: Mutex::new(repo),
        enumerations: Mutex::new(HashMap::new()),
        timestamp: i64::try_from(timestamp)?
            .saturating_add(11_644_473_600)
            .saturating_mul(10_000_000),
    });
    let callbacks = PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(start_enumeration),
        EndDirectoryEnumerationCallback: Some(end_enumeration),
        GetDirectoryEnumerationCallback: Some(enumerate),
        GetPlaceholderInfoCallback: Some(placeholder),
        GetFileDataCallback: Some(hydrate),
        QueryFileNameCallback: Some(query),
        NotificationCallback: Some(notification),
        CancelCommandCallback: None,
    };
    let empty = wide("");
    let mut mapping = PRJ_NOTIFICATION_MAPPING {
        NotificationBitMask: PRJ_NOTIFY_PRE_RENAME | PRJ_NOTIFY_PRE_SET_HARDLINK,
        NotificationRoot: empty.as_ptr(),
    };
    let options = PRJ_STARTVIRTUALIZING_OPTIONS {
        NotificationMappings: &mut mapping,
        NotificationMappingsCount: 1,
        ..Default::default()
    };
    let mut context = ptr::null_mut();
    check(
        unsafe {
            PrjStartVirtualizing(
                root_name.as_ptr(),
                &callbacks,
                (&*provider as *const Provider).cast(),
                &options,
                &mut context,
            )
        },
        "start ProjFS virtualization",
    )?;
    let projection = Projection {
        context,
        _provider: provider,
        _lock: lock,
        cancellation,
    };
    crate::worktree::prepare(&manifest.repository, &root, &state, &manifest.commit, git)?;
    Ok(projection)
}

// Do not unwind through system ABI. Context remains alive until StopVirtualizing
// has drained callbacks; immutable namespace plus mutexes permit concurrent calls.
unsafe fn callback(
    data: *const PRJ_CALLBACK_DATA,
    body: impl FnOnce(&PRJ_CALLBACK_DATA, &Provider) -> Result<HRESULT>,
) -> HRESULT {
    if data.is_null() {
        return E_INVALIDARG;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let data = unsafe { &*data };
        if data.InstanceContext.is_null() {
            return Ok(E_INVALIDARG);
        }
        let provider = unsafe { &*data.InstanceContext.cast::<Provider>() };
        body(data, provider)
    })) {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            use std::io::Write;
            let _ = writeln!(std::io::stderr(), "git-projfs callback failed: {error:#}");
            E_FAIL
        }
        Err(_) => E_UNEXPECTED,
    }
}
unsafe extern "system" fn start_enumeration(
    data: *const PRJ_CALLBACK_DATA,
    id: *const GUID,
) -> HRESULT {
    unsafe {
        callback(data, |data, provider| {
            if id.is_null() {
                return Ok(E_INVALIDARG);
            }
            let path = string(data.FilePathName);
            let parent = if path.is_empty() {
                1
            } else {
                match provider.lookup(&path) {
                    Some(entry) if entry.kind.is_dir() => entry.inode,
                    _ => return Ok(hr(ERROR_FILE_NOT_FOUND)),
                }
            };
            let mut entries: Vec<_> = provider
                .children
                .get(&parent)
                .into_iter()
                .flatten()
                .map(|&index| provider.namespace.entries[index].clone())
                .collect();
            entries.sort_by(|a, b| {
                PrjFileNameCompare(wide(&a.name).as_ptr(), wide(&b.name).as_ptr()).cmp(&0)
            });
            provider
                .enumerations
                .lock()
                .map_err(|_| anyhow::anyhow!("enumeration lock poisoned"))?
                .insert(
                    guid_key(&*id),
                    Enumeration {
                        entries,
                        next: 0,
                        expression: None,
                    },
                );
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn end_enumeration(
    data: *const PRJ_CALLBACK_DATA,
    id: *const GUID,
) -> HRESULT {
    unsafe {
        callback(data, |_, provider| {
            if id.is_null() {
                return Ok(E_INVALIDARG);
            }
            provider
                .enumerations
                .lock()
                .map_err(|_| anyhow::anyhow!("enumeration lock poisoned"))?
                .remove(&guid_key(&*id));
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn enumerate(
    data: *const PRJ_CALLBACK_DATA,
    id: *const GUID,
    expression: *const u16,
    buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    unsafe {
        callback(data, |data, provider| {
            if id.is_null() {
                return Ok(E_INVALIDARG);
            }
            let mut enumerations = provider
                .enumerations
                .lock()
                .map_err(|_| anyhow::anyhow!("enumeration lock poisoned"))?;
            let Some(enumeration) = enumerations.get_mut(&guid_key(&*id)) else {
                return Ok(E_INVALIDARG);
            };
            if data.Flags & PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN != 0 {
                enumeration.next = 0;
                enumeration.expression = None;
            }
            if enumeration.expression.is_none() {
                enumeration.expression = Some(wide(if expression.is_null() {
                    "*".to_owned()
                } else {
                    string(expression)
                }));
            }
            let mut emitted = 0;
            while let Some(entry) = enumeration.entries.get(enumeration.next) {
                let name = wide(&entry.name);
                if !PrjFileNameMatch(
                    name.as_ptr(),
                    enumeration.expression.as_ref().unwrap().as_ptr(),
                ) {
                    enumeration.next += 1;
                    continue;
                }
                let info = provider.info(entry)?;
                let result = if entry.kind == Kind::Symlink {
                    let (target, _) = provider.symlink_info(entry)?;
                    let extra = PRJ_EXTENDED_INFO {
                        InfoType: PRJ_EXT_INFO_TYPE_SYMLINK,
                        Anonymous: PRJ_EXTENDED_INFO_0 {
                            Symlink: PRJ_EXTENDED_INFO_0_0 {
                                TargetName: target.as_ptr(),
                            },
                        },
                        ..Default::default()
                    };
                    PrjFillDirEntryBuffer2(buffer, name.as_ptr(), &info, &extra)
                } else {
                    PrjFillDirEntryBuffer(name.as_ptr(), &info, buffer)
                };
                // Keep the cursor on this entry so it is returned on the next call.
                if result == hr(ERROR_INSUFFICIENT_BUFFER) {
                    return Ok(if emitted == 0 { result } else { S_OK });
                }
                if result < 0 {
                    return Ok(result);
                }
                enumeration.next += 1;
                emitted += 1;
                if data.Flags & PRJ_CB_DATA_FLAG_ENUM_RETURN_SINGLE_ENTRY != 0 {
                    break;
                }
            }
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn query(data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    unsafe {
        callback(data, |data, provider| {
            Ok(if provider.lookup(&string(data.FilePathName)).is_some() {
                S_OK
            } else {
                hr(ERROR_FILE_NOT_FOUND)
            })
        })
    }
}
unsafe extern "system" fn placeholder(data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    unsafe {
        callback(data, |data, provider| {
            let Some(entry) = provider.lookup(&string(data.FilePathName)) else {
                return Ok(hr(ERROR_FILE_NOT_FOUND));
            };
            let mut info = PRJ_PLACEHOLDER_INFO {
                FileBasicInfo: provider.info(entry)?,
                ..Default::default()
            };
            info.VersionInfo.ProviderID[..12].copy_from_slice(b"git-projfs-1");
            if let Some(oid) = &entry.oid {
                info.VersionInfo.ContentID[..oid.len()].copy_from_slice(oid.as_bytes());
            }
            let name = wide(&entry.path);
            if entry.kind == Kind::Symlink {
                let (target, _) = provider.symlink_info(entry)?;
                let extra = PRJ_EXTENDED_INFO {
                    InfoType: PRJ_EXT_INFO_TYPE_SYMLINK,
                    Anonymous: PRJ_EXTENDED_INFO_0 {
                        Symlink: PRJ_EXTENDED_INFO_0_0 {
                            TargetName: target.as_ptr(),
                        },
                    },
                    ..Default::default()
                };
                Ok(PrjWritePlaceholderInfo2(
                    data.NamespaceVirtualizationContext,
                    name.as_ptr(),
                    &info,
                    std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
                    &extra,
                ))
            } else {
                Ok(PrjWritePlaceholderInfo(
                    data.NamespaceVirtualizationContext,
                    name.as_ptr(),
                    &info,
                    std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
                ))
            }
        })
    }
}
struct Aligned(*mut std::ffi::c_void);
impl Drop for Aligned {
    fn drop(&mut self) {
        unsafe { PrjFreeAlignedBuffer(self.0) }
    }
}
unsafe extern "system" fn hydrate(
    data: *const PRJ_CALLBACK_DATA,
    offset: u64,
    length: u32,
) -> HRESULT {
    unsafe {
        callback(data, |data, provider| {
            if length == 0 {
                return Ok(S_OK);
            }
            let Some(entry) = provider.lookup(&string(data.FilePathName)) else {
                return Ok(hr(ERROR_FILE_NOT_FOUND));
            };
            if entry.kind != Kind::File {
                return Ok(hr(ERROR_INVALID_PARAMETER));
            }
            // DataStreamId and ContentID identify the original blob after a rename.
            let oid = if !data.VersionInfo.is_null() {
                let bytes = &(&*data.VersionInfo).ContentID[..40];
                let oid = std::str::from_utf8(bytes)?;
                ensure!(
                    oid.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "invalid placeholder content identity"
                );
                oid
            } else {
                entry.oid.as_deref().context("file without object ID")?
            };
            let bytes = provider
                .repo
                .lock()
                .map_err(|_| anyhow::anyhow!("repository lock poisoned"))?
                .read_blob(oid)?;
            let mut instance = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
            let result = PrjGetVirtualizationInstanceInfo(
                data.NamespaceVirtualizationContext,
                &mut instance,
            );
            if result < 0 {
                return Ok(result);
            }
            let (mut start, end) = crate::hydration_range(
                offset,
                length,
                bytes.len() as u64,
                instance.WriteAlignment,
            )?;
            let alignment = instance.WriteAlignment as u64;
            let capacity = (1024 * 1024u64 / alignment).max(1) * alignment;
            let capacity = capacity.min(u32::MAX as u64 / alignment * alignment);
            ensure!(capacity > 0, "unsupported hydration alignment");
            let buffer = Aligned(PrjAllocateAlignedBuffer(
                data.NamespaceVirtualizationContext,
                usize::try_from(capacity.min(end - start))?,
            ));
            if buffer.0.is_null() {
                return Ok(E_OUTOFMEMORY);
            }
            while start < end {
                let length = (end - start).min(capacity) as u32;
                ptr::copy_nonoverlapping(
                    bytes[usize::try_from(start)?..usize::try_from(start + length as u64)?]
                        .as_ptr(),
                    buffer.0.cast(),
                    length as usize,
                );
                let result = PrjWriteFileData(
                    data.NamespaceVirtualizationContext,
                    &data.DataStreamId,
                    buffer.0,
                    start,
                    length,
                );
                if result < 0 {
                    return Ok(result);
                }
                start += length as u64;
            }
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn notification(
    data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    _destination: *const u16,
    _parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    unsafe {
        callback(data, |_, _| {
            // Directory renames require view rebasing; reject instead of losing
            // unmaterialized descendants. File renames preserve the original ID.
            if notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
                || (notification == PRJ_NOTIFICATION_PRE_RENAME && is_directory)
            {
                Ok(hr(ERROR_NOT_SUPPORTED))
            } else {
                Ok(S_OK)
            }
        })
    }
}
