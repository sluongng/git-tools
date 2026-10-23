//! Native source capture, separate from output staging. Directory oplocks are
//! advisory change detectors, not locks: any break invalidates the whole capture.
//! https://learn.microsoft.com/windows/win32/api/winioctl/ni-winioctl-fsctl_request_oplock
use super::*;
use output_core::{
    SourceFrontendFence, SourceLimits, SourceReceipt, SourceSnapshot, SourceVersion,
};
use std::{
    cell::UnsafeCell,
    io,
    mem::size_of,
    os::windows::io::{FromRawHandle, OwnedHandle},
    time::{Duration, Instant},
};
use windows_sys::Win32::System::{
    IO::{CancelIoEx, DeviceIoControl, GetOverlappedResult, OVERLAPPED},
    Ioctl::{
        FSCTL_REQUEST_OPLOCK, OPLOCK_LEVEL_CACHE_READ, REQUEST_OPLOCK_CURRENT_VERSION,
        REQUEST_OPLOCK_INPUT_BUFFER, REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
        REQUEST_OPLOCK_OUTPUT_BUFFER,
    },
    Threading::CreateEventW,
};

#[derive(Clone, Debug)]
pub struct NativeCaptureLimits {
    pub source: SourceLimits,
    pub max_kernel_handles: usize,
    pub timeout: Duration,
}
impl Default for NativeCaptureLimits {
    fn default() -> Self {
        Self {
            source: SourceLimits::default(),
            max_kernel_handles: 65_536,
            timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeCaptureError {
    Busy,
    Unsupported,
    Limit,
}
impl std::fmt::Display for NativeCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Busy => "native source changed or has active writers; retry capture",
            Self::Unsupported => {
                "native source cannot be captured with the required filesystem guarantees"
            }
            Self::Limit => "native source capture exceeds configured limits",
        })
    }
}
impl std::error::Error for NativeCaptureError {}
fn busy() -> anyhow::Error {
    NativeCaptureError::Busy.into()
}
fn unsupported() -> anyhow::Error {
    NativeCaptureError::Unsupported.into()
}
fn limited() -> anyhow::Error {
    NativeCaptureError::Limit.into()
}
fn native_error(error: io::Error) -> anyhow::Error {
    match error.raw_os_error().map(|n| n as u32) {
        Some(
            ERROR_SHARING_VIOLATION
            | ERROR_LOCK_VIOLATION
            | ERROR_FILE_NOT_FOUND
            | ERROR_PATH_NOT_FOUND,
        ) => busy(),
        Some(
            ERROR_NOT_SUPPORTED
            | ERROR_INVALID_FUNCTION
            | ERROR_CANNOT_GRANT_REQUESTED_OPLOCK
            | ERROR_OPLOCK_NOT_GRANTED
            | ERROR_INVALID_PARAMETER,
        ) => unsupported(),
        _ => error.into(),
    }
}
/// File contents are not read for a trusted unchanged placeholder. New, renamed
/// or modified files use a private immutable captured copy, not a stale locator.
pub enum NativeSourceContent {
    Directory,
    Remote(Artifact),
    Local(Arc<CapturedSourceFile>),
}
pub struct NativeSourceEntry {
    pub path: String,
    /// Existing executable bits are retained from the native base. Windows has
    /// no executable-bit mutation API; new regular files use non-executable mode.
    pub mode: u32,
    pub content: NativeSourceContent,
}
pub struct CapturedSourceFile {
    file: File,
    path: PathBuf,
    directory: Arc<private_fs::PrivateDir>,
    size: u64,
    hash: [u8; 32],
}
impl CapturedSourceFile {
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn blake3(&self) -> [u8; 32] {
        self.hash
    }
    pub fn open(self: &Arc<Self>) -> Result<CapturedSourceReader> {
        let handle = unsafe {
            ReOpenFile(
                self.file.as_raw_handle(),
                FILE_GENERIC_READ,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                0,
            )
        };
        ensure!(
            handle != INVALID_HANDLE_VALUE,
            "cannot reopen captured source file"
        );
        let mut file = unsafe { File::from_raw_handle(handle) };
        file.seek(SeekFrom::Start(0))?;
        Ok(CapturedSourceReader {
            file,
            _owner: self.clone(),
        })
    }
}
impl Drop for CapturedSourceFile {
    fn drop(&mut self) {
        let _ = &self.directory;
        let _ = private_fs::windows::remove_if_same(&self.path, &self.file);
    }
}
pub struct CapturedSourceReader {
    file: File,
    _owner: Arc<CapturedSourceFile>,
}
impl Read for CapturedSourceReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.file.read(bytes)
    }
}
impl Seek for CapturedSourceReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}
struct CaptureOwner {
    mount: Arc<ProjfsMount>,
}
impl Drop for CaptureOwner {
    fn drop(&mut self) {
        self.mount
            .provider
            .source_captures
            .fetch_sub(1, Ordering::Release);
    }
}
/// A native NTFS inventory and owned private payload copies. Retains all kernel
/// fences until dropped. Do not replace this inventory with Tree's inventory.
pub struct NativeSourceCapture {
    entries: Vec<NativeSourceEntry>,
    fences: Vec<Oplock>,
    _readers: Vec<File>,
    _root: SourceRootGuard,
    core: SourceSnapshot,
    owner: Arc<CaptureOwner>,
    rename_epoch: u64,
    expires: Instant,
}
impl NativeSourceCapture {
    pub fn entries(&self) -> &[NativeSourceEntry] {
        &self.entries
    }
    pub fn version(&self) -> &SourceVersion {
        self.core.version()
    }
    pub fn base_root(&self) -> [u8; 32] {
        self.core.base_root()
    }
    pub fn validate_still_current(&self) -> Result<()> {
        if Instant::now() >= self.expires {
            return Err(limited());
        }
        let provider = &self.owner.mount.provider;
        provider
            .source_control
            .as_ref()
            .context("native source marker is unavailable")?
            .verify()?;
        if provider.stopping.load(Ordering::Acquire)
            || provider.draining.load(Ordering::Acquire)
            || provider.distrust.lock().unwrap().serial != self.rename_epoch
        {
            return Err(busy());
        }
        for fence in &self.fences {
            fence.validate()?;
        }
        Ok(())
    }
    /// Confirm exactly the uploaded NTFS inventory. A caller must have uploaded
    /// Local contents and constructed the native root from `entries()`, not from
    /// Tree-only records. The wrapper never exposes the unfenced core snapshot.
    pub fn acknowledge_root(
        self,
        root: [u8; 32],
        expected: SourceVersion,
        request_id: String,
        limits: &SourceLimits,
    ) -> Result<SourceReceipt> {
        self.validate_still_current()?;
        let Self {
            entries,
            fences,
            _readers: readers,
            _root: root_guard,
            core,
            owner,
            ..
        } = self;
        // This aggregate also preserves drop order during unwinding: no
        // shutdown/counter release may precede draining the kernel fences.
        struct Guards {
            _entries: Vec<NativeSourceEntry>,
            _fences: Vec<Oplock>,
            _readers: Vec<File>,
            _root: SourceRootGuard,
            _owner: Arc<CaptureOwner>,
        }
        let guards = Guards {
            _entries: entries,
            _fences: fences,
            _readers: readers,
            _root: root_guard,
            _owner: owner,
        };
        let result = core.acknowledge_root(root, expected, &request_id, limits);
        drop(guards);
        result
    }
}

pub(super) struct Oplock {
    file: File,
    _input: Box<REQUEST_OPLOCK_INPUT_BUFFER>,
    output: Box<UnsafeCell<REQUEST_OPLOCK_OUTPUT_BUFFER>>,
    overlapped: Box<UnsafeCell<OVERLAPPED>>,
    _event: OwnedHandle,
}
// All native I/O buffers have stable heap addresses. Kernel-mutated structures
// live in UnsafeCell; Rust never reads them directly after submission. Checking
// and cancellation use the OS synchronization APIs. Drop drains before freeing.
unsafe impl Send for Oplock {}
unsafe impl Sync for Oplock {}
impl Oplock {
    pub(super) fn open(path: &Path, directory: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .share_mode(if directory {
                FILE_SHARE_READ | FILE_SHARE_WRITE
            } else {
                FILE_SHARE_READ
            })
            .custom_flags(
                FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
            )
            .open(path)
            .map_err(native_error)?;
        let metadata = file.metadata()?;
        if metadata.is_dir() != directory {
            return Err(busy());
        }
        let tag = tag(&file)?;
        if tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
            && tag.ReparseTag != windows_sys::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS
        {
            return Err(unsupported());
        }
        let event = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error().into());
        }
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        let overlapped = Box::new(UnsafeCell::new(OVERLAPPED {
            hEvent: event.as_raw_handle(),
            ..Default::default()
        }));
        let input = Box::new(REQUEST_OPLOCK_INPUT_BUFFER {
            StructureVersion: REQUEST_OPLOCK_CURRENT_VERSION as u16,
            StructureLength: size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u16,
            RequestedOplockLevel: OPLOCK_LEVEL_CACHE_READ,
            Flags: REQUEST_OPLOCK_INPUT_FLAG_REQUEST,
        });
        let output = Box::new(UnsafeCell::new(REQUEST_OPLOCK_OUTPUT_BUFFER::default()));
        let result = unsafe {
            DeviceIoControl(
                file.as_raw_handle(),
                FSCTL_REQUEST_OPLOCK,
                (&*input as *const REQUEST_OPLOCK_INPUT_BUFFER).cast(),
                size_of::<REQUEST_OPLOCK_INPUT_BUFFER>() as u32,
                output.get().cast(),
                size_of::<REQUEST_OPLOCK_OUTPUT_BUFFER>() as u32,
                ptr::null_mut(),
                overlapped.get(),
            )
        };
        if result != 0 {
            return Err(busy());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_PENDING as i32) {
            return Err(native_error(error));
        }
        let lock = Self {
            file,
            _input: input,
            output,
            overlapped,
            _event: event,
        };
        lock.validate()?;
        Ok(lock)
    }
    pub(super) fn validate(&self) -> Result<()> {
        let mut bytes = 0;
        let done = unsafe {
            GetOverlappedResult(
                self.file.as_raw_handle(),
                self.overlapped.get(),
                &mut bytes,
                0,
            )
        };
        if done == 0
            && io::Error::last_os_error().raw_os_error() == Some(ERROR_IO_INCOMPLETE as i32)
        {
            Ok(())
        } else {
            Err(busy())
        }
    }
}
impl Drop for Oplock {
    fn drop(&mut self) {
        let _ = &self.output;
        unsafe {
            CancelIoEx(self.file.as_raw_handle(), self.overlapped.get());
            let mut bytes = 0;
            GetOverlappedResult(
                self.file.as_raw_handle(),
                self.overlapped.get(),
                &mut bytes,
                1,
            );
        }
    }
}
pub(super) fn tag(file: &File) -> Result<FILE_ATTRIBUTE_TAG_INFO> {
    let mut tag = FILE_ATTRIBUTE_TAG_INFO::default();
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileAttributeTagInfo,
            (&mut tag as *mut FILE_ATTRIBUTE_TAG_INFO).cast(),
            size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(tag)
}
struct Inventory {
    provider: Arc<Provider>,
    directory: Arc<private_fs::PrivateDir>,
    limits: NativeCaptureLimits,
    deadline: Instant,
    entries: Vec<NativeSourceEntry>,
    fences: Vec<Oplock>,
    readers: Vec<File>,
    path_bytes: usize,
    dirty_bytes: u64,
    discovered: usize,
}
impl Inventory {
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(limited());
        }
        if self.provider.stopping.load(Ordering::Acquire)
            || self.provider.draining.load(Ordering::Acquire)
        {
            return Err(busy());
        }
        Ok(())
    }
    fn account(&mut self, path: &str) -> Result<()> {
        self.check()?;
        if self.discovered >= self.limits.source.max_entries
            || path.len() > self.limits.source.max_path_bytes
            || path.split('/').count() > self.limits.source.max_depth
        {
            return Err(limited());
        }
        self.discovered += 1;
        self.path_bytes = self
            .path_bytes
            .checked_add(path.len() + 256)
            .ok_or_else(limited)?;
        if self.path_bytes > self.limits.source.max_metadata_bytes {
            return Err(limited());
        }
        Ok(())
    }
    fn fence(&mut self, path: &Path, directory: bool) -> Result<usize> {
        if self.fences.len().saturating_mul(2) + self.readers.len().saturating_mul(2) + 2
            > self.limits.max_kernel_handles
        {
            return Err(limited());
        }
        let fence = Oplock::open(path, directory)?;
        let index = self.fences.len();
        self.fences.push(fence);
        Ok(index)
    }
    fn validate(&self, ancestors: &[usize]) -> Result<()> {
        self.check()?;
        for index in ancestors {
            self.fences[*index].validate()?;
        }
        Ok(())
    }
    fn walk(&mut self, relative: &str, ancestors: &mut Vec<usize>) -> Result<()> {
        self.check()?;
        let absolute = self.provider.root.join(relative);
        let index = self.fence(&absolute, true)?;
        self.validate(ancestors)?;
        ancestors.push(index);
        // Charge the complete queued inventory before descending. Charging only
        // emitted entries would permit depth*max_entries pending-name memory.
        let mut children = Vec::new();
        for entry in std::fs::read_dir(&absolute).map_err(native_error)? {
            let name = entry?.file_name();
            let name = name.to_str().ok_or_else(unsupported)?.to_owned();
            if relative.is_empty() && name.eq_ignore_ascii_case(".jj") {
                continue;
            }
            crate::validate_component(&name)?;
            let path = if relative.is_empty() {
                name.clone()
            } else {
                format!("{relative}/{name}")
            };
            self.account(&path)?;
            children.push(name);
        }
        children.sort_by(|a, b| {
            unsafe { PrjFileNameCompare(wide(a).as_ptr(), wide(b).as_ptr()) }
                .cmp(&0)
                .then_with(|| a.as_bytes().cmp(b.as_bytes()))
        });
        let mut previous: Option<&String> = None;
        for name in &children {
            if let Some(old) = previous {
                ensure!(
                    unsafe { PrjFileNameCompare(wide(old).as_ptr(), wide(name).as_ptr()) } != 0,
                    "ambiguous native source names"
                );
            }
            previous = Some(name);
        }
        for name in children {
            let path = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            self.validate(ancestors)?;
            let absolute = self.provider.root.join(&path);
            let metadata = std::fs::symlink_metadata(&absolute).map_err(native_error)?;
            if metadata.is_dir() {
                self.entries.push(NativeSourceEntry {
                    path: path.clone(),
                    mode: 0o755,
                    content: NativeSourceContent::Directory,
                });
                self.walk(&path, ancestors)?;
            } else {
                if !metadata.is_file() {
                    return Err(unsupported());
                }
                let file_index = self.fence(&absolute, false)?;
                self.validate(ancestors)?;
                let file = &self.fences[file_index].file;
                let fingerprint = file_fingerprint(file)?;
                let state = self.provider.native_state(&path)?.ok_or_else(busy)?;
                if state & PRJ_FILE_STATE_TOMBSTONE != 0 {
                    return Err(busy());
                }
                let base = self.provider.lookup(&path).ok();
                let unchanged = base.as_ref().filter(|node| {
                    node.kind == NodeKind::File
                        && state & PRJ_FILE_STATE_FULL == 0
                        && !self.provider.is_distrusted(&path)
                        && node.remote.is_some()
                        && file.metadata().is_ok_and(|m| m.len() == node.size)
                        && self
                            .provider
                            .fingerprints
                            .lock()
                            .unwrap()
                            .records
                            .get(&node.id)
                            == Some(&fingerprint)
                });
                let mode = base
                    .as_ref()
                    .filter(|node| node.kind == NodeKind::File)
                    .map_or(0o644, |node| node.mode);
                let content = if let Some(node) = unchanged {
                    let artifact = node.remote.as_ref().unwrap();
                    // Tinytree file IDs are raw BLAKE3, never a digest-shaped
                    // SHA256 locator silently reinterpreted as a native ID.
                    if artifact.digest_function != 9 {
                        return Err(unsupported());
                    }
                    self.path_bytes = self
                        .path_bytes
                        .checked_add(artifact.backend.len())
                        .and_then(|n| n.checked_add(artifact.instance.len()))
                        .and_then(|n| n.checked_add(artifact.hash.len()))
                        .ok_or_else(limited)?;
                    if self.path_bytes > self.limits.source.max_metadata_bytes {
                        return Err(limited());
                    }
                    NativeSourceContent::Remote(artifact.clone())
                } else {
                    if self.fences.len().saturating_mul(2)
                        + self.readers.len().saturating_mul(2)
                        + 2
                        > self.limits.max_kernel_handles
                    {
                        return Err(limited());
                    }
                    // The no-follow attribute/oplock handle above establishes
                    // the allowed native file identity. Open data normally so
                    // ProjFS can hydrate a distrusted/renamed placeholder; raw
                    // OPEN_REPARSE_POINT data reads must not bypass that filter.
                    // Revalidate identity and every ancestor BEFORE reading.
                    let mut input = OpenOptions::new()
                        .read(true)
                        .share_mode(FILE_SHARE_READ)
                        .open(&absolute)
                        .map_err(native_error)?;
                    if file_fingerprint(&input)? != fingerprint
                        || !input.metadata()?.is_file()
                        || tag(&input)?.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
                            && tag(&input)?.ReparseTag
                                != windows_sys::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS
                    {
                        return Err(busy());
                    }
                    self.validate(ancestors)?;
                    self.fences[file_index].validate()?;
                    let captured = self.copy(&mut input)?;
                    self.readers.push(input);
                    NativeSourceContent::Local(captured)
                };
                self.entries.push(NativeSourceEntry {
                    path,
                    mode,
                    content,
                });
            }
        }
        self.validate(ancestors)?;
        ancestors.pop();
        Ok(())
    }
    fn copy(&mut self, input: &mut File) -> Result<Arc<CapturedSourceFile>> {
        let size = input.metadata()?.len();
        self.dirty_bytes = self.dirty_bytes.checked_add(size).ok_or_else(limited)?;
        if self.dirty_bytes > self.limits.source.max_local_bytes {
            return Err(limited());
        }
        let file = tempfile::Builder::new()
            .prefix("native-capture-")
            .disable_cleanup(true)
            .tempfile_in(self.directory.path())?;
        let (file, path) = file.into_parts();
        let path = path.to_path_buf();
        let mut captured = CapturedSourceFile {
            file,
            path,
            directory: self.directory.clone(),
            size,
            hash: [0; 32],
        };
        let mut hash = blake3::Hasher::new();
        let mut total = 0u64;
        let mut buffer = [0u8; 128 * 1024];
        loop {
            self.check()?;
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            total = total.checked_add(count as u64).ok_or_else(limited)?;
            if total > size {
                return Err(busy());
            }
            hash.update(&buffer[..count]);
            captured.file.write_all(&buffer[..count])?;
        }
        if total != size {
            return Err(busy());
        }
        captured.file.sync_all()?;
        captured.hash = *hash.finalize().as_bytes();
        Ok(Arc::new(captured))
    }
}
fn names(absolute: &Path, relative: &str, limits: &NativeCaptureLimits) -> Result<Vec<String>> {
    let mut result = Vec::new();
    let mut bytes = 0usize;
    for entry in std::fs::read_dir(absolute).map_err(native_error)? {
        if result.len() >= limits.source.max_entries {
            return Err(limited());
        }
        let name = entry?.file_name();
        let name = name.to_str().ok_or_else(unsupported)?.to_owned();
        if relative.is_empty() && name.eq_ignore_ascii_case(".jj") {
            continue;
        }
        crate::validate_component(&name)?;
        bytes = bytes.checked_add(name.len() + 64).ok_or_else(limited)?;
        if bytes > limits.source.max_metadata_bytes {
            return Err(limited());
        }
        result.push(name);
    }
    Ok(result)
}
/// Instantiate only placeholder metadata before establishing oplocks; otherwise
/// our own placeholder creation would correctly invalidate a directory fence.
fn prime(provider: &Provider, limits: &NativeCaptureLimits, deadline: Instant) -> Result<()> {
    let mut pending = vec![(String::new(), 0usize)];
    let mut count = 0usize;
    let mut bytes = 0usize;
    while let Some((relative, depth)) = pending.pop() {
        if Instant::now() >= deadline {
            return Err(limited());
        }
        if depth > limits.source.max_depth {
            return Err(limited());
        }
        let absolute = provider.root.join(&relative);
        for name in names(&absolute, &relative, limits)? {
            count += 1;
            bytes = bytes
                .checked_add(relative.len() + name.len() + 256)
                .ok_or_else(limited)?;
            if count > limits.source.max_entries || bytes > limits.source.max_metadata_bytes {
                return Err(limited());
            }
            let path = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            if path.len() > limits.source.max_path_bytes {
                return Err(limited());
            }
            let absolute = provider.root.join(&path);
            let file = OpenOptions::new()
                .access_mode(FILE_READ_ATTRIBUTES)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&absolute)
                .map_err(native_error)?;
            let metadata = file.metadata()?;
            let tag = tag(&file)?;
            if tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
                && tag.ReparseTag
                    != windows_sys::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS
            {
                return Err(unsupported());
            }
            if metadata.is_dir() {
                pending.push((path, depth + 1));
            }
        }
    }
    Ok(())
}
impl ProjfsMount {
    /// Capture native edits without hydrating trusted unchanged placeholders.
    /// Kernel oplocks must be available; this never falls back to Tree-only state
    /// or a caller-provided quiescence boolean. Busy writers cause an error.
    pub async fn capture_source(
        self: &Arc<Self>,
        directory: Arc<private_fs::PrivateDir>,
        limits: NativeCaptureLimits,
    ) -> Result<NativeSourceCapture> {
        ensure!(
            limits.source.max_entries > 0
                && (1..=256).contains(&limits.source.max_depth)
                && (1..=64 * 1024).contains(&limits.source.max_path_bytes)
                && (1..=256 * 1024 * 1024).contains(&limits.source.max_metadata_bytes)
                && limits.source.max_local_bytes > 0
                && (4..=262_144).contains(&limits.max_kernel_handles)
                && !limits.timeout.is_zero()
                && limits.timeout <= Duration::from_secs(60),
            "invalid native capture limits"
        );
        let capture_path = directory.path().canonicalize()?;
        if capture_path.starts_with(&self.provider.root)
            || self.provider.root.starts_with(&capture_path)
        {
            return Err(unsupported());
        }
        let deadline = Instant::now()
            .checked_add(limits.timeout)
            .ok_or_else(limited)?;
        let _publication = tokio::time::timeout(limits.timeout, self.publication.lock())
            .await
            .map_err(|_| busy())?;
        if self.state.join("publication.json").exists() {
            return Err(busy());
        }
        let owner = {
            let _pending = self.provider.pending.lock().unwrap();
            if self.provider.stopping.load(Ordering::Acquire)
                || self.provider.draining.load(Ordering::Acquire)
                || self
                    .provider
                    .source_captures
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return Err(busy());
            }
            Arc::new(CaptureOwner {
                mount: self.clone(),
            })
        };
        let mount = self.clone();
        tokio::task::spawn_blocking(move || {
            // Hydrating a distrusted placeholder may itself break NTFS read
            // oplocks. Retry the complete bounded inventory, never a partial
            // namespace, using the SAME operation deadline. Persistent writers
            // still produce Busy; no mutation or snapshot acknowledgement occurs.
            for attempt in 0..3 {
                let result = (|| {
                    mount
                        .provider
                        .source_control
                        .as_ref()
                        .context("native source marker is unavailable")?
                        .verify()?;
                    let root = SourceRootGuard::open(
                        &mount.provider.root,
                        &mount.state,
                        mount.provider.tree.identity(),
                    )?;
                    let core = mount.provider.tree.freeze_source(
                        SourceFrontendFence::AuthoritativelyQuiesced,
                        &limits.source,
                    )?;
                    prime(&mount.provider, &limits, deadline)?;
                    let rename_epoch = mount.provider.distrust.lock().unwrap().serial;
                    let mut inventory = Inventory {
                        provider: mount.provider.clone(),
                        directory: directory.clone(),
                        limits: limits.clone(),
                        deadline,
                        entries: Vec::new(),
                        fences: Vec::new(),
                        readers: Vec::new(),
                        path_bytes: 0,
                        dirty_bytes: 0,
                        discovered: 0,
                    };
                    // Administration is excluded from source contents, not
                    // from change detection. A new/deleted control child must
                    // invalidate capture even when provider callbacks are absent.
                    inventory.fence(&mount.provider.root.join(".jj"), true)?;
                    inventory.walk("", &mut Vec::new())?;
                    inventory
                        .entries
                        .sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
                    let capture = NativeSourceCapture {
                        entries: inventory.entries,
                        core,
                        fences: inventory.fences,
                        _readers: inventory.readers,
                        _root: root,
                        owner: owner.clone(),
                        rename_epoch,
                        expires: deadline,
                    };
                    capture.validate_still_current()?;
                    Ok::<_, anyhow::Error>(capture)
                })();
                match result {
                    Err(error)
                        if attempt < 2
                            && Instant::now() < deadline
                            && error.downcast_ref::<NativeCaptureError>()
                                == Some(&NativeCaptureError::Busy) => {}
                    other => return other,
                }
            }
            Err(busy())
        })
        .await
        .context("native source capture worker failed")?
    }
}
