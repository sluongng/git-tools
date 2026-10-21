//! Native output projection. Callback pointers never escape their invocation.
use crate::identity::{Content, MAX_ORIGINS, Origin, validate_origins};
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use output_core::{Artifact, Node, NodeKind, PreparedStage, Projection, Tree, TreeStat};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::windows::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawHandle,
    },
    path::{Component, Path, PathBuf},
    ptr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore};
use windows_sys::{
    Win32::{
        Foundation::*,
        Storage::{FileSystem::*, ProjectedFileSystem::*},
        System::Com::CoCreateGuid,
    },
    core::{GUID, HRESULT},
};

fn wide(s: impl AsRef<OsStr>) -> Vec<u16> {
    s.as_ref().encode_wide().chain(Some(0)).collect()
}
fn hr(c: u32) -> HRESULT {
    if c == 0 {
        0
    } else {
        (c & 0xffff | 0x80070000) as i32
    }
}
fn hydration_error(error: &anyhow::Error) -> HRESULT {
    if error
        .downcast_ref::<output_core::CacheLimitError>()
        .is_some()
    {
        hr(ERROR_FILE_TOO_LARGE)
    } else {
        E_FAIL
    }
}
fn check(h: HRESULT, op: &str) -> Result<()> {
    ensure!(h >= 0, "{op}: HRESULT {:#010x}", h as u32);
    Ok(())
}
unsafe fn string(p: *const u16) -> String {
    if p.is_null() {
        return String::new();
    }
    let mut n = 0;
    while unsafe { *p.add(n) } != 0 {
        n += 1
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(p, n) })
}
fn guid_key(g: &GUID) -> [u8; 16] {
    let mut k = [0; 16];
    k[..4].copy_from_slice(&g.data1.to_le_bytes());
    k[4..6].copy_from_slice(&g.data2.to_le_bytes());
    k[6..8].copy_from_slice(&g.data3.to_le_bytes());
    k[8..].copy_from_slice(&g.data4);
    k
}
fn guid(b: [u8; 16]) -> GUID {
    GUID {
        data1: u32::from_le_bytes(b[..4].try_into().unwrap()),
        data2: u16::from_le_bytes(b[4..6].try_into().unwrap()),
        data3: u16::from_le_bytes(b[6..8].try_into().unwrap()),
        data4: b[8..].try_into().unwrap(),
    }
}
fn timestamp(n: u64) -> i64 {
    (n / 100)
        .saturating_add(116_444_736_000_000_000)
        .min(i64::MAX as u64) as i64
}
fn info(n: &Node) -> PRJ_PLACEHOLDER_INFO {
    let mut i = PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: PRJ_FILE_BASIC_INFO {
            IsDirectory: n.kind == NodeKind::Directory,
            FileSize: n.size as i64,
            CreationTime: timestamp(n.ctime),
            LastAccessTime: timestamp(n.mtime),
            LastWriteTime: timestamp(n.mtime),
            ChangeTime: timestamp(n.ctime),
            FileAttributes: if n.kind == NodeKind::Directory {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_ARCHIVE
            },
        },
        ..Default::default()
    };
    i.VersionInfo.ProviderID[..16].copy_from_slice(b"cas-output-v0001");
    i.VersionInfo.ContentID[..8].copy_from_slice(&n.id.to_le_bytes());
    i.VersionInfo.ContentID[8..16].copy_from_slice(&n.generation.to_le_bytes());
    i
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    root: PathBuf,
    instance: [u8; 16],
    tree_identity: String,
    #[serde(default)]
    legacy_ceiling: Option<u64>,
    #[serde(default)]
    origins: Vec<Origin>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Fingerprint {
    volume: u64,
    file: [u8; 16],
}
struct Fingerprints {
    records: HashMap<u64, Fingerprint>,
    log: File,
    path: PathBuf,
    entries: usize,
}
impl Fingerprints {
    fn open(path: &Path) -> Result<Self> {
        let mut log = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        let mut bytes = Vec::new();
        log.read_to_end(&mut bytes)?;
        let mut records = HashMap::new();
        let mut valid = 0;
        let mut entries = 0;
        for line in bytes.split_inclusive(|b| *b == b'\n') {
            if line.last() != Some(&b'\n') {
                break;
            }
            let (node, fingerprint): (u64, Fingerprint) = serde_json::from_slice(line)?;
            records.insert(node, fingerprint);
            valid += line.len();
            entries += 1;
        }
        log.set_len(valid as u64)?;
        Ok(Self {
            records,
            log,
            path: path.to_owned(),
            entries,
        })
    }
    fn record(&mut self, id: u64, fingerprint: Fingerprint) -> Result<()> {
        if self.records.get(&id) == Some(&fingerprint) {
            return Ok(());
        }
        let mut bytes = serde_json::to_vec(&(id, &fingerprint))?;
        bytes.push(b'\n');
        self.log.seek(SeekFrom::End(0))?;
        self.log.write_all(&bytes)?;
        // Losing a trailing identity record only disables digest reuse after a
        // crash; it cannot authorize stale bytes. Avoid a disk fsync per stat.
        self.records.insert(id, fingerprint);
        self.entries += 1;
        Ok(())
    }
    fn collect(&mut self, tree: &Tree) -> Result<()> {
        self.records.retain(|id, _| tree.get(*id).is_ok());
        // Bound stale log growth without rewriting the whole live identity set
        // for each staged output. Losing a trailing identity disables reuse;
        // it never authorizes a different file's payload digest.
        if self.entries <= self.records.len().saturating_mul(2).saturating_add(64) {
            return Ok(());
        }
        let mut bytes = Vec::new();
        for (id, fingerprint) in &self.records {
            serde_json::to_writer(&mut bytes, &(id, fingerprint))?;
            bytes.push(b'\n');
        }
        durable_replace(&self.path, &bytes)?;
        self.log = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.entries = self.records.len();
        Ok(())
    }
}
fn native_fingerprint(path: &Path) -> Result<Option<Fingerprint>> {
    let file = match OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut info = FILE_ID_INFO::default();
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&mut info as *mut FILE_ID_INFO).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Some(Fingerprint {
        volume: info.VolumeSerialNumber,
        file: info.FileId.Identifier,
    }))
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct RenameState {
    serial: u64,
    paths: Vec<(String, u64)>,
}
#[derive(Serialize, Deserialize)]
struct Publication {
    prepared: PreparedStage,
    rename_epoch: u64,
}
struct Enumeration {
    nodes: Vec<Node>,
    next: usize,
    expression: Option<Vec<u16>>,
}
struct Request {
    canceled: AtomicBool,
    finished: AtomicBool,
    wake: Notify,
}
struct Provider {
    tree: Arc<Tree>,
    root: PathBuf,
    manifest: Mutex<Manifest>,
    manifest_file: PathBuf,
    context: Mutex<usize>,
    stopping: AtomicBool,
    draining: AtomicBool,
    distrust: Mutex<RenameState>,
    fingerprints: Mutex<Fingerprints>,
    distrust_file: PathBuf,
    enumerations: Mutex<HashMap<[u8; 16], Enumeration>>,
    pending: Mutex<HashMap<i32, Arc<Request>>>,
    early_cancellations: Mutex<VecDeque<i32>>,
    completed: AtomicU64,
    canceled: AtomicU64,
    budget: Arc<Semaphore>,
    runtime: tokio::runtime::Handle,
}
impl Provider {
    fn placeholder_info(&self, node: &Node) -> Result<PRJ_PLACEHOLDER_INFO> {
        let mut result = info(node);
        if node.kind == NodeKind::Directory {
            // Directories never hydrate bytes through ContentID.
            return Ok(result);
        }
        let Some(artifact) = &node.remote else {
            // Native edited/full files stay authoritative in NTFS. Do not
            // publish a sparse core-local file that has no immutable locator.
            bail!("core-local file projection is unsupported; preserve the original local payload");
        };
        self.tree.validate_artifact(artifact)?;
        let mut manifest = self.manifest.lock().unwrap();
        let origin = if let Some(origin) = manifest.origins.iter().find(|origin| {
            origin.backend == artifact.backend && origin.instance == artifact.instance
        }) {
            origin.clone()
        } else {
            ensure!(
                manifest.origins.len() < MAX_ORIGINS,
                "placeholder origin limit reached"
            );
            let mut id = GUID::default();
            check(unsafe { CoCreateGuid(&mut id) }, "create origin GUID")?;
            let origin = Origin {
                key: guid_key(&id),
                backend: artifact.backend.clone(),
                instance: artifact.instance.clone(),
            };
            let mut next = manifest.clone();
            next.origins.push(origin.clone());
            validate_origins(&next.origins)?;
            // No placeholder can name this key until the complete origin
            // security scope is durably available across provider restarts.
            durable_replace(&self.manifest_file, &serde_json::to_vec(&next)?)?;
            *manifest = next;
            origin
        };
        let (provider, content) =
            Content::remote(node.id, node.generation, &origin, artifact)?.encode()?;
        result.VersionInfo.ProviderID = provider;
        result.VersionInfo.ContentID = content;
        Ok(result)
    }
    fn collect_retired(&self) -> Result<()> {
        self.tree.reclaim_retired_at_boundary(64)?;
        self.fingerprints.lock().unwrap().collect(&self.tree)?;
        Ok(())
    }
    fn rename_epoch(&self, path: &str) -> u64 {
        let wanted = wide(path.replace('/', "\\"));
        self.distrust.lock().unwrap().paths.iter().find(|(p,_)|unsafe{PrjFileNameCompare(wide(p.replace('/', "\\")).as_ptr(),wanted.as_ptr())}==0).map_or(0,|(_,epoch)|*epoch)
    }
    fn is_distrusted(&self, path: &str) -> bool {
        self.rename_epoch(path) != 0
    }
    fn distrust_paths(&self, paths: &[String]) -> Result<()> {
        let mut current = self.distrust.lock().unwrap();
        let mut next = current.clone();
        next.serial = next
            .serial
            .checked_add(1)
            .context("rename epoch exhausted")?;
        for path in paths {
            let wanted = wide(path.replace('/', "\\"));
            next.paths.retain(|(p,_)|unsafe{PrjFileNameCompare(wide(p.replace('/', "\\")).as_ptr(),wanted.as_ptr())}!=0);
            next.paths.push((path.clone(), next.serial));
        }
        durable_replace(&self.distrust_file, &serde_json::to_vec(&next)?)?;
        *current = next;
        Ok(())
    }
    fn trust_path(&self, path: &str, epoch: u64) -> Result<()> {
        let mut current = self.distrust.lock().unwrap();
        let wanted = wide(path.replace('/', "\\"));
        let mut next = current.clone();
        next.paths.retain(|(p, stamp)| {
            *stamp != epoch
                || unsafe {
                    PrjFileNameCompare(wide(p.replace('/', "\\")).as_ptr(), wanted.as_ptr())
                } != 0
        });
        if next.paths.len() != current.paths.len() {
            durable_replace(&self.distrust_file, &serde_json::to_vec(&next)?)?;
            *current = next;
        }
        Ok(())
    }
    fn lookup(&self, path: &str) -> Result<Node> {
        let mut n = self.tree.get(1)?;
        for part in path.split(['\\', '/']).filter(|s| !s.is_empty()) {
            crate::validate_component(part)?;
            let wanted = wide(part);
            n = self
                .children(n.id)?
                .into_iter()
                .find(
                    |n| unsafe { PrjFileNameCompare(wide(&n.name).as_ptr(), wanted.as_ptr()) } == 0,
                )
                .context("output not found")?;
        }
        Ok(n)
    }
    fn children(&self, parent: u64) -> Result<Vec<Node>> {
        let mut all = Vec::new();
        let mut after = 0;
        loop {
            let nodes = self.tree.list(parent, after, 1024)?;
            if nodes.is_empty() {
                break;
            }
            after = nodes.last().unwrap().id;
            all.extend(nodes);
        }
        all.sort_by(|a, b| {
            unsafe { PrjFileNameCompare(wide(&a.name).as_ptr(), wide(&b.name).as_ptr()) }.cmp(&0)
        });
        Ok(all)
    }
    fn native_state(&self, path: &str) -> Result<Option<PRJ_FILE_STATE>> {
        let mut state = 0;
        let h = unsafe {
            PrjGetOnDiskFileState(
                wide(self.root.join(path.replace('/', "\\"))).as_ptr(),
                &mut state,
            )
        };
        if h == hr(ERROR_FILE_NOT_FOUND) || h == hr(ERROR_PATH_NOT_FOUND) {
            return Ok(None);
        }
        check(h, "inspect native output state")?;
        Ok(Some(state))
    }
    fn context_call(
        &self,
        f: impl FnOnce(PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT) -> HRESULT,
    ) -> Result<()> {
        let context = self.context.lock().unwrap();
        ensure!(
            *context != 0 && !self.stopping.load(Ordering::Acquire),
            "projection stopped"
        );
        check(f(*context as _), "ProjFS publication")
    }
    fn flush(&self) -> Result<()> {
        self.context_call(|c| unsafe { PrjClearNegativePathCache(c, ptr::null_mut()) })
    }
    fn validate_path(&self, path: &str) -> Result<()> {
        ensure!(
            !path.starts_with(['\\', '/']) && !path.contains('\\'),
            "expected relative slash-separated path"
        );
        let mut parent = self.tree.get(1)?;
        let parts: Vec<_> = path.split('/').collect();
        for part in &parts {
            crate::validate_component(part)?;
        }
        for (index, part) in parts.iter().enumerate() {
            crate::validate_component(part)?;
            let wanted = wide(part);
            if let Some(existing) = self.children(parent.id)?.into_iter().find(
                |n| unsafe { PrjFileNameCompare(wide(&n.name).as_ptr(), wanted.as_ptr()) } == 0,
            ) {
                ensure!(
                    existing.name == *part,
                    "Windows case alias conflicts with {}",
                    existing.name
                );
                parent = existing;
            } else {
                break;
            }
            if index + 1 < parts.len() {
                ensure!(
                    parent.kind == NodeKind::Directory,
                    "non-directory output ancestor"
                )
            }
        }
        Ok(())
    }
    fn native_symlink(&self, path: &str) -> Result<Option<String>> {
        let parent = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        self.check_ancestors(parent)?;
        let absolute = self.root.join(path);
        let meta = match std::fs::symlink_metadata(&absolute) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if !meta.file_type().is_symlink() {
            return Ok(None);
        }
        let target = std::fs::read_link(&absolute)?;
        let mut names: Vec<String> = parent
            .split('/')
            .filter(|p| !p.is_empty())
            .map(str::to_owned)
            .collect();
        for component in target.components() {
            match component {
                Component::Normal(name) => {
                    let name = name.to_str().context("non UTF-8 output symlink")?;
                    crate::validate_component(name)?;
                    names.push(name.to_owned());
                }
                Component::ParentDir => {
                    ensure!(
                        names.pop().is_some(),
                        "output symlink leaves projection root"
                    );
                }
                Component::CurDir => {}
                _ => bail!("absolute output symlinks are unsupported"),
            }
        }
        self.check_ancestors(&names.join("/"))?;
        Ok(Some(
            target
                .to_str()
                .context("non UTF-8 symlink target")?
                .replace('\\', "/"),
        ))
    }
    fn check_ancestors(&self, path: &str) -> Result<()> {
        let mut p = self.root.clone();
        for part in path.split('/').filter(|s| !s.is_empty()) {
            p.push(part);
            match std::fs::symlink_metadata(&p) {
                Ok(m) if m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                    let mut state = 0;
                    check(
                        unsafe { PrjGetOnDiskFileState(wide(&p).as_ptr(), &mut state) },
                        "inspect output ancestor",
                    )?;
                    ensure!(
                        state & PRJ_FILE_STATE_PLACEHOLDER != 0,
                        "external reparse output ancestor"
                    );
                }
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// Provider lifetime; `shutdown` drains native callbacks and prevents all late
/// asynchronous hydration writes before releasing the native context.
pub struct ProjfsMount {
    provider: Arc<Provider>,
    // FFI needs a stable address for the Arc wrapper, not its Provider pointee.
    #[allow(clippy::redundant_allocation)]
    _context_owner: Box<Arc<Provider>>,
    _lock: File,
    state: PathBuf,
    publication: tokio::sync::Mutex<()>,
}
/// Native asynchronous hydration counters, independent of shared CAS-cache metrics.
#[derive(Clone, Copy, Debug)]
pub struct Metrics {
    pub pending: usize,
    pub completed: u64,
    pub canceled: u64,
}
impl ProjfsMount {
    pub fn metrics(&self) -> Metrics {
        Metrics {
            pending: self.provider.pending.lock().unwrap().len(),
            completed: self.provider.completed.load(Ordering::Relaxed),
            canceled: self.provider.canceled.load(Ordering::Relaxed),
        }
    }
    async fn recover_publication(&self) -> Result<()> {
        let journal = self.state.join("publication.json");
        if journal.exists() {
            let publication: Publication = serde_json::from_slice(&std::fs::read(&journal)?)?;
            let prepared = publication.prepared;
            if !self.provider.tree.prepared_stage_valid(&prepared)? {
                std::fs::remove_file(journal)?;
                return Ok(());
            }
            self.publish_native(&prepared)?;
            let path = prepared.path.clone();
            self.provider.tree.commit_stage(prepared).await?;
            self.provider.trust_path(&path, publication.rename_epoch)?;
            self.provider.flush()?;
            std::fs::remove_file(journal)?;
        }
        Ok(())
    }
    fn publish_native(&self, prepared: &PreparedStage) -> Result<()> {
        self.provider.check_ancestors(&prepared.path)?;
        let absolute = self.provider.root.join(&prepared.path);
        // Full native action directories do not project purely virtual children.
        // Materialize metadata-only placeholders explicitly under those parents.
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let before = native_fingerprint(&absolute)?;
        let mut created = false;
        let native = wide(prepared.path.replace('/', "\\"));
        let info = self.provider.placeholder_info(&prepared.node)?;
        self.provider.context_call(|context| unsafe {
            let mut reason = 0;
            let result = PrjUpdateFileIfNeeded(
                context,
                native.as_ptr(),
                &info,
                std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
                PRJ_UPDATE_ALLOW_TOMBSTONE | PRJ_UPDATE_ALLOW_DIRTY_METADATA,
                &mut reason,
            );
            if result == hr(ERROR_FILE_NOT_FOUND) || result == hr(ERROR_PATH_NOT_FOUND) {
                let result = PrjWritePlaceholderInfo(
                    context,
                    native.as_ptr(),
                    &info,
                    std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
                );
                if result >= 0 {
                    created = true;
                }
                result
            } else {
                result
            }
        })?;
        let after = native_fingerprint(&absolute)?;
        // Only preserve an existing native identity across update. A concurrent
        // rename/replacement cannot associate an unrelated file with this digest.
        if (created || before.is_some() && before == after)
            && let Some(after) = after
        {
            self.provider
                .fingerprints
                .lock()
                .unwrap()
                .record(prepared.node.id, after)?;
        }
        Ok(())
    }
    fn stop(&self) {
        if self.provider.stopping.swap(true, Ordering::AcqRel) {
            return;
        }
        // Abort each pending native command before stopping virtualization.
        // StopVirtualizing alone can release sparse reads without their payload.
        let pending: Vec<_> = self.provider.pending.lock().unwrap().drain().collect();
        for (_, request) in &pending {
            request.canceled.store(true, Ordering::Release);
            request.wake.notify_one();
        }
        let context = {
            let mut context = self.provider.context.lock().unwrap();
            if *context != 0 {
                for (id, request) in &pending {
                    if !request.finished.swap(true, Ordering::AcqRel) {
                        unsafe {
                            PrjCompleteCommand(
                                *context as _,
                                *id,
                                hr(ERROR_OPERATION_ABORTED),
                                ptr::null(),
                            );
                        }
                        self.provider.completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            std::mem::take(&mut *context)
        };
        if context != 0 {
            unsafe { PrjStopVirtualizing(context as _) }
        }
    }
}
impl Drop for ProjfsMount {
    fn drop(&mut self) {
        if !self.provider.stopping.load(Ordering::Acquire) {
            self.provider.draining.store(true, Ordering::Release);
            // The public shutdown is asynchronous. The synchronous fallback
            // keeps the multithread runtime progressing while draining page-in.
            let drain = || {
                while !self.provider.pending.lock().unwrap().is_empty() {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            };
            if tokio::runtime::Handle::try_current().is_ok_and(|runtime| {
                runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
            }) {
                tokio::task::block_in_place(drain);
            } else {
                drain();
            }
        }
        self.stop()
    }
}

/// State must be outside the mount. An existing nonempty mount requires the
/// matching durable root GUID; native edits are never imported into core state.
pub async fn start(tree: Arc<Tree>, root: &Path, state: &Path) -> Result<ProjfsMount> {
    let runtime =
        tokio::runtime::Handle::try_current().context("start ProjFS inside a Tokio runtime")?;
    ensure!(
        runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread,
        "ProjFS requires a multithread Tokio runtime for native paging I/O"
    );
    std::fs::create_dir_all(state)?;
    std::fs::create_dir_all(root)?;
    let state = state.canonicalize()?;
    let root = root.canonicalize()?;
    ensure!(
        !state.starts_with(&root),
        "ProjFS state must be outside root"
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(state.join("provider.lock"))?;
    // A temp file is never authoritative: no native change precedes its rename.
    for name in ["instance.tmp", "publication.tmp"] {
        let temp = state.join(name);
        if temp.exists() {
            std::fs::remove_file(temp)?;
        }
    }
    let path = state.join("instance.json");
    let manifest: Manifest = if path.exists() {
        let mut bytes = Vec::new();
        File::open(&path)?
            .take(16 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= 16 * 1024 * 1024,
            "projection manifest exceeds supported size"
        );
        let mut m: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            matches!(m.version, 2 | 3) && m.root == root && m.tree_identity == tree.identity(),
            "projection root does not match durable state"
        );
        if m.version == 2 {
            ensure!(
                m.origins.is_empty() && m.legacy_ceiling.is_none(),
                "ambiguous legacy projection manifest"
            );
            m.version = 3;
            m.legacy_ceiling = Some(tree.allocation_highwater());
            // Provider callbacks are not running. Preserve the legacy domain
            // durably before changing the core's all-generations fence.
            durable_replace(&path, &serde_json::to_vec(&m)?)?;
        }
        ensure!(
            m.legacy_ceiling.is_some(),
            "missing legacy placeholder boundary"
        );
        validate_origins(&m.origins)?;
        for origin in &m.origins {
            tree.validate_artifact(&Artifact {
                backend: origin.backend.clone(),
                instance: origin.instance.clone(),
                digest_function: 1,
                hash: "0".repeat(64),
                size: 0,
            })?;
        }
        m
    } else {
        ensure!(
            std::fs::symlink_metadata(&root)?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            "existing marked root requires original state"
        );
        ensure!(
            std::fs::read_dir(&root)?.next().is_none(),
            "new projection root must be empty"
        );
        let mut id = GUID::default();
        check(unsafe { CoCreateGuid(&mut id) }, "create projection GUID")?;
        let m = Manifest {
            version: 3,
            root: root.clone(),
            instance: guid_key(&id),
            tree_identity: tree.identity().to_owned(),
            legacy_ceiling: Some(0),
            origins: Vec::new(),
        };
        durable_publish(&path, &serde_json::to_vec(&m)?)?;
        m
    };
    let root_name = wide(&root);
    let mut native = 0;
    if unsafe { PrjGetOnDiskFileState(root_name.as_ptr(), &mut native) } < 0
        || native & PRJ_FILE_STATE_PLACEHOLDER == 0
    {
        check(
            unsafe {
                PrjMarkDirectoryAsPlaceholder(
                    root_name.as_ptr(),
                    ptr::null(),
                    ptr::null(),
                    &guid(manifest.instance),
                )
            },
            "mark ProjFS root",
        )?;
    }
    // The original manifest is required for an already-marked root. Its synced
    // upgrade precedes this finite fence; legacy IDs remain protected forever,
    // while new remote placeholders carry their own immutable CAS locators.
    tree.enable_immutable_placeholder_locators(manifest.legacy_ceiling.unwrap())?;
    let provider = Arc::new(Provider {
        tree,
        root,
        manifest: Mutex::new(manifest),
        manifest_file: path,
        context: Mutex::new(0),
        stopping: AtomicBool::new(false),
        draining: AtomicBool::new(false),
        distrust: Mutex::new(if state.join("native-renames.json").exists() {
            serde_json::from_slice(&std::fs::read(state.join("native-renames.json"))?)?
        } else {
            RenameState::default()
        }),
        distrust_file: state.join("native-renames.json"),
        fingerprints: Mutex::new(Fingerprints::open(&state.join("native-identities.log"))?),
        enumerations: Mutex::new(HashMap::new()),
        pending: Mutex::new(HashMap::new()),
        early_cancellations: Mutex::new(VecDeque::new()),
        completed: AtomicU64::new(0),
        canceled: AtomicU64::new(0),
        budget: Arc::new(Semaphore::new(32)),
        runtime,
    });
    let context_owner = Box::new(provider.clone());
    let callbacks = PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(start_enumeration),
        EndDirectoryEnumerationCallback: Some(end_enumeration),
        GetDirectoryEnumerationCallback: Some(enumerate),
        GetPlaceholderInfoCallback: Some(placeholder),
        GetFileDataCallback: Some(hydrate),
        QueryFileNameCallback: Some(query),
        NotificationCallback: Some(notification),
        CancelCommandCallback: Some(cancel),
    };
    let empty = wide("");
    let mut mapping = PRJ_NOTIFICATION_MAPPING {
        NotificationBitMask: PRJ_NOTIFY_PRE_RENAME | PRJ_NOTIFY_PRE_SET_HARDLINK,
        NotificationRoot: empty.as_ptr(),
    };
    let options = PRJ_STARTVIRTUALIZING_OPTIONS {
        Flags: PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
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
                (&*context_owner as *const Arc<Provider>).cast(),
                &options,
                &mut context,
            )
        },
        "start output ProjFS",
    )?;
    *provider.context.lock().unwrap() = context as usize;
    let mount = ProjfsMount {
        provider,
        _context_owner: context_owner,
        _lock: lock,
        state,
        publication: tokio::sync::Mutex::new(()),
    };
    mount.recover_publication().await?;
    mount.provider.collect_retired()?;
    Ok(mount)
}

#[async_trait]
impl Projection for ProjfsMount {
    fn root(&self) -> &Path {
        &self.provider.root
    }
    async fn ensure_dir(&self, path: &str) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        let _guard = self.publication.lock().await;
        self.recover_publication().await?;
        self.provider.validate_path(path)?;
        self.provider.check_ancestors(path)?;
        self.provider.tree.ensure_dir(path).await?;
        self.provider.flush()
    }
    async fn stage(&self, path: &str, artifact: Artifact) -> Result<()> {
        let _guard = self.publication.lock().await;
        self.recover_publication().await?;
        self.provider.validate_path(path)?;
        self.provider.check_ancestors(path)?;
        ensure!(
            artifact.size <= i64::MAX as u64,
            "output exceeds native Windows file size"
        );
        let rename_epoch = self.provider.rename_epoch(path);
        let prepared = self.provider.tree.prepare_stage(path, artifact).await?;
        let journal = self.state.join("publication.json");
        durable_publish(
            &journal,
            &serde_json::to_vec(&Publication {
                prepared: prepared.clone(),
                rename_epoch,
            })?,
        )
        .context("pending publication requires recovery before new staging")?;
        if let Err(error) = self.publish_native(&prepared) {
            self.provider.tree.abort_stage(prepared).await?;
            std::fs::remove_file(journal)?;
            return Err(
                error.context("output busy, dirty or sharing-conflicted; native bytes preserved")
            );
        }
        // Retain the journal on commit/flush failure. Restart retries the exact
        // immutable generation; it never substitutes today's path binding.
        self.provider.tree.commit_stage(prepared).await?;
        self.provider.trust_path(path, rename_epoch)?;
        self.provider.flush()?;
        std::fs::remove_file(journal)?;
        self.provider.collect_retired()?;
        Ok(())
    }
    async fn stat(&self, path: &str, digest_function: i32) -> Result<Option<TreeStat>> {
        if path.is_empty() {
            return Ok(Some(TreeStat::Directory));
        }
        self.provider.validate_path(path)?;
        if let Some(target) = self.provider.native_symlink(path)? {
            return Ok(Some(TreeStat::Symlink { target }));
        }
        self.provider.check_ancestors(path)?;
        // GetOnDiskFileState can report NOT_FOUND for an opaque tombstone,
        // rather than returning the TOMBSTONE bit. Native absence is authoritative.
        // lstat of a live virtual entry only instantiates metadata, never payload.
        let metadata = match std::fs::symlink_metadata(self.root().join(path)) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if let Some(s) = self.provider.native_state(path)? {
            // Native deletion wins even when optional identity metadata is lost.
            if s & PRJ_FILE_STATE_TOMBSTONE != 0 {
                return Ok(None);
            }
            let expected_identity = self.provider.lookup(path).ok().and_then(|n| {
                self.provider
                    .fingerprints
                    .lock()
                    .unwrap()
                    .records
                    .get(&n.id)
                    .cloned()
            });
            let matching_identity = matches!((expected_identity,native_fingerprint(&self.root().join(path))?), (Some(expected),Some(actual)) if expected==actual);
            // DIRTY_PLACEHOLDER means metadata only; FULL marks modified data.
            // Bazel chmod/mtime updates must not forfeit a verified locator.
            if s & PRJ_FILE_STATE_FULL != 0
                || self.provider.is_distrusted(path)
                || !matching_identity
            {
                return match std::fs::symlink_metadata(self.root().join(path)) {
                    Ok(m) if m.is_dir() => Ok(Some(TreeStat::Directory)),
                    Ok(m) if m.file_type().is_symlink() => Ok(Some(TreeStat::Unknown)),
                    Ok(_) => Ok(Some(TreeStat::File { artifact: None })),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e.into()),
                };
            }
        } else {
            // An unclassified native item cannot authorize a remote locator.
            return Ok(Some(if metadata.is_dir() {
                TreeStat::Directory
            } else {
                TreeStat::File { artifact: None }
            }));
        }
        self.provider.tree.stat(path, digest_function).await
    }
    async fn refresh(&self, prefix: &str) -> Result<()> {
        {
            let _guard = self.publication.lock().await;
            self.recover_publication().await?;
        }
        // FMB may stall; keep publication for unrelated output bases available.
        let mut plan = self.provider.tree.prepare_refresh(prefix).await?;
        let _guard = self.publication.lock().await;
        self.recover_publication().await?;
        plan.retain_candidates(|path, node| {
            // Native data/rename authority can change while FMB is in flight.
            // Keep such nodes and let normal stat/rebuild reconciliation decide.
            if self.provider.is_distrusted(path) {
                return Ok(false);
            }
            if self
                .provider
                .native_state(path)?
                .is_some_and(|state| state & (PRJ_FILE_STATE_FULL | PRJ_FILE_STATE_TOMBSTONE) != 0)
            {
                return Ok(false);
            }
            let expected = self
                .provider
                .fingerprints
                .lock()
                .unwrap()
                .records
                .get(&node.id)
                .cloned();
            if let Some(actual) = native_fingerprint(&self.root().join(path))? {
                if expected.as_ref() != Some(&actual) {
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        let missing = self.provider.tree.apply_refresh(plan).await?;
        for path in missing {
            let n = wide(path.replace('/', "\\"));
            self.provider.context_call(|c| unsafe {
                let mut why = 0;
                let h = PrjDeleteFile(c, n.as_ptr(), PRJ_UPDATE_NONE, &mut why);
                if h == hr(ERROR_FILE_NOT_FOUND) || h == hr(ERROR_PATH_NOT_FOUND) {
                    S_OK
                } else {
                    h
                }
            })?;
        }
        self.provider.flush()?;
        self.provider.collect_retired()
    }
    async fn clean(&self, prefix: &str) -> Result<()> {
        let _guard = self.publication.lock().await;
        if !prefix.is_empty() {
            self.provider.validate_path(prefix)?;
            self.provider.check_ancestors(prefix)?;
        }
        // Explicit clean supersedes an interrupted stage within its subtree;
        // an unrelated pending stage must first finish normal publication.
        let journal = self.state.join("publication.json");
        if journal.exists() {
            let publication: Publication = serde_json::from_slice(&std::fs::read(&journal)?)?;
            let affected = prefix.is_empty()
                || publication.prepared.path == prefix
                || publication.prepared.path.starts_with(&format!("{prefix}/"));
            if affected {
                self.provider.tree.abort_stage(publication.prepared).await?;
                std::fs::remove_file(journal)?;
            } else {
                self.recover_publication().await?;
            }
        }
        // Unlink the provider namespace first: a native recursive delete must
        // not enumerate untouched virtual descendants again while deleting.
        let mut paths = Vec::new();
        if let Ok(node) = self.provider.lookup(prefix) {
            let mut queue = vec![(prefix.to_owned(), node)];
            while let Some((path, node)) = queue.pop() {
                if node.kind == NodeKind::Directory {
                    for child in self.provider.children(node.id)? {
                        let child_path = if path.is_empty() {
                            child.name.clone()
                        } else {
                            format!("{path}/{}", child.name)
                        };
                        queue.push((child_path, child));
                    }
                }
                if !path.is_empty() {
                    paths.push(path);
                }
            }
        }
        self.provider.tree.clean(prefix).await?;
        let base = self.root().join(prefix);
        if prefix.is_empty() {
            for entry in std::fs::read_dir(&base)? {
                let path = entry?.path();
                remove_native(&path)
                    .with_context(|| format!("remove native {}", path.display()))?;
            }
        } else if base.exists() {
            remove_native(&base)?;
        }
        paths.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
        for path in paths {
            self.provider.context_call(|c| unsafe {
                let mut why = 0;
                let h = PrjDeleteFile(
                    c,
                    wide(path.replace('/', "\\")).as_ptr(),
                    PRJ_UPDATE_ALLOW_TOMBSTONE
                        | PRJ_UPDATE_ALLOW_DIRTY_METADATA
                        | PRJ_UPDATE_ALLOW_READ_ONLY,
                    &mut why,
                );
                if h == hr(ERROR_FILE_NOT_FOUND) || h == hr(ERROR_PATH_NOT_FOUND) {
                    S_OK
                } else {
                    h
                }
            })?;
        }
        self.provider.flush()?;
        self.provider.collect_retired()
    }
    async fn shutdown(&self) -> Result<()> {
        // Cached paging reads are not reliably cancellable on Windows. Complete
        // verified in-flight ranges before StopVirtualizing; aborting them can
        // expose sparse zero-filled pages even when native completion is failed.
        self.provider.draining.store(true, Ordering::Release);
        while !self.provider.pending.lock().unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        self.stop();
        Ok(())
    }
}
fn durable_replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension("tmp");
    if temp.exists() {
        std::fs::remove_file(&temp)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    if unsafe {
        MoveFileExW(
            wide(&temp).as_ptr(),
            wide(path).as_ptr(),
            MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn durable_publish(path: &Path, bytes: &[u8]) -> Result<()> {
    ensure!(!path.exists(), "authoritative journal already exists");
    let temp = path.with_extension("tmp");
    if temp.exists() {
        std::fs::remove_file(&temp)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    // Same-volume atomic publication; WRITE_THROUGH requests durable native
    // completion. No native generation changes until this rename succeeds.
    if unsafe {
        MoveFileExW(
            wide(&temp).as_ptr(),
            wide(path).as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn remove_native(path: &Path) -> Result<()> {
    let m = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if m.file_attributes() & FILE_ATTRIBUTE_DIRECTORY != 0 {
        if m.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 && m.file_type().is_symlink() {
            std::fs::remove_dir(path)?;
        } else {
            std::fs::remove_dir_all(path)?;
        }
    } else {
        if m.permissions().readonly()
            && unsafe {
                SetFileAttributesW(
                    wide(path).as_ptr(),
                    m.file_attributes() & !FILE_ATTRIBUTE_READONLY,
                )
            } == 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        std::fs::remove_file(path)?
    }
    Ok(())
}

unsafe fn callback(
    data: *const PRJ_CALLBACK_DATA,
    body: impl FnOnce(&PRJ_CALLBACK_DATA, &Arc<Provider>) -> Result<HRESULT>,
) -> HRESULT {
    if data.is_null() {
        return E_INVALIDARG;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let d = unsafe { &*data };
        if d.InstanceContext.is_null() {
            return Ok(E_INVALIDARG);
        }
        let p = unsafe { &*d.InstanceContext.cast::<Arc<Provider>>() };
        body(d, p)
    })) {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => {
            eprintln!("output-projfs callback: {e:#}");
            hydration_error(&e)
        }
        Err(_) => E_UNEXPECTED,
    }
}
unsafe extern "system" fn start_enumeration(
    d: *const PRJ_CALLBACK_DATA,
    id: *const GUID,
) -> HRESULT {
    unsafe {
        callback(d, |d, p| {
            ensure!(!id.is_null(), "missing enumeration ID");
            // A locally created directory or a retired placeholder directory
            // still has native children; merge those with an empty remote set.
            let nodes = match p.lookup(&string(d.FilePathName)) {
                Ok(n) => p.children(n.id)?,
                Err(_) => Vec::new(),
            };
            p.enumerations.lock().unwrap().insert(
                guid_key(&*id),
                Enumeration {
                    nodes,
                    next: 0,
                    expression: None,
                },
            );
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn end_enumeration(d: *const PRJ_CALLBACK_DATA, id: *const GUID) -> HRESULT {
    unsafe {
        callback(d, |_, p| {
            ensure!(!id.is_null(), "missing enumeration ID");
            p.enumerations.lock().unwrap().remove(&guid_key(&*id));
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn enumerate(
    d: *const PRJ_CALLBACK_DATA,
    id: *const GUID,
    expression: *const u16,
    buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    unsafe {
        callback(d, |d, p| {
            ensure!(!id.is_null(), "missing enumeration ID");
            let mut enums = p.enumerations.lock().unwrap();
            let e = enums
                .get_mut(&guid_key(&*id))
                .context("missing enumeration")?;
            if d.Flags & PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN != 0 {
                e.next = 0;
                e.expression = None
            }
            if e.expression.is_none() {
                e.expression = Some(wide(if expression.is_null() {
                    "*".to_owned()
                } else {
                    string(expression)
                }));
            }
            let mut emitted = 0;
            while let Some(n) = e.nodes.get(e.next) {
                let name = wide(&n.name);
                if !PrjFileNameMatch(name.as_ptr(), e.expression.as_ref().unwrap().as_ptr()) {
                    e.next += 1;
                    continue;
                }
                ensure!(
                    n.kind != NodeKind::Symlink,
                    "remote symlink projection is unsupported; use confined native output symlinks"
                );
                let h = PrjFillDirEntryBuffer(name.as_ptr(), &info(n).FileBasicInfo, buffer);
                if h == hr(ERROR_INSUFFICIENT_BUFFER) {
                    return Ok(if emitted == 0 { h } else { S_OK });
                }
                if h < 0 {
                    return Ok(h);
                }
                e.next += 1;
                emitted += 1;
                if d.Flags & PRJ_CB_DATA_FLAG_ENUM_RETURN_SINGLE_ENTRY != 0 {
                    break;
                }
            }
            Ok(S_OK)
        })
    }
}
unsafe extern "system" fn query(d: *const PRJ_CALLBACK_DATA) -> HRESULT {
    unsafe {
        callback(d, |d, p| {
            Ok(if p.lookup(&string(d.FilePathName)).is_ok() {
                S_OK
            } else {
                hr(ERROR_FILE_NOT_FOUND)
            })
        })
    }
}
unsafe extern "system" fn placeholder(d: *const PRJ_CALLBACK_DATA) -> HRESULT {
    unsafe {
        callback(d, |d, p| {
            let path = string(d.FilePathName);
            let n = match p.lookup(&path) {
                Ok(n) => n,
                Err(_) => return Ok(hr(ERROR_FILE_NOT_FOUND)),
            };
            ensure!(n.kind != NodeKind::Symlink, "remote symlinks unsupported");
            let info = p.placeholder_info(&n)?;
            let result = PrjWritePlaceholderInfo(
                d.NamespaceVirtualizationContext,
                wide(&path).as_ptr(),
                &info,
                std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
            );
            if result >= 0
                && let Some(identity) = native_fingerprint(&p.root.join(&path))?
            {
                p.fingerprints.lock().unwrap().record(n.id, identity)?;
            }
            Ok(result)
        })
    }
}

struct Aligned(*mut std::ffi::c_void);
impl Drop for Aligned {
    fn drop(&mut self) {
        unsafe { PrjFreeAlignedBuffer(self.0) }
    }
}
struct ReadRequest {
    id: i32,
    artifact: Artifact,
    stream: [u8; 16],
    offset: u64,
    length: u32,
}
unsafe extern "system" fn hydrate(
    d: *const PRJ_CALLBACK_DATA,
    offset: u64,
    length: u32,
) -> HRESULT {
    unsafe {
        callback(d, |d, p| {
            if length == 0 {
                return Ok(S_OK);
            }
            if p.stopping.load(Ordering::Acquire) || p.draining.load(Ordering::Acquire) {
                return Ok(hr(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE));
            }
            ensure!(
                !d.VersionInfo.is_null(),
                "hydration requires immutable content identity"
            );
            let v = &*d.VersionInfo;
            let content = Content::decode(&v.ProviderID, &v.ContentID)?;
            let artifact = match &content {
                Content::Legacy { node, generation } => {
                    let node = p.tree.get(*node)?;
                    ensure!(
                        node.generation == *generation && node.kind == NodeKind::File,
                        "legacy placeholder generation mismatch"
                    );
                    node.remote
                        .context("legacy placeholder missing immutable artifact")?
                }
                Content::Remote { .. } => content.artifact(&p.manifest.lock().unwrap().origins)?,
            };
            p.tree.validate_artifact(&artifact)?;
            let request = ReadRequest {
                id: d.CommandId,
                artifact,
                stream: guid_key(&d.DataStreamId),
                offset,
                length,
            };
            let permit = match p.budget.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => return Ok(hr(ERROR_NOT_ENOUGH_MEMORY)),
            };
            // Cancellation may race the original callback even before its
            // asynchronous request is registered. Keep a bounded recent history
            // (far larger than the 32 outstanding request budget).
            let pending = Arc::new(Request {
                canceled: AtomicBool::new(false),
                finished: AtomicBool::new(false),
                wake: Notify::new(),
            });
            {
                let mut active = p.pending.lock().unwrap();
                // Serialize registration with the shutdown snapshot. A callback
                // that raced stop fails synchronously instead of escaping it.
                if p.stopping.load(Ordering::Acquire) || p.draining.load(Ordering::Acquire) {
                    return Ok(hr(ERROR_FILE_SYSTEM_VIRTUALIZATION_UNAVAILABLE));
                }
                let mut early = p.early_cancellations.lock().unwrap();
                if let Some(index) = early.iter().position(|id| *id == request.id) {
                    early.remove(index);
                    pending.canceled.store(true, Ordering::Release);
                }
                active.insert(request.id, pending.clone());
            }
            let p = p.clone();
            let runtime = p.runtime.clone();
            runtime.spawn(async move {
                let _permit = permit;
                let worker_provider = p.clone();
                let completion_request = pending.clone();
                let worker_request_id = request.id;
                // A panic in cache/digest code must still complete the native
                // operation once. No borrowed callback pointer reaches either task.
                let worker = tokio::spawn(async move {
                    tokio::select! {
                        _=pending.wake.notified()=>Ok(hr(ERROR_OPERATION_ABORTED)),
                        result=read_request(&worker_provider,&request,&pending)=>result,
                    }
                });
                let completion = match worker.await {
                    Ok(Ok(result)) => result,
                    Ok(Err(error)) => {
                        eprintln!("output-projfs hydration: {error:#}");
                        hydration_error(&error)
                    }
                    Err(error) => {
                        eprintln!("output-projfs worker failed: {error}");
                        E_UNEXPECTED
                    }
                };
                let context = p.context.lock().unwrap();
                if *context != 0 && !completion_request.finished.swap(true, Ordering::AcqRel) {
                    let result = if p.stopping.load(Ordering::Acquire) {
                        hr(ERROR_OPERATION_ABORTED)
                    } else {
                        completion
                    };
                    PrjCompleteCommand(*context as _, worker_request_id, result, ptr::null());
                    p.completed.fetch_add(1, Ordering::Relaxed);
                }
                let mut active = p.pending.lock().unwrap();
                if active
                    .get(&worker_request_id)
                    .is_some_and(|request| Arc::ptr_eq(request, &completion_request))
                {
                    active.remove(&worker_request_id);
                }
            });
            Ok(hr(ERROR_IO_PENDING))
        })
    }
}
async fn read_request(p: &Provider, r: &ReadRequest, cancel: &Request) -> Result<HRESULT> {
    if cancel.canceled.load(Ordering::Acquire) {
        return Ok(hr(ERROR_OPERATION_ABORTED));
    }
    // Remote v2 identities remain sufficient after their retired inode metadata
    // is collected, including after provider restart or native rename.
    let blob = p.tree.content(&r.artifact).await?;
    if cancel.canceled.load(Ordering::Acquire) {
        return Ok(hr(ERROR_OPERATION_ABORTED));
    }
    let mut file = File::open(blob.path())?;
    let mut instance = PRJ_VIRTUALIZATION_INSTANCE_INFO::default();
    {
        let context = p.context.lock().unwrap();
        if *context == 0 {
            return Ok(hr(ERROR_OPERATION_ABORTED));
        }
        check(
            unsafe { PrjGetVirtualizationInstanceInfo(*context as _, &mut instance) },
            "query ProjFS alignment",
        )?;
    }
    let alignment = instance.WriteAlignment as u64;
    ensure!(
        alignment > 0 && alignment <= 1024 * 1024,
        "unsupported write alignment"
    );
    let end = r
        .offset
        .checked_add(r.length as u64)
        .context("range overflow")?;
    ensure!(end <= r.artifact.size, "range beyond immutable output");
    let mut position = r.offset / alignment * alignment;
    let end = end.checked_add(alignment - 1).unwrap_or(r.artifact.size) / alignment * alignment;
    let end = end.min(r.artifact.size);
    let capacity = 1024 * 1024 / alignment * alignment;
    let buffer = {
        let context = p.context.lock().unwrap();
        if *context == 0 {
            return Ok(hr(ERROR_OPERATION_ABORTED));
        }
        Aligned(unsafe { PrjAllocateAlignedBuffer(*context as _, capacity as usize) })
    };
    ensure!(!buffer.0.is_null(), "aligned allocation failed");
    while position < end {
        if cancel.canceled.load(Ordering::Acquire) {
            return Ok(hr(ERROR_OPERATION_ABORTED));
        }
        let count = (end - position).min(capacity) as usize;
        file.seek(SeekFrom::Start(position))?;
        let bytes = unsafe { std::slice::from_raw_parts_mut(buffer.0.cast::<u8>(), count) };
        file.read_exact(bytes)?;
        {
            let context = p.context.lock().unwrap();
            if *context == 0 || p.stopping.load(Ordering::Acquire) {
                return Ok(hr(ERROR_OPERATION_ABORTED));
            }
            check(
                unsafe {
                    PrjWriteFileData(
                        *context as _,
                        &guid(r.stream),
                        buffer.0,
                        position,
                        count as u32,
                    )
                },
                "write immutable output range",
            )?;
        }
        position += count as u64;
    }
    Ok(S_OK)
}
unsafe extern "system" fn cancel(d: *const PRJ_CALLBACK_DATA) {
    unsafe {
        let _ = callback(d, |d, p| {
            p.canceled.fetch_add(1, Ordering::Relaxed);
            let active = p.pending.lock().unwrap();
            if let Some(r) = active.get(&d.CommandId) {
                r.canceled.store(true, Ordering::Release);
                r.wake.notify_one();
            } else {
                let mut early = p.early_cancellations.lock().unwrap();
                if early.len() == 128 {
                    early.pop_front();
                }
                early.push_back(d.CommandId);
            }
            Ok(S_OK)
        });
    }
}
unsafe extern "system" fn notification(
    d: *const PRJ_CALLBACK_DATA,
    is_dir: bool,
    n: PRJ_NOTIFICATION,
    destination: *const u16,
    _parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    unsafe {
        callback(d, |data, provider| {
            if n == PRJ_NOTIFICATION_PRE_SET_HARDLINK {
                return Ok(hr(ERROR_NOT_SUPPORTED));
            }
            if n == PRJ_NOTIFICATION_PRE_RENAME && is_dir {
                // Native action scratch directories may be renamed atomically.
                // A directory with projected descendants requires namespace
                // rebasing; reject that case rather than lose virtual children.
                for path in [string(data.FilePathName), string(destination)] {
                    if !path.is_empty()
                        && let Ok(node) = provider.lookup(&path)
                        && node.kind == NodeKind::Directory
                        && !provider.children(node.id)?.is_empty()
                    {
                        return Ok(hr(ERROR_NOT_SUPPORTED));
                    }
                }
            }
            if n == PRJ_NOTIFICATION_PRE_RENAME {
                // Windows can leave renamed placeholders clean. Their destination
                // path must not inherit the locator of an overwritten generation.
                // Persist before permitting rename; a failed rename may conservatively
                // cost digest reuse but never lets stale metadata claim content.
                provider.distrust_paths(&[string(data.FilePathName), string(destination)])?;
            }
            Ok(S_OK)
        })
    }
}
