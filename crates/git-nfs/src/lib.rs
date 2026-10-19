//! User-mode, loopback NFSv3 frontend. All mutations use the durable overlay.
//! This is a local-user trust boundary, not an authenticated remote export.
use async_trait::async_trait;
use git_overlay::overlay::{Kind, Overlay};
use git_overlay::worktree::WorkingTree;
use git_workspace::backend::GitRepo;
use nfsserve::{nfs::*, vfs::*};
use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

struct State {
    overlay: Overlay,
    repo: GitRepo,
    // Keep the exclusive owner lock alive until all RPC operations are gone.
    _working: WorkingTree,
}

#[derive(Clone)]
pub struct GitNfs {
    state: Arc<Mutex<State>>,
}

fn status(error: anyhow::Error) -> nfsstat3 {
    match error
        .downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
    {
        Some(libc::ENOENT) => nfsstat3::NFS3ERR_NOENT,
        Some(libc::ENOTDIR) => nfsstat3::NFS3ERR_NOTDIR,
        Some(libc::EISDIR) => nfsstat3::NFS3ERR_ISDIR,
        Some(libc::EEXIST) => nfsstat3::NFS3ERR_EXIST,
        Some(libc::ENOTEMPTY) => nfsstat3::NFS3ERR_NOTEMPTY,
        Some(libc::EINVAL) => nfsstat3::NFS3ERR_INVAL,
        Some(libc::EPERM) | Some(libc::EACCES) => nfsstat3::NFS3ERR_ACCES,
        Some(libc::EFBIG) => nfsstat3::NFS3ERR_FBIG,
        Some(libc::ENOSPC) => nfsstat3::NFS3ERR_NOSPC,
        _ => nfsstat3::NFS3ERR_IO,
    }
}
fn timestamp(time: SystemTime) -> nfstime3 {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    nfstime3 {
        seconds: duration.as_secs().min(u32::MAX as u64) as u32,
        nseconds: duration.subsec_nanos(),
    }
}
fn attrs(state: &mut State, id: u64) -> Result<fattr3, nfsstat3> {
    let entry = state.overlay.entry(id).map_err(status)?;
    if !entry.linked {
        return Err(nfsstat3::NFS3ERR_STALE);
    }
    let size = if entry.kind.is_dir() {
        0
    } else {
        state.overlay.size(id, &mut state.repo).map_err(status)?
    };
    Ok(fattr3 {
        ftype: match entry.kind {
            Kind::File => ftype3::NF3REG,
            Kind::Symlink => ftype3::NF3LNK,
            _ => ftype3::NF3DIR,
        },
        mode: entry.mode,
        nlink: if entry.kind.is_dir() { 2 } else { 1 },
        // This is a single-user local export. RPC AUTH_SYS must not grant ownership.
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: 1,
        fileid: id,
        atime: timestamp(entry.mtime),
        mtime: timestamp(entry.mtime),
        ctime: timestamp(entry.ctime),
    })
}
fn validate_attrs(attr: &sattr3) -> Result<(), nfsstat3> {
    if matches!(attr.uid,set_uid3::uid(uid) if uid != unsafe { libc::getuid() })
        || matches!(attr.gid,set_gid3::gid(gid) if gid != unsafe { libc::getgid() })
    {
        return Err(nfsstat3::NFS3ERR_PERM);
    }
    if matches!(attr.size,set_size3::size(size) if size > i64::MAX as u64) {
        return Err(nfsstat3::NFS3ERR_FBIG);
    }
    if matches!(attr.mtime,set_mtime::SET_TO_CLIENT_TIME(t) if t.nseconds >= 1_000_000_000) {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    Ok(())
}
fn apply_attrs(state: &mut State, id: u64, attr: sattr3) -> Result<(), nfsstat3> {
    validate_attrs(&attr)?;
    let mode = match attr.mode {
        set_mode3::mode(v) => Some(v & 0o777),
        _ => None,
    };
    let size = match attr.size {
        set_size3::size(v) => Some(v),
        _ => None,
    };
    let mtime = match attr.mtime {
        set_mtime::DONT_CHANGE => None,
        set_mtime::SET_TO_SERVER_TIME => Some(SystemTime::now()),
        set_mtime::SET_TO_CLIENT_TIME(t) if t.nseconds < 1_000_000_000 => {
            Some(UNIX_EPOCH + std::time::Duration::new(t.seconds.into(), t.nseconds))
        }
        _ => return Err(nfsstat3::NFS3ERR_INVAL),
    };
    // atime is not tracked: reads deliberately do not dirty the workspace.
    state
        .overlay
        .setattr(id, mode, size, mtime, &mut state.repo)
        .map_err(status)
}
fn name(bytes: &filename3) -> Result<&OsStr, nfsstat3> {
    if bytes.0.len() > 255 {
        return Err(nfsstat3::NFS3ERR_NAMETOOLONG);
    }
    if bytes.0.is_empty()
        || bytes.0.contains(&0)
        || bytes.0.contains(&b'/')
        || bytes.0 == b"."
        || bytes.0 == b".."
    {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    Ok(OsStr::from_bytes(&bytes.0))
}
fn exclusive_record(state: &State, parent: u64, name: &OsStr) -> std::path::PathBuf {
    state._working.directory.join("nfs-exclusive").join(format!(
        "{parent}-{}",
        blake3::hash(name.as_bytes()).to_hex()
    ))
}
fn retire_exclusive_record(state: &State, parent: u64, name: &OsStr) {
    let record = exclusive_record(state, parent, name);
    if std::fs::remove_file(&record).is_ok() {
        // Cleanup is non-authoritative; an I/O failure may retain a stale record,
        // but stable-inode verification prevents it granting a false replay.
        let _ = std::fs::File::open(record.parent().unwrap()).and_then(|file| file.sync_all());
    }
}
impl GitNfs {
    pub fn new(repo: GitRepo, overlay: Overlay, working: WorkingTree) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                repo,
                overlay,
                _working: working,
            })),
        }
    }
    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut State) -> Result<T, nfsstat3> + Send + 'static,
    ) -> Result<T, nfsstat3> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            action(&mut guard)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }
    pub async fn sync(&self) -> Result<(), nfsstat3> {
        self.run(|state| state.overlay.sync().map_err(status)).await
    }
}

impl GitNfs {
    async fn create_inner(
        &self,
        parent: u64,
        filename: &filename3,
        attr: sattr3,
        guarded: bool,
    ) -> Result<(u64, fattr3), nfsstat3> {
        validate_attrs(&attr)?;
        let filename = filename.clone();
        self.run(move |s| {
            let filename = name(&filename)?;
            let id = match s.overlay.lookup(parent, filename) {
                Ok(id) => {
                    if guarded {
                        return Err(nfsstat3::NFS3ERR_EXIST);
                    }
                    if s.overlay.entry(id).map_err(status)?.kind != Kind::File {
                        return Err(nfsstat3::NFS3ERR_EXIST);
                    }
                    id
                }
                Err(e) => match status(e) {
                    nfsstat3::NFS3ERR_NOENT => s
                        .overlay
                        .create(parent, filename, Kind::File, 0o644, None)
                        .map_err(status)?,
                    error => return Err(error),
                },
            };
            apply_attrs(s, id, attr)?;
            s.overlay.sync().map_err(status)?;
            Ok((id, attrs(s, id)?))
        })
        .await
    }
}
#[async_trait]
impl NFSFileSystem for GitNfs {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }
    fn root_dir(&self) -> u64 {
        1
    }
    async fn lookup(&self, parent: u64, filename: &filename3) -> Result<u64, nfsstat3> {
        let filename = filename.clone();
        self.run(move |s| {
            if !s.overlay.entry(parent).map_err(status)?.kind.is_dir() {
                return Err(nfsstat3::NFS3ERR_NOTDIR);
            }
            if filename.0 == b"." {
                attrs(s, parent)?;
                return Ok(parent);
            }
            if filename.0 == b".." {
                return Ok(s.overlay.entry(parent).map_err(status)?.parent.max(1));
            }
            s.overlay.lookup(parent, name(&filename)?).map_err(status)
        })
        .await
    }
    async fn getattr(&self, id: u64) -> Result<fattr3, nfsstat3> {
        self.run(move |s| attrs(s, id)).await
    }
    async fn setattr(&self, id: u64, attr: sattr3) -> Result<fattr3, nfsstat3> {
        self.setattr_guarded(id, attr, None).await
    }
    async fn setattr_guarded(
        &self,
        id: u64,
        attr: sattr3,
        guard: Option<nfstime3>,
    ) -> Result<fattr3, nfsstat3> {
        self.run(move |s| {
            if let Some(guard) = guard {
                let current = attrs(s, id)?.ctime;
                if current.seconds != guard.seconds || current.nseconds != guard.nseconds {
                    return Err(nfsstat3::NFS3ERR_NOT_SYNC);
                }
            }
            apply_attrs(s, id, attr)?;
            s.overlay.sync().map_err(status)?;
            attrs(s, id)
        })
        .await
    }
    async fn read(&self, id: u64, offset: u64, count: u32) -> Result<(Vec<u8>, bool), nfsstat3> {
        if count > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        self.run(move |s| {
            if s.overlay.entry(id).map_err(status)?.kind != Kind::File {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            let size = s.overlay.size(id, &mut s.repo).map_err(status)?;
            let data = s
                .overlay
                .read_range(id, offset, count as usize, &mut s.repo)
                .map_err(status)?;
            let eof = offset.saturating_add(data.len() as u64) >= size;
            Ok((data, eof))
        })
        .await
    }
    async fn write(&self, id: u64, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        if data.len() > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let data = data.to_vec();
        self.run(move |s| {
            if s.overlay.entry(id).map_err(status)?.kind != Kind::File {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            s.overlay
                .write(id, offset, &data, &mut s.repo)
                .map_err(status)?;
            // nfsserve acknowledges FILE_SYNC, so persistence must precede the reply.
            s.overlay.sync().map_err(status)?;
            attrs(s, id)
        })
        .await
    }
    async fn create(
        &self,
        parent: u64,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(u64, fattr3), nfsstat3> {
        self.create_inner(parent, filename, attr, false).await
    }
    async fn create_guarded(
        &self,
        parent: u64,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(u64, fattr3), nfsstat3> {
        self.create_inner(parent, filename, attr, true).await
    }
    async fn create_exclusive(
        &self,
        parent: u64,
        filename: &filename3,
        verifier: [u8; 8],
    ) -> Result<u64, nfsstat3> {
        let filename = filename.clone();
        self.run(move |s| {
            let filename = name(&filename)?;
            // Stable inode IDs bind a replay record to this exact namespace generation.
            let directory = s._working.directory.join("nfs-exclusive");
            std::fs::create_dir_all(&directory).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            std::fs::File::open(&s._working.directory)
                .and_then(|file| file.sync_all())
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            let record = exclusive_record(s, parent, filename);
            match s.overlay.lookup(parent, filename) {
                Ok(id) => {
                    if let Ok(bytes) = std::fs::read(&record)
                        && bytes.len() == 16
                        && bytes[..8] == verifier
                        && bytes[8..] == id.to_be_bytes()
                    {
                        return Ok(id);
                    }
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }
                Err(error) => {
                    if !matches!(status(error), nfsstat3::NFS3ERR_NOENT) {
                        return Err(nfsstat3::NFS3ERR_IO);
                    }
                }
            }
            let id = s
                .overlay
                .create(parent, filename, Kind::File, 0o644, None)
                .map_err(status)?;
            s.overlay.sync().map_err(status)?;
            let temporary = record.with_extension("pending");
            let mut file = std::fs::File::create(&temporary).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            file.write_all(&verifier)
                .and_then(|_| file.write_all(&id.to_be_bytes()))
                .and_then(|_| file.sync_all())
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            std::fs::rename(&temporary, &record).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            std::fs::File::open(&directory)
                .and_then(|file| file.sync_all())
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
            Ok(id)
        })
        .await
    }
    async fn mkdir(&self, parent: u64, filename: &filename3) -> Result<(u64, fattr3), nfsstat3> {
        let filename = filename.clone();
        self.run(move |s| {
            let id = s
                .overlay
                .create(parent, name(&filename)?, Kind::Directory, 0o755, None)
                .map_err(status)?;
            s.overlay.sync().map_err(status)?;
            Ok((id, attrs(s, id)?))
        })
        .await
    }
    async fn remove(&self, parent: u64, filename: &filename3) -> Result<(), nfsstat3> {
        let filename = filename.clone();
        self.run(move |s| {
            s.overlay
                .unlink(parent, name(&filename)?, false)
                .map_err(status)?;
            retire_exclusive_record(s, parent, name(&filename)?);
            s.overlay.sync().map_err(status)
        })
        .await
    }
    async fn rmdir(&self, parent: u64, filename: &filename3) -> Result<(), nfsstat3> {
        let filename = filename.clone();
        self.run(move |s| {
            s.overlay
                .unlink(parent, name(&filename)?, true)
                .map_err(status)?;
            s.overlay.sync().map_err(status)
        })
        .await
    }
    async fn rename(
        &self,
        parent: u64,
        filename: &filename3,
        dest: u64,
        target: &filename3,
    ) -> Result<(), nfsstat3> {
        let filename = filename.clone();
        let target = target.clone();
        self.run(move |s| {
            s.overlay
                .rename(parent, name(&filename)?, dest, name(&target)?, 0)
                .map_err(status)?;
            if parent != dest || filename.0 != target.0 {
                retire_exclusive_record(s, parent, name(&filename)?);
                retire_exclusive_record(s, dest, name(&target)?);
            }
            s.overlay.sync().map_err(status)
        })
        .await
    }
    async fn readdir(
        &self,
        parent: u64,
        after: u64,
        max: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        self.run(move |s| {
            let ids = s.overlay.children(parent).map_err(status)?;
            let start = if after == 0 {
                0
            } else {
                ids.iter()
                    .position(|id| *id == after)
                    .ok_or(nfsstat3::NFS3ERR_BAD_COOKIE)?
                    + 1
            };
            let end = start.saturating_add(max.min(4096)).min(ids.len());
            let mut entries = Vec::new();
            for id in &ids[start..end] {
                let entry = s.overlay.entry(*id).map_err(status)?;
                entries.push(DirEntry {
                    fileid: *id,
                    name: entry.name.as_bytes().into(),
                    attr: attrs(s, *id)?,
                });
            }
            Ok(ReadDirResult {
                entries,
                end: end == ids.len(),
            })
        })
        .await
    }
    async fn readdir_simple(
        &self,
        parent: u64,
        after: u64,
        max: usize,
    ) -> Result<ReadDirSimpleResult, nfsstat3> {
        self.run(move |s| {
            let ids = s.overlay.children(parent).map_err(status)?;
            let start = if after == 0 {
                0
            } else {
                ids.iter()
                    .position(|id| *id == after)
                    .ok_or(nfsstat3::NFS3ERR_BAD_COOKIE)?
                    + 1
            };
            let end = start.saturating_add(max.min(4096)).min(ids.len());
            let mut entries = Vec::new();
            for id in &ids[start..end] {
                let entry = s.overlay.entry(*id).map_err(status)?;
                entries.push(DirEntrySimple {
                    fileid: *id,
                    name: entry.name.as_bytes().into(),
                });
            }
            Ok(ReadDirSimpleResult {
                entries,
                end: end == ids.len(),
            })
        })
        .await
    }
    async fn symlink(
        &self,
        parent: u64,
        filename: &filename3,
        target: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(u64, fattr3), nfsstat3> {
        let filename = filename.clone();
        let target = target.0.clone();
        if target.contains(&0) {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        self.run(move |s| {
            let id = s
                .overlay
                .create(parent, name(&filename)?, Kind::Symlink, 0o777, Some(target))
                .map_err(status)?;
            s.overlay.sync().map_err(status)?;
            Ok((id, attrs(s, id)?))
        })
        .await
    }
    async fn readlink(&self, id: u64) -> Result<nfspath3, nfsstat3> {
        self.run(move |s| {
            if s.overlay.entry(id).map_err(status)?.kind != Kind::Symlink {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            Ok(s.overlay.read(id, &mut s.repo).map_err(status)?.into())
        })
        .await
    }
}
