//! Durable Fjall overlay and a read-only journal service for Git subprocesses.
use super::{
    BaseKind, Entry, Kind, Snapshot,
    codec::{decode, encode},
};
use crate::storage::open_fjall;
use anyhow::{Context, Result, ensure};
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Clone)]
struct Handles {
    entries: SingleWriterTxKeyspace,
    meta: SingleWriterTxKeyspace,
    journal: SingleWriterTxKeyspace,
    db: Arc<SingleWriterTxDatabase>,
}
pub(super) struct Store {
    handles: Option<Handles>,
    service: Option<Service>,
}
fn number(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().context("invalid overlay integer")?,
    ))
}
fn encode_entry(entry: &Entry) -> Result<Vec<u8>> {
    let lengths = [
        75usize,
        entry.name.as_bytes().len(),
        entry.base_oid.as_ref().map_or(0, String::len),
        entry.data.as_ref().map_or(0, Vec::len),
        super::timestamp(entry.mtime).len(),
        super::timestamp(entry.ctime).len(),
    ];
    let length = lengths.into_iter().try_fold(0usize, usize::checked_add);
    if length.and_then(|n| u32::try_from(n).ok()).is_none() {
        return Err(super::error(libc::EFBIG));
    }
    Ok(encode(entry))
}
impl Handles {
    fn open(directory: &Path, initialize: bool) -> Result<Self> {
        let db = open_fjall(&directory.join("overlay.fjall"))?;
        if !initialize {
            for name in ["entries", "meta", "journal"] {
                ensure!(db.keyspace_exists(name), "overlay keyspace missing");
            }
        }
        let entries = db.keyspace("entries", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        let journal = db.keyspace("journal", KeyspaceCreateOptions::default)?;
        Ok(Self {
            entries,
            meta,
            journal,
            db,
        })
    }
    fn query(&self, request: Query) -> Result<Response> {
        let tx = self.db.read_tx();
        let identity = tx
            .get(&self.meta, b"identity")?
            .context("overlay identity missing")?;
        let identity = std::str::from_utf8(&identity)?;
        let generation = number(
            &tx.get(&self.meta, b"generation")?
                .context("overlay generation missing")?,
        )?;
        let token = format!("v2:{identity}:{generation}");
        let after = match request.token {
            None => Some(0),
            Some(previous) => {
                let parts: Vec<_> = previous.split(':').collect();
                if parts.len() == 3 && parts[0] == "v2" && parts[1] == identity {
                    parts[2].parse::<u64>().ok().filter(|n| *n <= generation)
                } else {
                    None
                }
            }
        };
        let paths = if let Some(after) = after {
            let mut paths = BTreeSet::new();
            if let Some(first) = after.checked_add(1) {
                for item in tx.range(&self.journal, first.to_be_bytes().to_vec()..) {
                    let (key, path) = item.into_inner()?;
                    ensure!(key.len() == 16, "invalid overlay journal key");
                    paths.insert(path.to_vec());
                }
            }
            paths.into_iter().collect()
        } else {
            vec![b"/".to_vec()]
        };
        Ok(Response {
            token,
            paths,
            error: None,
        })
    }
}
impl Store {
    pub(super) fn open(
        directory: &Path,
        snapshot: &Snapshot,
        gitdir: &Path,
    ) -> Result<(Self, BTreeMap<u64, Entry>)> {
        let endpoint = Endpoint::new(directory)?;
        let lock = endpoint.lock()?;
        ensure!(
            try_lock(&lock)?,
            "Fjall overlay already has an active owner"
        );
        let handles = Handles::open(directory, true)?;
        let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
        if let Some(commit) = tx.get(&handles.meta, b"commit")? {
            ensure!(
                &*commit == snapshot.commit.as_bytes(),
                "overlay belongs to a different pinned commit"
            );
        } else {
            ensure!(
                tx.iter(&handles.entries).next().is_none()
                    && tx.iter(&handles.journal).next().is_none(),
                "uninitialized overlay contains records"
            );
            for n in &snapshot.nodes {
                ensure!(
                    n.parent != 1 || !n.name.as_bytes().eq_ignore_ascii_case(b".git"),
                    "Git tree conflicts with protected .git entry"
                );
                let kind = match n.kind {
                    BaseKind::File => Kind::File,
                    BaseKind::Directory => Kind::Directory,
                    BaseKind::Symlink => Kind::Symlink,
                    BaseKind::Gitlink => Kind::Gitlink,
                };
                let mode = match kind {
                    Kind::Directory | Kind::Gitlink => 0o755,
                    Kind::Symlink => 0o777,
                    Kind::File => {
                        if n.mode & 0o111 != 0 {
                            0o755
                        } else {
                            0o644
                        }
                    }
                };
                let e = Entry {
                    inode: n.inode,
                    parent: n.parent,
                    name: n.name.clone(),
                    kind,
                    mode,
                    base_oid: n.oid.clone(),
                    data: None,
                    mtime: snapshot.timestamp,
                    ctime: snapshot.timestamp,
                    linked: true,
                    control: false,
                };
                tx.insert(&handles.entries, e.inode.to_be_bytes(), encode_entry(&e)?);
            }
            let mut identity = [0u8; 16];
            File::open("/dev/urandom")?.read_exact(&mut identity)?;
            tx.insert(
                &handles.meta,
                b"identity",
                hex::encode(identity).into_bytes(),
            );
            tx.insert(&handles.meta, b"generation", 0u64.to_be_bytes());
            tx.insert(&handles.meta, b"commit", snapshot.commit.as_bytes());
        }
        let mut loaded = BTreeMap::new();
        for item in tx.iter(&handles.entries) {
            let (key, value) = item.into_inner()?;
            let e = decode(&value)?;
            ensure!(number(&key)? == e.inode, "overlay entry key mismatch");
            loaded.insert(e.inode, e);
        }
        let absolute = gitdir
            .canonicalize()
            .context("resolve worktree Git directory")?;
        ensure!(
            !absolute.as_os_str().as_bytes().contains(&b'\n'),
            "Git directory path contains newline"
        );
        let mut pointer = b"gitdir: ".to_vec();
        pointer.extend_from_slice(absolute.as_os_str().as_bytes());
        pointer.push(b'\n');
        let control = loaded.values().find(|e| e.control).cloned();
        let mut control = if let Some(control) = control {
            control
        } else {
            Entry {
                inode: loaded
                    .keys()
                    .next_back()
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    .context("overlay inode overflow")?,
                parent: 1,
                name: OsString::from(".git"),
                kind: Kind::File,
                mode: 0o444,
                base_oid: None,
                data: None,
                mtime: snapshot.timestamp,
                ctime: snapshot.timestamp,
                linked: true,
                control: true,
            }
        };
        control.data = Some(pointer);
        tx.insert(
            &handles.entries,
            control.inode.to_be_bytes(),
            encode_entry(&control)?,
        );
        loaded.insert(control.inode, control);
        tx.commit()?;
        let service = Service::start(endpoint, lock, handles.clone())?;
        Ok((
            Self {
                handles: Some(handles),
                service: Some(service),
            },
            loaded,
        ))
    }
    pub(super) fn commit(&self, changed: &[Entry], paths: &[Vec<u8>]) -> Result<()> {
        let h = self.handles.as_ref().context("overlay closed")?;
        let mut tx = h.db.write_tx().durability(Some(PersistMode::SyncAll));
        let generation = number(
            &tx.get(&h.meta, b"generation")?
                .context("overlay generation missing")?,
        )?
        .checked_add(1)
        .context("overlay generation overflow")?;
        for entry in changed {
            tx.insert(&h.entries, entry.inode.to_be_bytes(), encode_entry(entry)?);
        }
        for (ordinal, path) in paths
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
        {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&generation.to_be_bytes());
            key[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
            tx.insert(&h.journal, key, path.as_slice());
        }
        tx.insert(&h.meta, b"generation", generation.to_be_bytes());
        tx.commit()?;
        Ok(())
    }
    pub(super) fn sync(&self) -> Result<()> {
        self.handles
            .as_ref()
            .context("overlay closed")?
            .db
            .persist(PersistMode::SyncAll)?;
        Ok(())
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        if let Some(mut service) = self.service.take() {
            service.stop();
            // The service lock stays held until both the serving thread and all
            // database/keyspace handles are gone, so offline readers cannot race.
            drop(self.handles.take());
            drop(service);
        }
    }
}

const MAX_REQUEST: usize = 4096;
const MAX_RESPONSE: usize = 64 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    token: Option<String>,
}
#[derive(Serialize, Deserialize)]
struct Response {
    token: String,
    paths: Vec<Vec<u8>>,
    error: Option<String>,
}
struct Endpoint {
    socket: PathBuf,
    lock: PathBuf,
}
impl Endpoint {
    fn new(directory: &Path) -> Result<Self> {
        let uid = unsafe { libc::geteuid() };
        let runtime = PathBuf::from(format!("/tmp/git-vfs-fjall-{uid}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&runtime) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let metadata = std::fs::symlink_metadata(&runtime)?;
        ensure!(
            metadata.is_dir()
                && metadata.uid() == uid
                && metadata.permissions().mode() & 0o777 == 0o700,
            "unsafe Fjall journal runtime directory"
        );
        let absolute = directory.canonicalize()?;
        let hash = hex::encode(Sha1::digest(absolute.as_os_str().as_bytes()));
        Ok(Self {
            socket: runtime.join(format!("{hash}.sock")),
            lock: runtime.join(format!("{hash}.lock")),
        })
    }
    fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&self.lock)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "unsafe Fjall journal owner lock"
        );
        Ok(file)
    }
}
fn try_lock(file: &File) -> Result<bool> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
        Ok(false)
    } else {
        Err(error.into())
    }
}
fn check_peer(stream: &UnixStream) -> Result<()> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: The output buffer and its length match SO_PEERCRED's ucred ABI.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut size,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    ensure!(
        size as usize == std::mem::size_of::<libc::ucred>()
            && credentials.uid == unsafe { libc::geteuid() },
        "journal peer has a different user identity"
    );
    Ok(())
}
fn configure(stream: &UnixStream) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    check_peer(stream)
}
fn read_frame(stream: &mut UnixStream, maximum: usize) -> Result<Vec<u8>> {
    let mut length = [0u8; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    ensure!(length <= maximum, "journal message exceeds protocol limit");
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body)?;
    Ok(body)
}
fn write_frame(stream: &mut UnixStream, body: &[u8], maximum: usize) -> Result<()> {
    ensure!(
        body.len() <= maximum,
        "journal message exceeds protocol limit"
    );
    stream.write_all(&u32::try_from(body.len())?.to_be_bytes())?;
    stream.write_all(body)?;
    Ok(())
}
struct Service {
    thread: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    wake: UnixStream,
    endpoint: Endpoint,
    _lock: File,
}
impl Service {
    fn start(endpoint: Endpoint, lock: File, handles: Handles) -> Result<Self> {
        match std::fs::remove_file(&endpoint.socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let listener = UnixListener::bind(&endpoint.socket)?;
        std::fs::set_permissions(&endpoint.socket, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let (wake, wake_read) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::Builder::new()
            .name("git-vfs-journal".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    let mut fds = [
                        libc::pollfd {
                            fd: listener.as_raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        },
                        libc::pollfd {
                            fd: wake_read.as_raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        },
                    ];
                    // SAFETY: Both descriptors remain owned by this thread and
                    // the array contains exactly the advertised pollfd count.
                    let ready =
                        unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
                    if ready < 0 {
                        if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                        {
                            continue;
                        }
                        break;
                    }
                    if fds[1].revents != 0 || thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let (mut stream, _) = match listener.accept() {
                        Ok(stream) => stream,
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => break,
                    };
                    if thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    if configure(&stream).is_err() {
                        continue;
                    }
                    let result = (|| -> Result<Response> {
                        let body = read_frame(&mut stream, MAX_REQUEST)?;
                        let request: Query = serde_json::from_slice(&body)?;
                        handles.query(request)
                    })();
                    let response = result.unwrap_or_else(|_| Response {
                        token: String::new(),
                        paths: Vec::new(),
                        error: Some("Fjall journal query failed".into()),
                    });
                    if let Ok(body) = serde_json::to_vec(&response) {
                        let _ = write_frame(&mut stream, &body, MAX_RESPONSE);
                    }
                }
            });
        match thread {
            Ok(thread) => Ok(Self {
                thread: Some(thread),
                stop,
                wake,
                endpoint,
                _lock: lock,
            }),
            Err(e) => {
                let _ = std::fs::remove_file(&endpoint.socket);
                Err(e.into())
            }
        }
    }
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            // A private wakeup descriptor still works if the socket pathname
            // was removed, unlike connecting to that pathname during shutdown.
            let _ = self.wake.write(&[1]);
            let _ = thread.join();
        }
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.endpoint.socket);
    }
}
fn query(directory: &Path, request: Query) -> Result<Response> {
    let endpoint = Endpoint::new(directory)?;
    match UnixStream::connect(&endpoint.socket) {
        Ok(mut stream) => {
            configure(&stream)?;
            write_frame(&mut stream, &serde_json::to_vec(&request)?, MAX_REQUEST)?;
            let response: Response =
                serde_json::from_slice(&read_frame(&mut stream, MAX_RESPONSE)?)?;
            ensure!(
                response.error.is_none(),
                "Fjall journal owner rejected query"
            );
            Ok(response)
        }
        Err(connect_error) => {
            let lock = endpoint.lock()?;
            ensure!(
                try_lock(&lock)?,
                "Fjall journal owner is active but unavailable: {connect_error}"
            );
            let handles = Handles::open(directory, false).context(
                "open offline Fjall journal; an active database owner may still be closing",
            )?;
            let response = handles.query(request)?;
            drop(handles);
            drop(lock);
            Ok(response)
        }
    }
}
pub(super) fn changes_since(directory: &Path, token: &str) -> Result<(String, Vec<Vec<u8>>)> {
    let response = query(
        directory,
        Query {
            token: Some(token.to_owned()),
        },
    )?;
    Ok((response.token, response.paths))
}
pub(super) fn dirty_paths(directory: &Path) -> Result<Vec<Vec<u8>>> {
    Ok(query(directory, Query { token: None })?.paths)
}
