//! Thin session orchestration. No proxy ownership, scheduler or automatic cleanup.
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub proxy_url: String,
    pub proxy_repo: String,
    pub reference: String,
    pub revision: String,
    pub bazelrc: PathBuf,
    pub git_vfs: PathBuf,
    pub git: PathBuf,
    pub bazel: PathBuf,
    pub mount_timeout_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Process {
    pid: u32,
    start_ticks: u64,
    executable: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    session_root: PathBuf,
    settings: Settings,
    phase: String,
    mount: Option<Process>,
    #[serde(default)]
    timings_ms: Vec<(String, u128)>,
    #[serde(default)]
    last_successful_build_unix_ms: Option<u128>,
    #[serde(default)]
    last_successful_save_unix_ms: Option<u128>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub session: PathBuf,
    pub revision: String,
    pub phase: String,
    /// Namespace/process readiness only; does not assert RBE or cache use.
    pub edit_ready: bool,
    pub mounted: bool,
    pub last_successful_build_unix_ms: Option<u128>,
    pub last_successful_save_unix_ms: Option<u128>,
    pub process_verified: bool,
    pub workspace: PathBuf,
    pub timings_ms: Vec<(String, u128)>,
}

// Until the ownership record is durable, dropping Child would orphan the VFS.
// This guard exclusively owns a direct, unreaped child: its PID cannot be
// recycled, unlike a PID loaded from a previous session's manifest.
struct StartupChild {
    child: Option<Child>,
}

impl StartupChild {
    fn abort(&mut self) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if child.try_wait()?.is_some() {
            self.child = None;
            return Ok(());
        }
        signal_unreaped_child(child)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait()?.is_some() {
                self.child = None;
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "new VFS PID {} ignored graceful shutdown; ownership was not durable; manual recovery required (no forced kill attempted)",
                child.id()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn startup_failed(&mut self, original: anyhow::Error) -> anyhow::Error {
        let pid = self.child.as_ref().map(Child::id).unwrap_or(0);
        match self.abort() {
            Ok(()) => original.context(format!("VFS startup failed; owned child PID {pid} reaped")),
            Err(cleanup) => {
                // The failure names the still-owned PID for manual recovery;
                // do not repeat another bounded wait from Drop or force-kill.
                drop(self.child.take());
                original.context(format!("VFS startup failed; cleanup of owned child PID {pid} failed: {cleanup:#}; manual recovery may be required; no forced kill attempted"))
            }
        }
    }
}

impl Drop for StartupChild {
    fn drop(&mut self) {
        if let Err(error) = self.abort() {
            eprintln!("VFS startup cleanup: {error:#}");
        }
    }
}

#[cfg(target_os = "linux")]
fn signal_unreaped_child(child: &Child) -> Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) };
    let result = if fd >= 0 {
        let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                fd.as_raw_fd(),
                libc::SIGINT,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        }
    } else {
        // An fd cannot be acquired under descriptor exhaustion. This fallback is
        // ONLY for this guard's exclusive direct child, with no intervening reap;
        // its PID remains reserved even if it exits concurrently. Never use it
        // for a persisted/resumed process or after try_wait returns Some.
        unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) as libc::c_long }
    };
    ensure!(
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
        "signalling new VFS PID {} failed: {}",
        child.id(),
        std::io::Error::last_os_error()
    );
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn signal_unreaped_child(_: &Child) -> Result<()> {
    bail!("Linux only")
}

struct Session {
    root: PathBuf,
    manifest: Manifest,
    _lock: File,
    timings: Vec<(String, u128)>,
}

fn platform() -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "agent-session currently supports Linux FUSE only; NFS/ProjFS orchestration is not implemented"
    );
    Ok(())
}

fn lock(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join("session.lock"))?;
    file.try_lock_exclusive()
        .context("another session operation is running")?;
    Ok(file)
}

fn validate(settings: &Settings) -> Result<()> {
    let authority = settings
        .proxy_url
        .strip_prefix("http://")
        .or_else(|| settings.proxy_url.strip_prefix("https://"))
        .context("proxy URL must be an HTTP(S) origin")?;
    ensure!(
        !authority.is_empty()
            && !authority.contains(['@', '?', '#'])
            && !authority.chars().any(char::is_whitespace)
            && !authority.trim_end_matches('/').contains('/'),
        "proxy URL must be an origin without credentials, query or path"
    );
    ensure!(
        !settings.proxy_repo.is_empty()
            && settings
                .proxy_repo
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)),
        "proxy repository must be a plain alias"
    );
    ensure!(
        settings.revision.len() == 40 && settings.revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "revision must be a full immutable SHA-1 commit"
    );
    ensure!(
        !settings.reference.is_empty()
            && !settings.reference.starts_with('-')
            && !settings.reference.chars().any(char::is_control),
        "invalid clone reference"
    );
    ensure!(
        (1..=3600).contains(&settings.mount_timeout_seconds),
        "mount timeout must be 1..3600 seconds"
    );
    Ok(())
}

// A caller's shell may be inside another worktree. Never let its contextual
// Git overrides redirect session operations into that unrelated repository.
fn isolate_git_context(command: &mut Command) {
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ] {
        command.env_remove(name);
    }
}

fn executable(path: &Path) -> Result<PathBuf> {
    if path.components().count() > 1 || path.is_absolute() {
        return path.canonicalize().context("resolving executable");
    }
    for directory in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let candidate = directory.join(path);
        if candidate.is_file() {
            return Ok(candidate.canonicalize()?);
        }
    }
    bail!("executable {} not found in PATH", path.display())
}

impl Session {
    fn open(root: &Path) -> Result<Self> {
        platform()?;
        let root = root.canonicalize()?;
        let guard = lock(&root)?;
        let manifest: Manifest = serde_json::from_reader(File::open(root.join("session.json"))?)?;
        ensure!(
            manifest.version == VERSION,
            "unsupported session manifest version {}",
            manifest.version
        );
        ensure!(
            manifest.session_root == root,
            "session manifest belongs to a different directory; copied/moved sessions cannot control the original processes"
        );
        validate(&manifest.settings)?;
        Ok(Self {
            root,
            manifest,
            _lock: guard,
            timings: vec![],
        })
    }

    fn persist(&self) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(&self.root)?;
        serde_json::to_writer_pretty(&mut file, &self.manifest)?;
        file.write_all(b"\n")?;
        file.as_file().sync_all()?;
        file.persist(self.root.join("session.json"))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    fn run(&mut self, phase: &str, command: &mut Command) -> Result<()> {
        self.manifest.phase = phase.to_owned();
        self.persist()?;
        let started = Instant::now();
        let status = command
            .stdout(Stdio::from(std::io::stderr()))
            .status()
            .with_context(|| format!("starting {phase}"))?;
        self.timings
            .push((phase.to_owned(), started.elapsed().as_millis()));
        self.manifest.timings_ms = self.timings.clone();
        self.persist()?;
        ensure!(
            status.success(),
            "{phase} failed ({status}); state retained at {}",
            self.root.display()
        );
        Ok(())
    }

    fn vfs(&self) -> Command {
        let mut command = Command::new(&self.manifest.settings.git_vfs);
        isolate_git_context(&mut command);
        command.arg("--git").arg(&self.manifest.settings.git);
        command
    }

    fn bazel(&self) -> Command {
        let mut command = Command::new(&self.manifest.settings.bazel);
        command
            .current_dir(self.root.join("workspace"))
            .arg("--nosystem_rc")
            .arg("--nohome_rc")
            .arg("--experimental_remote_repo_contents_cache")
            .arg("--digest_function=BLAKE3")
            .arg("--unix_digest_hash_attribute_name=user.blake3")
            .arg(format!(
                "--bazelrc={}",
                self.manifest.settings.bazelrc.display()
            ))
            .arg(format!(
                "--output_user_root={}",
                self.root.join("bazel-user").display()
            ))
            .arg(format!(
                "--output_base={}",
                self.root.join("bazel-output").display()
            ));
        command
    }

    fn report(&self) -> Result<Report> {
        Ok(Report {
            session: self.root.clone(),
            revision: self.manifest.settings.revision.clone(),
            phase: self.manifest.phase.clone(),
            edit_ready: mounted(&self.root.join("workspace"))?
                && self.manifest.mount.as_ref().is_some_and(process_matches),
            mounted: mounted(&self.root.join("workspace"))?,
            last_successful_build_unix_ms: self.manifest.last_successful_build_unix_ms,
            last_successful_save_unix_ms: self.manifest.last_successful_save_unix_ms,
            process_verified: self.manifest.mount.as_ref().is_some_and(process_matches),
            workspace: self.root.join("workspace"),
            timings_ms: if self.timings.is_empty() {
                self.manifest.timings_ms.clone()
            } else {
                self.timings.clone()
            },
        })
    }

    fn ready(&self) -> Result<()> {
        let report = self.report()?;
        ensure!(
            report.mounted && report.process_verified,
            "session is not ready; inspect status/vfs.log, then resume"
        );
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        if mounted(&self.root.join("workspace"))? {
            self.ready()?;
            self.manifest.phase = "edit_ready".into();
            return self.persist();
        }
        ensure!(
            !self.manifest.mount.as_ref().is_some_and(process_matches),
            "previous VFS process is still alive without a mount; stop it before resume"
        );
        ensure!(
            self.root.join("repo.git/HEAD").is_file(),
            "clone is incomplete; state retained for diagnosis; create a new session rather than overwriting it"
        );
        let revision = self.manifest.settings.revision.clone();
        let proxy_url = self.manifest.settings.proxy_url.clone();
        let proxy_repo = self.manifest.settings.proxy_repo.clone();
        let repo = self.root.join("repo.git");
        let mut check = Command::new(&self.manifest.settings.git);
        isolate_git_context(&mut check);
        check.arg("-C").arg(&repo).args([
            "rev-parse",
            "--verify",
            &format!("{revision}^{{commit}}"),
        ]);
        let output = check.output()?;
        if !output.status.success() {
            let mut fetch = Command::new(&self.manifest.settings.git);
            isolate_git_context(&mut fetch);
            fetch
                .arg("-C")
                .arg(&repo)
                .args(["fetch", "--depth=1", "origin", &revision]);
            self.run("fetch_revision", &mut fetch)?;
        }
        let output = check.output()?;
        ensure!(
            output.status.success()
                && String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .eq_ignore_ascii_case(&revision),
            "pinned revision does not resolve to the requested commit"
        );
        let mut warm = self.vfs();
        warm.arg("warm-blake3").arg("--repo").arg(&repo).args([
            "--revision",
            &revision,
            "--proxy-url",
            &proxy_url,
            "--proxy-repo",
            &proxy_repo,
        ]);
        self.run("mapping_import", &mut warm)?;
        self.manifest.phase = "mounting".into();
        self.persist()?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("vfs.log"))?;
        let mut mount = self.vfs();
        mount
            .arg("mount")
            .arg("--repo")
            .arg(repo)
            .args(["--revision", &revision])
            .arg("--overlay")
            .arg(self.root.join("overlay"))
            .args([
                "--blake3-proxy-url",
                &proxy_url,
                "--blake3-proxy-repo",
                &proxy_repo,
                "--prefetch-bazel",
            ])
            .arg(self.root.join("workspace"))
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            mount.process_group(0);
        }
        let started = Instant::now();
        let mut pending = StartupChild {
            child: Some(mount.spawn().context("starting VFS")?),
        };
        let ownership = (|| {
            let child = pending.child.as_ref().expect("startup guard owns child");
            let identity = process_identity(child.id()).context("recording VFS identity")?;
            self.manifest.mount = Some(identity);
            self.persist().context("persisting VFS ownership")
        })();
        if let Err(error) = ownership {
            return Err(pending.startup_failed(error));
        }
        // Ownership is now durable. Later readiness failures intentionally leave
        // the process available to the normal verified stop/resume workflow.
        let mut child = pending.child.take().expect("durable VFS child");
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("VFS exited ({status}); inspect vfs.log; all session state retained");
            }
            if mounted(&self.root.join("workspace"))? {
                break;
            }
            if started.elapsed()
                >= Duration::from_secs(self.manifest.settings.mount_timeout_seconds)
            {
                // Leave the recorded process intact: stop can safely address it.
                bail!("mount readiness timed out; use stop before resume; state retained");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        self.timings
            .push(("mount_ready".into(), started.elapsed().as_millis()));
        self.manifest.timings_ms = self.timings.clone();
        self.manifest.phase = "edit_ready".into();
        self.persist()
    }
}

pub fn create(root: &Path, mut settings: Settings) -> Result<Report> {
    platform()?;
    validate(&settings)?;
    settings.git_vfs = executable(&settings.git_vfs)?;
    settings.git = executable(&settings.git)?;
    settings.bazel = executable(&settings.bazel)?;
    settings.bazelrc = settings
        .bazelrc
        .canonicalize()
        .context("provisioned bazelrc must exist")?;
    ensure!(settings.bazelrc.is_file(), "bazelrc must be a file");
    fs::create_dir(root)
        .context("session directory must not exist (use resume for an existing session)")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    }
    let root = root.canonicalize()?;
    let guard = lock(&root)?;
    fs::create_dir(root.join("workspace"))?;
    let mut session = Session {
        root: root.clone(),
        manifest: Manifest {
            version: VERSION,
            session_root: root,
            settings,
            phase: "creating".into(),
            mount: None,
            timings_ms: vec![],
            last_successful_build_unix_ms: None,
            last_successful_save_unix_ms: None,
        },
        _lock: guard,
        timings: vec![],
    };
    session.persist()?;
    let mut clone = session.vfs();
    clone
        .args([
            "clone",
            "--depth",
            "1",
            "--branch",
            &session.manifest.settings.reference,
        ])
        .arg(format!(
            "{}/git/{}.git",
            session.manifest.settings.proxy_url.trim_end_matches('/'),
            session.manifest.settings.proxy_repo
        ))
        .arg(session.root.join("repo.git"));
    session.run("clone", &mut clone)?;
    session.start()?;
    session.report()
}

pub fn resume(root: &Path) -> Result<Report> {
    let mut session = Session::open(root)?;
    session.start()?;
    session.report()
}
pub fn status(root: &Path) -> Result<Report> {
    Session::open(root)?.report()
}

pub fn build(root: &Path, args: &[String]) -> Result<Report> {
    ensure!(!args.is_empty(), "build requires at least one target");
    let mut session = Session::open(root)?;
    session.ready()?;
    let mut command = session.bazel();
    command.arg("build").args(args);
    session.run("build", &mut command)?;
    session.manifest.last_successful_build_unix_ms =
        Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis());
    session.manifest.phase = "edit_ready".into();
    session.persist()?;
    session.report()
}

pub fn save(root: &Path, args: &[String]) -> Result<Report> {
    let mut session = Session::open(root)?;
    session.ready()?;
    let mut command = Command::new(&session.manifest.settings.git);
    isolate_git_context(&mut command);
    command
        .arg("-C")
        .arg(session.root.join("workspace"))
        .arg("commit")
        .args(args);
    session.run("native_commit", &mut command)?;
    session.manifest.last_successful_save_unix_ms =
        Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis());
    session.manifest.phase = "edit_ready".into();
    session.persist()?;
    session.report()
}

pub fn stop(root: &Path) -> Result<Report> {
    let mut session = Session::open(root)?;
    // Do not create a new Bazel server solely to stop it.
    if session
        .root
        .join("bazel-output/server/server.pid.txt")
        .exists()
    {
        let mut command = session.bazel();
        if !mounted(&session.root.join("workspace"))? {
            // Bazel unconditionally switches to batch mode outside a workspace,
            // even for shutdown. A tiny private control workspace keeps shutdown
            // on the existing private output-base connection path (which exits
            // without starting a server when no server can be connected).
            let control = session.root.join("bazel-control");
            fs::create_dir_all(&control)?;
            let marker = control.join("MODULE.bazel");
            if !marker.exists() {
                File::create(marker)?;
            }
            command.current_dir(control).arg("--noworkspace_rc");
        }
        command.arg("--nobatch").arg("shutdown");
        session.run("bazel_shutdown", &mut command)?;
    }
    if let Some(process) = session
        .manifest
        .mount
        .as_ref()
        .filter(|p| process_matches(p))
    {
        signal_owned(process)?;
        let started = Instant::now();
        while process_matches(process) || mounted(&session.root.join("workspace"))? {
            ensure!(
                started.elapsed() < Duration::from_secs(30),
                "VFS shutdown timed out; state retained; no forced kill attempted"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        session
            .timings
            .push(("vfs_shutdown".into(), started.elapsed().as_millis()));
    }
    ensure!(
        !mounted(&session.root.join("workspace"))?,
        "workspace is still mounted but ownership is unverified; refusing to unmount or kill it"
    );
    session.manifest.timings_ms = session.timings.clone();
    session.manifest.mount = None;
    session.manifest.phase = "stopped".into();
    session.persist()?;
    session.report()
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Result<Process> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .context("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    ensure!(fields.first().copied() != Some("Z"), "process is a zombie");
    let start_ticks = fields
        .get(19)
        .context("missing process start time")?
        .parse()?;
    Ok(Process {
        pid,
        start_ticks,
        executable: fs::read_link(format!("/proc/{pid}/exe"))?,
    })
}
#[cfg(not(target_os = "linux"))]
fn process_identity(_: u32) -> Result<Process> {
    bail!("Linux only")
}
fn process_matches(process: &Process) -> bool {
    process_identity(process.pid).is_ok_and(|current| {
        current.start_ticks == process.start_ticks && current.executable == process.executable
    })
}

#[cfg(target_os = "linux")]
fn signal_owned(process: &Process) -> Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // pidfd pins the kernel process identity. Revalidate after acquisition so a
    // recycled PID cannot ever become a signal target.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, process.pid, 0) };
    ensure!(
        fd >= 0,
        "pidfd_open failed: {} (kernel >= 5.3 required)",
        std::io::Error::last_os_error()
    );
    let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
    ensure!(
        process_matches(process),
        "VFS identity changed; refusing to signal"
    );
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            libc::SIGINT,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    ensure!(
        result == 0,
        "signalling VFS failed: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn signal_owned(_: &Process) -> Result<()> {
    bail!("Linux only")
}

#[cfg(target_os = "linux")]
fn mounted(path: &Path) -> Result<bool> {
    use std::os::unix::ffi::OsStrExt;
    let target = path.as_os_str().as_bytes();
    for line in fs::read("/proc/self/mountinfo")?.split(|b| *b == b'\n') {
        let Some(field) = line.split(|b| *b == b' ').nth(4) else {
            continue;
        };
        let mut decoded = Vec::new();
        let mut i = 0;
        while i < field.len() {
            if field[i] == b'\\'
                && i + 3 < field.len()
                && field[i + 1..i + 4]
                    .iter()
                    .all(|b| (b'0'..=b'7').contains(b))
            {
                decoded.push(
                    (field[i + 1] - b'0') * 64 + (field[i + 2] - b'0') * 8 + field[i + 3] - b'0',
                );
                i += 4;
            } else {
                decoded.push(field[i]);
                i += 1;
            }
        }
        if decoded == target {
            return Ok(true);
        }
    }
    Ok(false)
}
#[cfg(not(target_os = "linux"))]
fn mounted(_: &Path) -> Result<bool> {
    bail!("Linux only")
}
