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

/// A source mount hosted by the shared daemon. Writable state remains private.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountRequest {
    pub session: PathBuf,
    pub repo: PathBuf,
    pub revision: String,
    pub overlay: PathBuf,
    pub mountpoint: PathBuf,
    pub proxy_url: String,
    pub proxy_repo: String,
    pub git: PathBuf,
    pub timeout_seconds: u64,
}

/// External lifecycle authority. No daemon PID is stored as a killable child.
pub trait MountController {
    fn mount(&self, request: MountRequest) -> Result<()>;
    fn unmount(&self, session: &Path) -> Result<()>;
    fn is_mounted(&self, session: &Path) -> Result<bool>;
    /// Enforced cache/BOS flags, appended after user rc and command arguments.
    fn bazel_args(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

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
    /// Explicit host sharing; None preserves Bazel/provisioned rc behavior.
    #[serde(default)]
    pub repo_contents_cache: Option<PathBuf>,
    #[serde(default = "remote_repo_contents_cache_default")]
    pub remote_repo_contents_cache: bool,
}

fn remote_repo_contents_cache_default() -> bool {
    true
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
    daemon_managed: bool,
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
    pub daemon_managed: bool,
    pub workspace: PathBuf,
    /// Pinned explicit path, or Bazel's default/provisioned rc policy when None.
    pub repo_contents_cache: Option<PathBuf>,
    pub repo_contents_cache_policy: &'static str,
    pub remote_repo_contents_cache: bool,
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

fn validate(settings: &Settings, daemon_managed: bool) -> Result<()> {
    let authority = settings
        .proxy_url
        .strip_prefix("http://")
        .or_else(|| settings.proxy_url.strip_prefix("https://"))
        .context("proxy URL must be an HTTP(S) origin")?;
    let local_capability = daemon_managed
        && settings.proxy_url.starts_with("http://")
        && authority
            .trim_end_matches('/')
            .split_once('/')
            .is_some_and(|(address, path)| {
                address
                    .parse::<std::net::SocketAddr>()
                    .is_ok_and(|address| address.ip().is_loopback() && address.port() != 0)
                    && path.len() == 64
                    && path.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
    ensure!(
        !authority.is_empty()
            && !authority.contains(['@', '?', '#'])
            && !authority.chars().any(char::is_whitespace)
            && (local_capability || !authority.trim_end_matches('/').contains('/')),
        "proxy URL must not contain embedded credentials, query or fragment; only daemon-owned loopback capability prefixes are allowed"
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
    if let Some(cache) = &settings.repo_contents_cache {
        ensure!(
            cache.is_absolute(),
            "shared repo contents cache must be an absolute directory path"
        );
    }
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
        validate(&manifest.settings, manifest.daemon_managed)?;
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

    fn check_shared_cache(&self) -> Result<()> {
        if let Some(cache) = &self.manifest.settings.repo_contents_cache {
            ensure!(
                !cache.starts_with(&self.root) && !self.root.starts_with(cache),
                "shared repo contents cache and session directory must not overlap"
            );
            ensure!(
                cache.is_dir() && cache.canonicalize().is_ok_and(|current| current == *cache),
                "shared repo contents cache is missing or its canonical location changed; restore the pinned directory before resume/build"
            );
        }
        Ok(())
    }

    fn bazel(&self) -> Command {
        let mut command = Command::new(&self.manifest.settings.bazel);
        command
            .current_dir(self.root.join("workspace"))
            .arg("--nosystem_rc")
            .arg("--nohome_rc")
            .arg(if self.manifest.settings.remote_repo_contents_cache {
                "--experimental_remote_repo_contents_cache"
            } else {
                "--noexperimental_remote_repo_contents_cache"
            })
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
        self.report_with_controller(None)
    }

    fn report_with_controller(&self, controller: Option<&dyn MountController>) -> Result<Report> {
        let mount_present = mounted(&self.root.join("workspace"))?;
        let verified = if self.manifest.daemon_managed {
            match controller {
                Some(controller) => controller.is_mounted(&self.root)?,
                None => false,
            }
        } else {
            self.manifest.mount.as_ref().is_some_and(process_matches)
        };
        Ok(Report {
            session: self.root.clone(),
            revision: self.manifest.settings.revision.clone(),
            phase: self.manifest.phase.clone(),
            edit_ready: mount_present && verified,
            mounted: mount_present,
            last_successful_build_unix_ms: self.manifest.last_successful_build_unix_ms,
            last_successful_save_unix_ms: self.manifest.last_successful_save_unix_ms,
            process_verified: verified,
            daemon_managed: self.manifest.daemon_managed,
            workspace: self.root.join("workspace"),
            repo_contents_cache: self.manifest.settings.repo_contents_cache.clone(),
            repo_contents_cache_policy: if self.manifest.settings.repo_contents_cache.is_some() {
                "explicit pinned host directory"
            } else {
                "Bazel default under private output_user_root unless provisioned rc/build arguments override"
            },
            remote_repo_contents_cache: self.manifest.settings.remote_repo_contents_cache,
            timings_ms: if self.timings.is_empty() {
                self.manifest.timings_ms.clone()
            } else {
                self.timings.clone()
            },
        })
    }

    fn ready_with_controller(&self, controller: Option<&dyn MountController>) -> Result<()> {
        let report = self.report_with_controller(controller)?;
        ensure!(
            report.mounted && report.process_verified,
            "session is not ready; inspect status/vfs.log, then resume"
        );
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        self.start_with_controller(None)
    }

    fn start_with_controller(&mut self, controller: Option<&dyn MountController>) -> Result<()> {
        ensure!(
            self.manifest.daemon_managed == controller.is_some(),
            "session lifecycle owner differs; use the workspace daemon for daemon-managed sessions"
        );
        self.check_shared_cache()?;
        if mounted(&self.root.join("workspace"))? {
            self.ready_with_controller(controller)?;
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
        if let Some(controller) = controller {
            self.manifest.phase = "mounting".into();
            self.persist()?;
            let started = Instant::now();
            controller.mount(MountRequest {
                session: self.root.clone(),
                repo,
                revision,
                overlay: self.root.join("overlay"),
                mountpoint: self.root.join("workspace"),
                proxy_url,
                proxy_repo,
                git: self.manifest.settings.git.clone(),
                timeout_seconds: self.manifest.settings.mount_timeout_seconds,
            })?;
            self.ready_with_controller(Some(controller))?;
            self.timings
                .push(("mount_ready".into(), started.elapsed().as_millis()));
            self.manifest.timings_ms = self.timings.clone();
            self.manifest.phase = "edit_ready".into();
            return self.persist();
        }
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

pub fn create(root: &Path, settings: Settings) -> Result<Report> {
    create_inner(root, settings, None)
}

pub fn create_with_controller(
    root: &Path,
    settings: Settings,
    controller: &dyn MountController,
) -> Result<Report> {
    create_inner(root, settings, Some(controller))
}

fn create_inner(
    root: &Path,
    mut settings: Settings,
    controller: Option<&dyn MountController>,
) -> Result<Report> {
    platform()?;
    validate(&settings, controller.is_some())?;
    if let Some(cache) = &mut settings.repo_contents_cache {
        *cache = cache
            .canonicalize()
            .context("shared repo contents cache must already exist")?;
        ensure!(
            cache.is_dir(),
            "shared repo contents cache must be a directory"
        );
    }
    if controller.is_none() {
        settings.git_vfs = executable(&settings.git_vfs)?;
    }
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
            daemon_managed: controller.is_some(),
            timings_ms: vec![],
            last_successful_build_unix_ms: None,
            last_successful_save_unix_ms: None,
        },
        _lock: guard,
        timings: vec![],
    };
    session.check_shared_cache()?;
    session.persist()?;
    let mut clone = if controller.is_some() {
        let mut command = Command::new(&session.manifest.settings.git);
        isolate_git_context(&mut command);
        command.args([
            "--no-replace-objects",
            "-c",
            "protocol.version=2",
            "clone",
            "--bare",
            "--filter=blob:none",
            "--single-branch",
            "--no-local",
        ]);
        command
    } else {
        let mut command = session.vfs();
        command.arg("clone");
        command
    };
    // Keep the daemon's local HTTP capability out of process arguments. Git
    // receives HTTP authorization via its private environment during clone;
    // later native fetches read the equivalent private repository configuration.
    let proxy = session
        .manifest
        .settings
        .proxy_url
        .trim_end_matches('/')
        .to_owned();
    let capability_origin = if controller.is_some() {
        proxy
            .rsplit_once('/')
            .filter(|(_, suffix)| {
                suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .map(|(origin, _)| origin.to_owned())
    } else {
        None
    };
    if let Some(origin) = &capability_origin {
        let token = proxy.rsplit('/').next().expect("validated capability");
        clone
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", format!("http.{origin}/.extraHeader"))
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("authorization: Bearer {token}"),
            );
    }
    clone
        .args([
            "--depth",
            "1",
            "--branch",
            &session.manifest.settings.reference,
        ])
        .arg(format!(
            "{}/git/{}.git",
            capability_origin.as_deref().unwrap_or(&proxy),
            session.manifest.settings.proxy_repo
        ))
        .arg(session.root.join("repo.git"));
    session.run("clone", &mut clone)?;
    if let Some(origin) = &capability_origin {
        let token = proxy.rsplit('/').next().expect("validated capability");
        let mut config = OpenOptions::new()
            .append(true)
            .open(session.root.join("repo.git/config"))?;
        writeln!(
            config,
            "\n[http \"{origin}/\"]\n\textraHeader = authorization: Bearer {token}"
        )?;
        config.sync_all()?;
    }
    session.start_with_controller(controller)?;
    session.report_with_controller(controller)
}

pub fn resume(root: &Path) -> Result<Report> {
    let mut session = Session::open(root)?;
    session.start()?;
    session.report()
}
pub fn resume_with_controller(root: &Path, controller: &dyn MountController) -> Result<Report> {
    let mut session = Session::open(root)?;
    session.start_with_controller(Some(controller))?;
    session.report_with_controller(Some(controller))
}
pub fn status(root: &Path) -> Result<Report> {
    Session::open(root)?.report()
}

pub fn status_with_controller(root: &Path, controller: &dyn MountController) -> Result<Report> {
    Session::open(root)?.report_with_controller(Some(controller))
}

pub fn build(root: &Path, args: &[String]) -> Result<Report> {
    bazel_operation(root, "build", args, None)
}

pub fn test(root: &Path, args: &[String]) -> Result<Report> {
    bazel_operation(root, "test", args, None)
}

pub fn build_with_controller(
    root: &Path,
    args: &[String],
    controller: &dyn MountController,
) -> Result<Report> {
    bazel_operation(root, "build", args, Some(controller))
}

pub fn test_with_controller(
    root: &Path,
    args: &[String],
    controller: &dyn MountController,
) -> Result<Report> {
    bazel_operation(root, "test", args, Some(controller))
}

fn bazel_operation(
    root: &Path,
    operation: &str,
    args: &[String],
    controller: Option<&dyn MountController>,
) -> Result<Report> {
    ensure!(
        !args.is_empty(),
        "Bazel operation requires at least one target"
    );
    let mut session = Session::open(root)?;
    if session.manifest.settings.repo_contents_cache.is_some() {
        ensure!(
            !args.iter().any(|arg| matches!(
                arg.as_str(),
                "--repo_contents_cache" | "--repository_contents_cache"
            ) || arg.starts_with("--repo_contents_cache=")
                || arg.starts_with("--repository_contents_cache=")),
            "repo contents cache is pinned by session settings; select it at create time rather than overriding build arguments"
        );
    }
    session.ready_with_controller(controller)?;
    session.check_shared_cache()?;
    let mut command = session.bazel();
    command.arg(operation);
    let enforced = controller
        .map(|c| c.bazel_args())
        .transpose()?
        .unwrap_or_default();
    if session.manifest.settings.repo_contents_cache.is_some() || !enforced.is_empty() {
        // A trailing explicit value overrides earlier --config expansions. Keep
        // it before an optional target separator so Bazel still parses it.
        let separator = args
            .iter()
            .position(|arg| arg == "--")
            .unwrap_or(args.len());
        command.args(&args[..separator]);
        if let Some(cache) = &session.manifest.settings.repo_contents_cache {
            command.arg(format!("--repo_contents_cache={}", cache.display()));
        }
        command.args(enforced);
        command.args(&args[separator..]);
    } else {
        command.args(args);
    }
    session.run(operation, &mut command)?;
    session.manifest.last_successful_build_unix_ms =
        Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis());
    session.manifest.phase = "edit_ready".into();
    session.persist()?;
    session.report_with_controller(controller)
}

pub fn save(root: &Path, args: &[String]) -> Result<Report> {
    save_inner(root, args, None)
}

pub fn save_with_controller(
    root: &Path,
    args: &[String],
    controller: &dyn MountController,
) -> Result<Report> {
    save_inner(root, args, Some(controller))
}

fn save_inner(
    root: &Path,
    args: &[String],
    controller: Option<&dyn MountController>,
) -> Result<Report> {
    let mut session = Session::open(root)?;
    session.ready_with_controller(controller)?;
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
    session.report_with_controller(controller)
}

pub fn stop(root: &Path) -> Result<Report> {
    stop_inner(root, None)
}

pub fn stop_with_controller(root: &Path, controller: &dyn MountController) -> Result<Report> {
    stop_inner(root, Some(controller))
}

fn stop_inner(root: &Path, controller: Option<&dyn MountController>) -> Result<Report> {
    let mut session = Session::open(root)?;
    ensure!(
        session.manifest.daemon_managed == controller.is_some(),
        "session lifecycle owner differs; use workspace stop for daemon-managed sessions"
    );
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
    if let Some(controller) = controller {
        let started = Instant::now();
        controller.unmount(&session.root)?;
        session
            .timings
            .push(("vfs_shutdown".into(), started.elapsed().as_millis()));
    } else if let Some(process) = session
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
    session.report_with_controller(controller)
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
