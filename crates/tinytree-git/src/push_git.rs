//! Independent transport owner: no exporter writer lease reaches network I/O.
use super::disk::Disk;
use anyhow::{Context, Result, ensure};
use private_fs::PrivateDir;
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, sync::watch, time::Instant};
pub(super) struct Owner {
    pub disk: Arc<Disk>,
    pub local: PrivateDir,
    pub program: PathBuf,
    pub cancel: watch::Receiver<bool>,
}
impl Owner {
    pub async fn raw(
        self: &Arc<Self>,
        args: Vec<String>,
        maximum: usize,
        deadline: Instant,
    ) -> Result<(ExitStatus, Vec<u8>)> {
        ensure!(
            Instant::now() < deadline,
            "push operation deadline exceeded"
        );
        let owner = self.clone();
        let job = tokio::spawn(async move {
            ensure!(!*owner.cancel.borrow(), "push cancelled before admission");
            let mut command = Command::new(&owner.program);
            command.env_clear();
            for key in [
                "PATH",
                "TEST_SRCDIR",
                "TEST_WORKSPACE",
                "RUNFILES_DIR",
                "RUNFILES_MANIFEST_FILE",
            ] {
                if let Some(value) = std::env::var_os(key) {
                    command.env(key, value);
                }
            }
            command
                .env("HOME", owner.disk.dir.path())
                .env("XDG_CONFIG_HOME", owner.disk.dir.path())
                .env("TMPDIR", owner.disk.dir.path())
                .env("LC_ALL", "C")
                .env("TZ", "UTC")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_ASKPASS", "/bin/false")
                .env("GIT_ATTR_NOSYSTEM", "1")
                .env("GIT_ALLOW_PROTOCOL", "file:https")
                .args(["--no-pager", "--no-replace-objects", "--git-dir"])
                .arg(owner.local.path())
                .args([
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "gc.auto=0",
                    "-c",
                    "maintenance.auto=false",
                    "-c",
                    "credential.helper=",
                    "-c",
                    "http.sslVerify=true",
                    "-c",
                    "http.followRedirects=false",
                    "-c",
                    "push.followTags=false",
                    "-c",
                    "core.fsync=all",
                    "-c",
                    "core.fsyncMethod=fsync",
                ])
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let fence = owner.disk.lock.as_raw_fd();
            let (mut lifetime, inherited) = tokio::net::UnixStream::pair()?;
            let lifetime_fd = inherited.as_raw_fd();
            unsafe {
                command.pre_exec(move || {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    for fd in [fence, lifetime_fd] {
                        let flags = libc::fcntl(fd, libc::F_GETFD);
                        if flags < 0
                            || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
                        {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
            let mut child = command
                .spawn()
                .map_err(|_| anyhow::anyhow!("cannot start push Git worker"))?;
            drop(inherited); // Only accepted physical descendants retain the writer.
            let pid = child.id().map(|p| p as i32);
            let leader = pid.and_then(|p| info(p).ok().flatten());
            let stdout = child.stdout.take();
            // Every post-spawn failure joins the same cleanup path, never `?`-returns
            // past accepted subprocess ownership.
            let result = if let (Some(_), Some(_), Some(stdout)) = (pid, leader, stdout) {
                let transfer = async {
                    let mut output = Vec::new();
                    stdout
                        .take(maximum as u64 + 1)
                        .read_to_end(&mut output)
                        .await?;
                    ensure!(
                        output.len() <= maximum,
                        "push Git output byte bound exceeded"
                    );
                    let status = child.wait().await?;
                    Ok::<_, anyhow::Error>((status, output))
                };
                let mut cancel = owner.cancel.clone();
                tokio::select! {biased;_=super::stopped(&mut cancel)=>Err(anyhow::anyhow!("push Git worker cancelled; outcome unknown")),r=tokio::time::timeout_at(deadline,transfer)=>r.unwrap_or_else(|_|Err(anyhow::anyhow!("push Git deadline exceeded; outcome unknown")))}
            } else {
                Err(anyhow::anyhow!(
                    "push subprocess setup failed; outcome unknown"
                ))
            };
            let _ = child.start_kill();
            // Only EOF proves supported descendants closed their inherited ownership
            // endpoint. /proc snapshots target cleanup, never prove an empty process tree.
            // Trusted transports must preserve this descriptor and their owned session.
            loop {
                if let (Some(pid), Some(leader)) = (pid, leader) {
                    let _ = drain_session(pid, leader.start);
                }
                let mut byte = [0u8; 1];
                match tokio::time::timeout(Duration::from_millis(20), lifetime.read(&mut byte))
                    .await
                {
                    Ok(Ok(0)) => break,
                    Ok(Err(_)) => {
                        std::future::pending::<()>().await;
                    }
                    _ => {}
                }
            }
            let _ = child.wait().await;
            drop(owner);
            result
        });
        tokio::time::timeout_at(deadline, job)
            .await
            .map_err(|_| anyhow::anyhow!("push Git worker physically draining; outcome unknown"))?
            .map_err(|_| anyhow::anyhow!("push Git worker failed; outcome unknown"))?
    }
    pub async fn run(
        self: &Arc<Self>,
        args: Vec<String>,
        maximum: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>> {
        let (status, bytes) = self.raw(args, maximum, deadline).await?;
        ensure!(
            status.success(),
            "push Git operation rejected; outcome unknown"
        );
        Ok(bytes)
    }
}
#[derive(Clone, Copy)]
struct Process {
    session: i32,
    start: u64,
    zombie: bool,
}
fn info(pid: i32) -> Result<Option<Process>> {
    let text = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let rest = text.rsplit_once(')').context("invalid process stat")?.1;
    let f = rest.split_whitespace().collect::<Vec<_>>();
    ensure!(f.len() > 19, "short process stat");
    Ok(Some(Process {
        session: f[3].parse()?,
        start: f[19].parse()?,
        zombie: matches!(f[0], "Z" | "X"),
    }))
}
/// Returns true while any live owned-session work remains. A reused leader
/// identifies a different session, never an authority to signal that session.
fn drain_session(session: i32, start: u64) -> Result<bool> {
    if info(session)?.is_some_and(|p| p.start != start) {
        return Ok(false);
    }
    let mut live = false;
    for (index, entry) in std::fs::read_dir("/proc")?.enumerate() {
        ensure!(index < 65536, "push process inventory bound exceeded");
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        let Some(p) = info(pid)? else { continue };
        if p.session != session || p.zombie {
            continue;
        }
        live = true;
        let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
        if descriptor < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                continue;
            }
            anyhow::bail!("cannot own push descendant");
        }
        let fd = unsafe { File::from_raw_fd(descriptor) };
        if info(pid)?
            .is_some_and(|now| now.session == session && now.start == p.start && !now.zombie)
        {
            let result = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if result < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
                anyhow::bail!("cannot stop push descendant");
            }
        }
    }
    Ok(live)
}
