use anyhow::{Result, ensure};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    process::Command,
    sync::watch,
    time::{Instant, sleep},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessRecord {
    schema: u32,
    group: Option<i32>,
    quiescent: bool,
}
fn record(directory: &PrivateDir, name: &str, group: Option<i32>, quiescent: bool) -> Result<()> {
    directory.replace(
        name,
        &serde_json::to_vec(&ProcessRecord {
            schema: 1,
            group,
            quiescent,
        })?,
    )?;
    Ok(())
}
/// Crash recovery never kills a possibly reused PID/group. An unrecorded spawn
/// or still-live group requires operator recovery and retains the source mount.
pub(crate) fn require_quiescent(root: &Path) -> Result<()> {
    let directory = PrivateDir::open_existing(root)?;
    let mut count = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        count += 1;
        ensure!(count <= 1024, "process inventory budget exceeded");
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.ends_with(".process.json") {
            continue;
        }
        let state: ProcessRecord = serde_json::from_slice(&directory.read(name, 1024)?)?;
        ensure!(state.schema == 1, "invalid process ownership record");
        if state.quiescent {
            continue;
        }
        let group = state
            .group
            .ok_or_else(|| anyhow::anyhow!("spawn ownership requires operator recovery"))?;
        ensure!(!group_active(group)?, "process group remains active");
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn group_active(group: i32) -> Result<bool> {
    ensure!(group > 0, "invalid process group");
    let mut count = 0;
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|b| b.is_ascii_digit())
        {
            continue;
        }
        count += 1;
        ensure!(count <= 65536, "process inventory budget exceeded");
        let file = match std::fs::File::open(entry.path().join("stat")) {
            Ok(file) => file,
            Err(e) if matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => continue,
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        match file.take(4097).read_to_end(&mut bytes) {
            Ok(_) => {}
            Err(e) if matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => continue,
            Err(e) => return Err(e.into()),
        }
        ensure!(bytes.len() <= 4096, "process stat budget exceeded");
        let split = bytes
            .iter()
            .rposition(|b| *b == b')')
            .ok_or_else(|| anyhow::anyhow!("invalid process state"))?;
        let rest = std::str::from_utf8(&bytes[split + 1..])?;
        let mut fields = rest.split_whitespace();
        let state = fields.next();
        fields.next();
        let pgid = fields.next().and_then(|s| s.parse::<i32>().ok());
        if pgid == Some(group) && !matches!(state, Some("Z" | "X")) {
            return Ok(true);
        }
    }
    Ok(false)
}
#[cfg(not(target_os = "linux"))]
fn group_active(_: i32) -> Result<bool> {
    anyhow::bail!("native process-group proof requires Linux")
}
async fn teardown(group: i32) -> Result<()> {
    if !group_active(group)? {
        return Ok(());
    }
    #[cfg(unix)]
    unsafe {
        libc::kill(-group, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while group_active(group)? && Instant::now() < deadline {
        sleep(Duration::from_millis(25)).await;
    }
    if group_active(group)? {
        #[cfg(unix)]
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while group_active(group)? && Instant::now() < deadline {
            sleep(Duration::from_millis(25)).await;
        }
    }
    ensure!(!group_active(group)?, "process group is not quiescent");
    Ok(())
}
#[cfg(target_os = "linux")]
fn exited_without_reaping(child: i32) -> Result<Option<i32>> {
    let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            child as libc::id_t,
            &mut status,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            return Ok(None);
        }
        return Err(error.into());
    }
    if unsafe { status.si_pid() } == 0 {
        return Ok(None);
    }
    Ok(Some(if status.si_code == libc::CLD_EXITED {
        unsafe { status.si_status() }
    } else {
        -1
    }))
}
#[cfg(not(target_os = "linux"))]
fn exited_without_reaping(_: i32) -> Result<Option<i32>> {
    anyhow::bail!("native child identity proof requires Linux")
}
pub(crate) async fn execute(
    mut command: Command,
    stdout: &Path,
    stderr: &Path,
    timeout: Duration,
    limits: &[(PathBuf, u64)],
    mut cancellation: Option<&mut watch::Receiver<bool>>,
) -> Result<i32> {
    let open = |path: &Path| -> Result<std::fs::File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        Ok(options.open(path)?)
    };
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    command
        .stdin(Stdio::null())
        .stdout(open(stdout)?)
        .stderr(open(stderr)?)
        .kill_on_drop(true);
    let parent = PrivateDir::open_existing(
        stdout
            .parent()
            .ok_or_else(|| anyhow::anyhow!("missing process journal parent"))?,
    )?;
    let record_name = format!(
        "{}.process.json",
        stdout
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid process journal name"))?
    );
    record(&parent, &record_name, None, false)?;
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            record(&parent, &record_name, None, true)?;
            return Err(error.into());
        }
    };
    let group = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("missing child identity"))? as i32;
    if let Err(error) = record(&parent, &record_name, Some(group), false) {
        teardown(group).await.ok();
        // The durable starting record remains uncertain. Do not block forever
        // if both recording and group inspection fail; direct-child identity
        // is still owned, and recovery must preserve the source authority.
        child.start_kill().ok();
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .ok();
        return Err(error);
    }
    let deadline = Instant::now() + timeout;
    let result = loop {
        if let Some(status) = exited_without_reaping(group)? {
            break Ok(status);
        }
        if cancellation
            .as_mut()
            .is_some_and(|receiver| *receiver.borrow())
        {
            break Err(anyhow::anyhow!("check_cancelled"));
        }
        if Instant::now() >= deadline {
            break Err(anyhow::anyhow!("process_timeout"));
        }
        if limits
            .iter()
            .any(|(path, max)| std::fs::metadata(path).is_ok_and(|m| m.len() > *max))
        {
            break Err(anyhow::anyhow!("process_output_limit"));
        }
        sleep(Duration::from_millis(25)).await;
    };
    // WNOWAIT keeps the exited leader as a PID/PGID identity anchor. Never reap
    // it before group teardown: a released numeric PGID could be reused by an
    // unrelated process. Tokio's owned child is not polled/reaped until below.
    teardown(group).await?;
    child.wait().await?;
    record(&parent, &record_name, Some(group), true)?;
    ensure!(
        !limits
            .iter()
            .any(|(path, max)| std::fs::metadata(path).is_ok_and(|m| m.len() > *max)),
        "process_output_limit"
    );
    result
}
