use anyhow::{Result, ensure};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::Read, path::Path, process::Stdio, time::Duration};
use tokio::io::AsyncWriteExt;
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
    eligible: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessEnvelope {
    state: ProcessRecord,
    checksum: String,
}
fn record(
    directory: &PrivateDir,
    name: &str,
    group: Option<i32>,
    quiescent: bool,
    eligible: bool,
) -> Result<()> {
    let state = ProcessRecord {
        schema: 1,
        group,
        quiescent,
        eligible,
    };
    let checksum = blake3::hash(&serde_json::to_vec(&state)?)
        .to_hex()
        .to_string();
    directory.replace(
        name,
        &serde_json::to_vec(&ProcessEnvelope { state, checksum })?,
    )?;
    Ok(())
}
pub(crate) fn allows_verdict(directory: &PrivateDir) -> bool {
    let result = (|| -> Result<bool> {
        let envelope: ProcessEnvelope =
            serde_json::from_slice(&directory.read("run-0.stdout.process.json", 1024)?)?;
        ensure!(
            envelope.state.schema == 1
                && envelope.checksum
                    == blake3::hash(&serde_json::to_vec(&envelope.state)?)
                        .to_hex()
                        .to_string(),
            "checker transport checksum differs"
        );
        Ok(envelope.state.quiescent && envelope.state.eligible)
    })();
    result.unwrap_or(false)
}
/// Fixed current wrapper-record category only. Does not inspect or signal PIDs.
pub(crate) fn inspect_transport(directory: &PrivateDir, name: &str) -> i32 {
    let bytes = match directory.read(name, 1024) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 1,
        Err(_) => return 2,
    };
    let Ok(envelope) = serde_json::from_slice::<ProcessEnvelope>(&bytes) else {
        return 2;
    };
    let Ok(encoded) = serde_json::to_vec(&envelope.state) else {
        return 2;
    };
    if envelope.state.schema != 1
        || envelope.state.group.is_some_and(|group| group <= 0)
        || envelope.checksum != blake3::hash(&encoded).to_hex().to_string()
    {
        return 2;
    }
    if !envelope.state.quiescent {
        3
    } else if envelope.state.eligible {
        4
    } else {
        5
    }
}
pub(crate) struct Output {
    pub bytes: Vec<u8>,
    pub eligible: bool,
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
    #[cfg(unix)]
    unsafe {
        libc::kill(-group, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while group_active(group)? && Instant::now() < deadline {
        sleep(Duration::from_millis(25)).await;
    }
    {
        // Signal even after a false-empty /proc census; the unreaped leader
        // still reserves this owned group identity.
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
    request: &[u8],
    stdout: &Path,
    stderr: &Path,
    timeout: Duration,
    grace: Duration,
    cancellation: watch::Receiver<bool>,
) -> Result<Output> {
    ensure!(
        request.len() <= tinytree_check_runner::MAX_REQUEST_BYTES,
        "checker request exceeds limit"
    );
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
        .stdin(Stdio::piped())
        .stdout(open(stdout)?)
        .stderr(open(stderr)?)
        .kill_on_drop(true);
    let parent = PrivateDir::open_existing(
        stdout
            .parent()
            .ok_or_else(|| anyhow::anyhow!("missing checker attempt directory"))?,
    )?;
    let record_name = format!(
        "{}.process.json",
        stdout
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid checker process name"))?
    );
    record(&parent, &record_name, None, false, false)?;
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            record(&parent, &record_name, None, true, false)?;
            return Err(error.into());
        }
    };
    let group = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("missing checker identity"))? as i32;
    if let Err(error) = record(&parent, &record_name, Some(group), false, false) {
        teardown(group).await.ok();
        child.start_kill().ok();
        tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .ok();
        return Err(error);
    }
    let deadline = Instant::now() + timeout;
    let input = async {
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("checker input is unavailable"))?;
        input.write_all(request).await?;
        input.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let mut failed = tokio::time::timeout(Duration::from_secs(10), input)
        .await
        .map_or(true, |r| r.is_err());
    let mut stopping = None;
    let result = loop {
        let output_limit = std::fs::metadata(stdout)
            .is_ok_and(|m| m.len() > tinytree_check_runner::MAX_RECEIPT_BYTES as u64)
            || std::fs::metadata(stderr).is_ok_and(|m| m.len() > 65536);
        failed |= *cancellation.borrow() || Instant::now() >= deadline || output_limit;
        if let Some(status) = exited_without_reaping(group)? {
            break status;
        }
        if failed {
            failed = true;
            if stopping.is_none() {
                #[cfg(unix)]
                unsafe {
                    libc::kill(-group, libc::SIGTERM);
                }
                stopping = Some(Instant::now() + grace);
            }
        }
        if stopping.is_some_and(|end| Instant::now() >= end) {
            #[cfg(unix)]
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
            break -1;
        }
        sleep(Duration::from_millis(25)).await;
    };
    teardown(group).await?;
    child.wait().await?;
    let eligible = !failed
        && Instant::now() < deadline
        && !*cancellation.borrow()
        && std::fs::metadata(stdout)?.len() <= tinytree_check_runner::MAX_RECEIPT_BYTES as u64
        && std::fs::metadata(stderr)?.len() <= 65536;
    // Default-false eligibility is durable before spawn; only complete,
    // uninterrupted owned transport can ACK eligibility. A lost/failed ACK
    // therefore allows cleanup but never resurrects a late passing verdict.
    record(&parent, &record_name, Some(group), true, eligible)?;
    ensure!(result == 0, "checker process did not complete successfully");
    // Even a cancellation/timeout can return a valid confirmed cleanup receipt.
    // It is reconciled durably; callers independently decide whether to publish.
    let bytes = parent.read(
        stdout
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("missing checker output"))?,
        tinytree_check_runner::MAX_RECEIPT_BYTES,
    )?;
    ensure!(
        std::fs::metadata(stderr)?.len() <= 65536,
        "checker diagnostics exceed limit"
    );
    Ok(Output { bytes, eligible })
}
