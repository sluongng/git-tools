//! Explicit, fail-closed recovery of persisted, disconnected Linux FUSE mounts.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    id: u64,
    device: String,
    root: PathBuf,
    filesystem: String,
    source: String,
    owner: u32,
}
/// Native mount numbers are meaningful only within their original boot and
/// mount namespace. Keep this proof separate from legacy Git/BOS records.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeMountProof {
    pub kernel: Identity,
    context: MountContext,
    pub namespace_identity: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MountContext {
    boot_id: String,
    namespace_device: u64,
    namespace_inode: u64,
}
#[cfg(target_os = "linux")]
fn boot_id(input: &str) -> Result<String> {
    let value = input.strip_suffix('\n').unwrap_or(input);
    ensure!(
        value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }
            }),
        "invalid Linux boot identity"
    );
    Ok(value.to_owned())
}
impl MountContext {
    fn current() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use std::io::Read;
            use std::os::unix::fs::MetadataExt;
            let mut bytes = String::new();
            std::fs::File::open("/proc/sys/kernel/random/boot_id")?
                .take(65)
                .read_to_string(&mut bytes)?;
            let namespace = std::fs::metadata("/proc/self/ns/mnt")?;
            ensure!(
                namespace.ino() != 0,
                "invalid Linux mount namespace identity"
            );
            Ok(Self {
                boot_id: boot_id(&bytes)?,
                namespace_device: namespace.dev(),
                namespace_inode: namespace.ino(),
            })
        }
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("native disconnected-mount recovery currently requires Linux")
    }
}
impl NativeMountProof {
    pub fn capture(path: &Path, source: &str, namespace_identity: &str) -> Result<Self> {
        let context = MountContext::current()?;
        let kernel = identity(path)?.context("native kernel mount identity missing")?;
        ensure!(
            kernel.source == source
                && matches!(kernel.filesystem.as_str(), "fuse" | "fuse.git-tools-native"),
            "native mount is not the expected source filesystem"
        );
        #[cfg(unix)]
        ensure!(
            kernel.owner == unsafe { libc::geteuid() },
            "native mount belongs to another user"
        );
        ensure!(
            !namespace_identity.is_empty() && namespace_identity.len() <= 128,
            "native namespace identity is invalid"
        );
        ensure!(
            context == MountContext::current()?,
            "mount context changed during capture"
        );
        Ok(Self {
            kernel,
            context,
            namespace_identity: namespace_identity.to_owned(),
        })
    }
    pub fn detach(&self, path: &Path, source: &str) -> Result<bool> {
        ensure!(
            self.context == MountContext::current()?,
            "native mount boot or mount namespace changed; refuse unverified recovery"
        );
        detach(path, Some(&self.kernel), source)
    }
}

#[cfg(target_os = "linux")]
fn unescape(value: &str) -> Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let input = value.as_bytes();
    let mut output = Vec::new();
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'\\' {
            ensure!(index + 3 < input.len(), "invalid mountinfo escape");
            let digits = &input[index + 1..index + 4];
            ensure!(
                digits.iter().all(|digit| (b'0'..=b'7').contains(digit)),
                "invalid mountinfo escape"
            );
            let byte = u16::from(digits[0] - b'0') * 64
                + u16::from(digits[1] - b'0') * 8
                + u16::from(digits[2] - b'0');
            ensure!(byte <= 255, "invalid mountinfo escape");
            output.push(byte as u8);
            index += 4;
        } else {
            output.push(input[index]);
            index += 1;
        }
    }
    Ok(std::ffi::OsString::from_vec(output).into())
}
#[cfg(target_os = "linux")]
fn parse(input: &str, path: &Path) -> Result<Option<Identity>> {
    let mut found = None;
    for line in input.lines() {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        ensure!(fields.len() >= 10, "invalid mountinfo record");
        if unescape(fields[4])? != path {
            continue;
        }
        ensure!(
            found.is_none(),
            "stacked mounts at recovery target; manual inspection required"
        );
        let separator = fields
            .iter()
            .position(|field| *field == "-")
            .context("invalid mountinfo separator")?;
        ensure!(separator + 3 < fields.len(), "invalid mountinfo filesystem");
        let owner = fields[separator + 3]
            .split(',')
            .find_map(|option| option.strip_prefix("user_id="))
            .context("mount has no FUSE owner identity")?
            .parse()?;
        found = Some(Identity {
            id: fields[0].parse()?,
            device: fields[2].into(),
            root: unescape(fields[3])?,
            filesystem: fields[separator + 1].into(),
            source: fields[separator + 2].into(),
            owner,
        });
    }
    Ok(found)
}
#[cfg(target_os = "linux")]
pub fn identity(path: &Path) -> Result<Option<Identity>> {
    parse(&std::fs::read_to_string("/proc/self/mountinfo")?, path)
}
#[cfg(not(target_os = "linux"))]
pub fn identity(_: &Path) -> Result<Option<Identity>> {
    anyhow::bail!("explicit stale-mount recovery currently requires Linux")
}
fn wait(mut child: std::process::Child, budget: Duration) -> Result<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if start.elapsed() >= budget {
            let _ = child.kill();
            // A kernel filesystem wait can remain uninterruptible even after
            // SIGKILL. Reap asynchronously; never let that defeat the probe's
            // deadline or authorize an unmount of an unproven connection.
            let _ = std::thread::Builder::new()
                .name("recovery-child-reaper".into())
                .spawn(move || {
                    let _ = child.wait();
                });
            anyhow::bail!(
                "recovery probe/helper timed out; no unmount authorized; inspect the mount manually"
            )
        };
        std::thread::sleep(Duration::from_millis(10));
    }
}
/// Called only with the daemon's exclusive owner lock, or its own registry lock.
pub fn detach(path: &Path, expected: Option<&Identity>, source: &str) -> Result<bool> {
    let Some(current) = identity(path)? else {
        return Ok(false);
    };
    let expected = expected.context(
        "no persisted mount identity; refuse unverified recovery; inspect and unmount manually",
    )?;
    ensure!(
        &current == expected,
        "mount identity changed; refuse recovery of a replacement mount"
    );
    ensure!(
        matches!(
            current.filesystem.as_str(),
            "fuse" | "fuse.git-vfs" | "fuse.git-tools-outputs" | "fuse.git-tools-native"
        ) && current.source == source,
        "mount is not the expected project FUSE filesystem"
    );
    #[cfg(unix)]
    ensure!(
        current.owner == unsafe { libc::geteuid() },
        "mount belongs to another user"
    );
    // A child bounds a potentially hanging kernel lookup. We only kill/reap our
    // own unreaped probe, never an unrelated PID loaded from disk.
    let probe = Command::new(std::env::current_exe()?)
        .arg("mount-probe")
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    ensure!(
        wait(probe, Duration::from_millis(500))?.code() == Some(72),
        "mount is not proven disconnected (ENOTCONN); live or unresponsive mounts must be stopped by their owner"
    );
    ensure!(
        identity(path)?.as_ref() == Some(expected),
        "mount identity changed during probe; refuse recovery"
    );
    let unmount = |lazy: bool| -> Result<std::process::ExitStatus> {
        let mut command = Command::new("fusermount3");
        command.arg("-u");
        if lazy {
            command.arg("-z");
        }
        command
            .arg("--")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        wait(
            command
                .spawn()
                .context("start fusermount3; install the unprivileged FUSE helper")?,
            Duration::from_secs(2),
        )
    };
    if !unmount(false)?.success() {
        ensure!(
            identity(path)?.as_ref() == Some(expected),
            "mount identity changed; refuse lazy detach"
        );
        ensure!(
            unmount(true)?.success(),
            "verified dead mount could not be detached; inspect manually"
        );
    }
    ensure!(
        identity(path)?.is_none(),
        "mount remains after recovery; inspect manually"
    );
    Ok(true)
}
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn parses_escaped_target_and_rejects_stacked_mounts() {
        let line = "55 1 0:40 / /tmp/a\\040b rw - fuse git-vfs rw,user_id=1000,group_id=1000\n";
        let found = parse(line, Path::new("/tmp/a b")).unwrap().unwrap();
        assert_eq!(found.id, 55);
        assert_eq!(found.owner, 1000);
        assert!(parse(&line.repeat(2), Path::new("/tmp/a b")).is_err());
        assert!(parse(line, Path::new("/tmp/other")).unwrap().is_none());
    }
    #[test]
    fn native_recovery_rejects_changed_boot_or_namespace_before_any_detach() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("not-mounted");
        let line = "55 1 0:40 / /tmp/example rw - fuse git-tools-native-example rw,user_id=1000,group_id=1000\n";
        let kernel = parse(line, Path::new("/tmp/example")).unwrap().unwrap();
        let context = MountContext::current().unwrap();
        let mut proof = NativeMountProof {
            kernel,
            context: context.clone(),
            namespace_identity: "namespace".into(),
        };
        proof.context.namespace_inode = proof.context.namespace_inode.wrapping_add(1);
        assert!(proof.detach(&target, "git-tools-native-example").is_err());
        proof.context = context;
        proof.context.boot_id = "00000000-0000-0000-0000-000000000000".into();
        assert!(proof.detach(&target, "git-tools-native-example").is_err());
        assert!(!target.exists());
        assert!(boot_id(&"a".repeat(65)).is_err());
        assert!(boot_id("00000000-0000-0000-0000-000000000000\n").is_ok());
    }
}
