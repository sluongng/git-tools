use super::state::Resources;
use anyhow::{Context, Result, ensure};
use std::{
    os::fd::AsRawFd,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::Instant,
};
#[derive(Clone)]
pub(super) struct Git {
    pub owner: Arc<Resources>,
    program: PathBuf,
    pub deadline: Instant,
}
impl Git {
    pub fn new(owner: Arc<Resources>, deadline: Instant) -> Result<Self> {
        let requested = &owner.options.git_program;
        let program = if requested.is_absolute() {
            requested.clone()
        } else if requested.components().count() == 1 {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .map(|p| p.join(requested))
                .find(|p| p.is_file())
                .context("Git executable unavailable")?
        } else {
            std::env::current_dir()?.join(requested)
        }
        .canonicalize()?;
        Ok(Self {
            owner,
            program,
            deadline,
        })
    }
    pub async fn raw(
        &self,
        args: Vec<String>,
        input: Vec<u8>,
        maximum: usize,
    ) -> Result<(ExitStatus, Vec<u8>)> {
        ensure!(
            Instant::now() < self.deadline,
            "Git operation deadline exceeded"
        );
        let owner = self.owner.clone();
        let program = self.program.clone();
        let deadline = self.deadline;
        let job = tokio::spawn(async move {
            let mut command = Command::new(program);
            command.env_clear();
            // No source credentials, ambient Git configuration, replacements,
            // filters, hooks or automatic maintenance reach a child process.
            for name in [
                "PATH",
                "TEST_SRCDIR",
                "TEST_WORKSPACE",
                "RUNFILES_DIR",
                "RUNFILES_MANIFEST_FILE",
            ] {
                if let Some(value) = std::env::var_os(name) {
                    command.env(name, value);
                }
            }
            command
                .env("HOME", owner.state.path())
                .env("XDG_CONFIG_HOME", owner.state.path())
                .env("TMPDIR", owner.spool.path())
                .env("LC_ALL", "C")
                .env("TZ", "UTC")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_ATTR_NOSYSTEM", "1")
                .env("GIT_NO_LAZY_FETCH", "1");
            command
                .args([
                    "--no-pager",
                    "--no-replace-objects",
                    "--no-optional-locks",
                    "--no-lazy-fetch",
                    "-C",
                ])
                .arg(owner.options.source_git.as_path())
                .args([
                    "-c",
                    "core.fsync=all",
                    "-c",
                    "core.fsyncMethod=fsync",
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "gc.auto=0",
                    "-c",
                    "maintenance.auto=false",
                ])
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let lock = owner.lock.as_raw_fd();
            // Inherit an independent copy of the flock's open-file description.
            // Even abrupt adapter death cannot free the writer lock while an
            // accepted Git writer survives. No credential/socket is inherited.
            unsafe {
                command.pre_exec(move || {
                    if libc::dup2(lock, 198) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::fcntl(198, libc::F_SETFD, 0) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let mut child = command.spawn().context("cannot start Git object worker")?;
            let mut stdin = child.stdin.take().context("missing Git input")?;
            let stdout = child.stdout.take().context("missing Git output")?;
            let transfer = async move {
                let write = async move {
                    stdin.write_all(&input).await?;
                    drop(stdin);
                    Ok::<_, std::io::Error>(())
                };
                let read = async move {
                    let mut output = Vec::new();
                    stdout
                        .take(maximum as u64 + 1)
                        .read_to_end(&mut output)
                        .await?;
                    Ok::<_, std::io::Error>(output)
                };
                let (_, output) = tokio::try_join!(write, read)?;
                ensure!(output.len() <= maximum, "Git output byte budget exceeded");
                Ok::<_, anyhow::Error>(output)
            };
            let output = match tokio::time::timeout_at(deadline, transfer).await {
                Ok(Ok(output)) => output,
                _ => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    anyhow::bail!("Git object worker expired or failed; state retained");
                }
            };
            let status = match tokio::time::timeout_at(deadline, child.wait()).await {
                Ok(status) => status?,
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    anyhow::bail!("Git object worker deadline exceeded; state retained");
                }
            };
            drop(owner);
            Ok((status, output))
        });
        // On logical timeout, the accepted job continues kill+wait with its
        // physical flock owner. Dropping the JoinHandle never drops that owner.
        tokio::time::timeout_at(self.deadline, job)
            .await
            .map_err(|_| anyhow::anyhow!("Git worker is draining after deadline"))?
            .map_err(|_| anyhow::anyhow!("Git worker failed; reconcile private state"))?
    }
    pub async fn run(&self, args: &[&str], input: Vec<u8>, maximum: usize) -> Result<Vec<u8>> {
        let (status, output) = self
            .raw(args.iter().map(|s| s.to_string()).collect(), input, maximum)
            .await?;
        ensure!(
            status.success(),
            "Git operation rejected; adapter state retained"
        );
        Ok(output)
    }

    pub async fn read(&self, id: &str, expected: &str, maximum: usize) -> Result<Vec<u8>> {
        ensure!(
            super::state::valid_hex(id, 40),
            "history mode requires pinned SHA1 object IDs"
        );
        let output = self
            .run(
                &["cat-file", "--batch"],
                format!("{id}\n").into_bytes(),
                maximum
                    .checked_add(128)
                    .context("Git output bound overflow")?,
            )
            .await?;
        let newline = output
            .iter()
            .position(|b| *b == b'\n')
            .context("invalid Git batch framing")?;
        let header = std::str::from_utf8(&output[..newline])?
            .split(' ')
            .collect::<Vec<_>>();
        ensure!(
            header.len() == 3 && header[0] == id && header[1] == expected,
            "Git object kind or identity differs"
        );
        let size: usize = header[2].parse()?;
        ensure!(
            size <= maximum
                && output.len() == newline + 1 + size + 1
                && output.last() == Some(&b'\n'),
            "Git object size/framing exceeds limits"
        );
        let bytes = output[newline + 1..newline + 1 + size].to_vec();
        verify(id, expected, &bytes)?;
        Ok(bytes)
    }
    pub async fn validate(&self) -> Result<()> {
        ensure!(
            self.run(&["rev-parse", "--show-object-format"], vec![], 128)
                .await?
                == b"sha1\n",
            "history mode supports SHA1 Git only"
        );
        ensure!(
            self.run(&["rev-parse", "--is-shallow-repository"], vec![], 128)
                .await?
                == b"false\n",
            "shallow history is unsupported"
        );
        let path = self
            .run(
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                Vec::new(),
                4096,
            )
            .await?;
        let path = std::str::from_utf8(&path)?.trim_end_matches('\n');
        let common = std::path::Path::new(path);
        ensure!(
            common.is_absolute(),
            "Git common directory must be absolute"
        );
        for name in [
            "objects/info/alternates",
            "objects/info/http-alternates",
            "shallow",
        ] {
            ensure!(
                std::fs::symlink_metadata(common.join(name))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                "alternate or partial Git source is unsupported"
            );
        }
        let config = self
            .run(
                &["config", "--local", "--null", "--list"],
                Vec::new(),
                65536,
            )
            .await?;
        for record in config.split(|b| *b == 0).filter(|b| !b.is_empty()) {
            let key = record.split(|b| *b == b'\n').next().unwrap();
            ensure!(
                key != b"extensions.partialclone"
                    && !key.ends_with(b".promisor")
                    && !key.ends_with(b".partialclonefilter"),
                "partial/promisor Git source is unsupported"
            );
        }
        let pack = common.join("objects/pack");
        let mut count = 0usize;
        for entry in std::fs::read_dir(pack)? {
            count += 1;
            ensure!(
                count <= self.owner.options.limits.max_objects + 1024,
                "Git pack inventory limit exceeded"
            );
            ensure!(
                entry?.path().extension().is_none_or(|s| s != "promisor"),
                "promisor Git source is unsupported"
            );
        }
        Ok(())
    }
}
pub(super) fn verify(id: &str, kind: &str, bytes: &[u8]) -> Result<()> {
    use sha1::Digest;
    let mut h = sha1::Sha1::new();
    h.update(format!("{kind} {}\0", bytes.len()).as_bytes());
    h.update(bytes);
    ensure!(
        super::state::hex(&h.finalize()) == id,
        "Git object checksum differs"
    );
    Ok(())
}
