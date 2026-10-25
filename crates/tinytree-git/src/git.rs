use crate::state::{Resources, hex, valid_hex};
use anyhow::{Context, Result, ensure};
use sha1::Digest as _;
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
pub(crate) struct Git {
    pub owner: Arc<Resources>,
    program: PathBuf,
    pub deadline: Instant,
}
pub(crate) struct Written {
    pub oid: String,
    pub digest: [u8; 32],
    pub size: u64,
}
impl Git {
    pub fn new(owner: Arc<Resources>, deadline: Instant) -> Result<Self> {
        let requested = &owner.options.git_program;
        let program = if requested.is_absolute() {
            requested.clone()
        } else if requested.components().count() == 1 {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .map(|base| base.join(requested))
                .find(|path| path.is_file())
                .context("Git executable not found")?
        } else {
            std::env::current_dir()?.join(requested)
        };
        let program = program
            .canonicalize()
            .context("cannot resolve Git executable")?;
        ensure!(program.is_file(), "Git executable is not a regular file");
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
                .env("GIT_ATTR_NOSYSTEM", "1");
            command
                .args(["--no-pager", "--no-replace-objects", "--git-dir"])
                .arg(owner.destination.path())
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
    pub async fn initialize(&self) -> Result<()> {
        let version = self.run(&["version"], Vec::new(), 256).await?;
        let version = std::str::from_utf8(&version)?
            .trim()
            .strip_prefix("git version ")
            .context("unsupported Git version output")?;
        let mut parts = version.split('.');
        let major: usize = parts.next().context("Git major version absent")?.parse()?;
        let minor: usize = parts.next().context("Git minor version absent")?.parse()?;
        ensure!(
            major > 2 || major == 2 && minor >= 38,
            "Git 2.38 or newer is required for explicit object/reference fsync policy"
        );
        if std::fs::read_dir(self.owner.destination.path())?
            .next()
            .is_none()
        {
            self.run(
                &["init", "--bare", "--template=", "--object-format=sha1"],
                Vec::new(),
                4096,
            )
            .await?;
        }
        ensure!(
            self.run(&["rev-parse", "--is-bare-repository"], Vec::new(), 128)
                .await?
                == b"true\n",
            "destination must be bare Git"
        );
        ensure!(
            self.run(&["rev-parse", "--show-object-format"], Vec::new(), 128)
                .await?
                == b"sha1\n",
            "destination must use SHA-1 objects"
        );
        let config = self
            .run(
                &["config", "--local", "--null", "--list"],
                Vec::new(),
                65536,
            )
            .await?;
        for record in config.split(|b| *b == 0).filter(|r| !r.is_empty()) {
            let key = record.split(|b| *b == b'\n').next().unwrap();
            ensure!(
                [
                    b"core.repositoryformatversion".as_slice(),
                    b"core.filemode",
                    b"core.bare",
                    b"core.logallrefupdates",
                    b"extensions.objectformat"
                ]
                .contains(&key),
                "destination has unsupported local Git configuration"
            );
        }
        for name in [
            "shallow",
            "info/grafts",
            "objects/info/alternates",
            "objects/info/http-alternates",
            "config.worktree",
            "commondir",
            "worktrees",
            "refs/replace",
        ] {
            ensure!(
                std::fs::symlink_metadata(self.owner.destination.path().join(name))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                "destination has unsupported Git indirection or partial history"
            );
        }
        self.run(
            &[
                "check-ref-format",
                &self.owner.options.destination_reference,
            ],
            Vec::new(),
            1024,
        )
        .await?;
        Ok(())
    }
    pub async fn reference(&self) -> Result<Option<String>> {
        let reference = &self.owner.options.destination_reference;
        let (symbolic, _) = self
            .raw(
                vec!["symbolic-ref".into(), "--quiet".into(), reference.clone()],
                Vec::new(),
                4096,
            )
            .await?;
        ensure!(
            !symbolic.success(),
            "destination reference must not be symbolic"
        );
        let (status, out) = self
            .raw(
                vec![
                    "rev-parse".into(),
                    "--verify".into(),
                    "--quiet".into(),
                    "--end-of-options".into(),
                    reference.clone(),
                ],
                Vec::new(),
                128,
            )
            .await?;
        if !status.success() {
            return Ok(None);
        }
        Ok(Some(oid(&out)?))
    }
    pub async fn object(&self, kind: &str, bytes: Vec<u8>) -> Result<Written> {
        let size = bytes.len() as u64;
        let digest = *blake3::hash(&bytes).as_bytes();
        let expected = git_hash(kind, &bytes);
        let output = self
            .run(
                &["hash-object", "-w", "--no-filters", "--stdin", "-t", kind],
                bytes,
                128,
            )
            .await?;
        let actual = oid(&output)?;
        ensure!(actual == expected, "Git object hash differs");
        self.verify_object(kind, &actual, digest, size).await?;
        Ok(Written {
            oid: actual,
            digest,
            size,
        })
    }
    pub async fn verify_object(
        &self,
        kind: &str,
        id: &str,
        digest: [u8; 32],
        size: u64,
    ) -> Result<()> {
        ensure!(valid_hex(id, 40), "invalid mapped Git identity");
        let bytes = self
            .run(&["cat-file", kind, id], Vec::new(), usize::try_from(size)?)
            .await?;
        ensure!(
            bytes.len() as u64 == size
                && *blake3::hash(&bytes).as_bytes() == digest
                && git_hash(kind, &bytes) == id,
            "mapped Git object differs or is missing"
        );
        Ok(())
    }
    pub async fn deliver(&self, new: &str, old: Option<&str>) -> Result<()> {
        let current = self.reference().await?;
        if current.as_deref() == Some(new) {
            return Ok(());
        }
        ensure!(
            current.as_deref() == old,
            "destination diverged; no overwrite attempted"
        );
        if let Some(old) = old {
            self.run(&["merge-base", "--is-ancestor", old, new], Vec::new(), 1024)
                .await
                .context("destination update is not a fast-forward")?;
        }
        let zero = "0".repeat(40);
        self.run(
            &[
                "update-ref",
                "--no-deref",
                &self.owner.options.destination_reference,
                new,
                old.unwrap_or(&zero),
            ],
            Vec::new(),
            1024,
        )
        .await?;
        ensure!(
            self.reference().await?.as_deref() == Some(new),
            "destination publication requires readback reconciliation"
        );
        Ok(())
    }
}
fn oid(bytes: &[u8]) -> Result<String> {
    let value = std::str::from_utf8(bytes)?.trim_end_matches('\n');
    ensure!(valid_hex(value, 40), "invalid Git object identity");
    Ok(value.into())
}
fn git_hash(kind: &str, bytes: &[u8]) -> String {
    let mut hash = sha1::Sha1::new();
    hash.update(format!("{kind} {}\0", bytes.len()).as_bytes());
    hash.update(bytes);
    hex(&hash.finalize())
}
