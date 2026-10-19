//! Isolated native Git index and fsmonitor bridge for a writable projected tree.
use crate::backend::GitRepo;
use crate::filesystem::Snapshot;
use crate::overlay::dirty_paths;
use anyhow::{Context, Result, bail, ensure};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub struct WorkingTree {
    pub directory: PathBuf,
    pub gitdir: PathBuf,
    _lock: File,
}

impl WorkingTree {
    pub fn lock(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory).context("create writable overlay directory")?;
        let directory = directory.canonicalize()?;
        ensure!(
            !directory.as_os_str().as_bytes().contains(&b'\n'),
            "overlay path cannot contain a newline"
        );
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("mount.lock"))?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!("overlay is already mounted or locked by another process");
        }
        fs::create_dir_all(directory.join("git"))?;
        Ok(Self {
            gitdir: directory.join("git"),
            directory,
            _lock: lock,
        })
    }

    pub fn prepare(
        &self,
        repo: &GitRepo,
        snapshot: &Snapshot,
        mountpoint: &Path,
        executable: &Path,
    ) -> Result<()> {
        let ready = self.directory.join("git-ready");
        let fresh = !ready.exists();
        if !self.gitdir.join("config").exists() {
            run(
                repo.git_binary(),
                None,
                None,
                &[
                    "init".into(),
                    "--bare".into(),
                    "--object-format=sha1".into(),
                    "--template=".into(),
                    self.gitdir.as_os_str().to_owned(),
                ],
                None,
            )?;
        }
        let format = run(
            repo.git_binary(),
            Some(&self.gitdir),
            None,
            &["rev-parse".into(), "--show-object-format".into()],
            None,
        )?;
        ensure!(
            format == b"sha1\n",
            "writable overlay Git admin must use SHA-1 objects"
        );
        // Rebind independently of init/config existence. This also recovers an
        // interrupted first initialization and permits moving a same-commit clone.
        let objects = repo.repository_path().join("objects");
        let mut alternate = objects.as_os_str().as_bytes().to_vec();
        ensure!(
            !alternate.contains(&b'\n'),
            "object-store path cannot contain a newline"
        );
        alternate.push(b'\n');
        let info = self.gitdir.join("objects/info");
        fs::create_dir_all(&info)?;
        let temporary = info.join(".git-vfs-alternates.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&alternate)?;
        file.sync_all()?;
        fs::rename(&temporary, info.join("alternates"))?;
        File::open(&info)?.sync_all()?;
        if fresh {
            // Shallowness belongs to the repository admin, not its alternate
            // object store. Seed it once, then preserve native Git's subsequent
            // deepening/unshallowing across remounts.
            let boundary = repo.repository_path().join("shallow");
            match fs::read(&boundary) {
                Ok(bytes) => {
                    let temporary = self.gitdir.join(".git-vfs-shallow.tmp");
                    let mut file = File::create(&temporary)?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    fs::rename(&temporary, self.gitdir.join("shallow"))?;
                    File::open(&self.gitdir)?.sync_all()?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Recover a first initialization interrupted before git-ready,
                    // even if the source was unshallowed in the meantime.
                    match fs::remove_file(self.gitdir.join("shallow")) {
                        Ok(()) => File::open(&self.gitdir)?.sync_all()?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error).context("clear initial shallow boundary"),
                    }
                }
                Err(error) => return Err(error).context("read initial shallow boundary"),
            }
        }
        // A remount normally keeps every managed value unchanged. Read them in
        // one process instead of taking Git's config lock and rewriting the
        // config once per setting. Keep Git as the writer for changed values:
        // it preserves arbitrary user config and provides atomic lock/rename.
        let local_config = if fresh {
            // All managed settings must be initialized; do not add a config
            // read to the first mount's critical path.
            Vec::new()
        } else {
            self.git(
                repo,
                mountpoint,
                &[
                    "config".into(),
                    "--local".into(),
                    "--null".into(),
                    "--list".into(),
                ],
                None,
            )?
        };
        for (key, value) in [
            ("core.bare", "false"),
            ("core.filemode", "true"),
            ("core.symlinks", "true"),
            ("core.autocrlf", "false"),
            ("core.sparseCheckout", "false"),
            ("core.untrackedCache", "false"),
            ("core.fsmonitorHookVersion", "2"),
            // Git classifies FUSE as remote. Our hook uses a local durable journal,
            // rather than relying on filesystem notification support.
            ("fsmonitor.allowRemote", "true"),
            ("core.ignorecase", "false"),
            ("core.ignorestat", "false"),
        ] {
            self.set_config(repo, mountpoint, &local_config, key, OsStr::new(value))?;
        }
        self.set_config(
            repo,
            mountpoint,
            &local_config,
            "core.worktree",
            mountpoint.as_os_str(),
        )?;
        let hooks = self.gitdir.join("hooks");
        fs::create_dir_all(&hooks)?;
        self.set_config(
            repo,
            mountpoint,
            &local_config,
            "core.hooksPath",
            hooks.as_os_str(),
        )?;
        // Copy configured promisor transports into the isolated admin directory without printing URLs.
        // The object store is shared read-only through alternates; user refs and index are isolated.
        if fresh
            && let Ok(remotes) =
                repo.git_output(&["config", "--null", "--get-regexp", "^remote\\."], false)
        {
            for record in remotes.split(|&b| b == 0).filter(|r| !r.is_empty()) {
                let newline = record
                    .iter()
                    .position(|&b| b == b'\n')
                    .context("invalid Git remote config output")?;
                let key = OsStr::from_bytes(&record[..newline]).to_owned();
                let value = OsStr::from_bytes(&record[newline + 1..]).to_owned();
                self.git(
                    repo,
                    mountpoint,
                    &[
                        "config".into(),
                        "--local".into(),
                        "--add".into(),
                        key,
                        value,
                    ],
                    None,
                )?;
            }
        }
        let hook = hooks.join("git-vfs-fsmonitor");
        let mut script = b"#!/bin/sh\nexec ".to_vec();
        script.extend(shell_quote(executable.as_os_str()));
        script.extend_from_slice(b" fsmonitor --overlay ");
        script.extend(shell_quote(self.directory.as_os_str()));
        script.extend_from_slice(b" \"$@\"\n");
        fs::write(&hook, script)?;
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o700))?;
        // Git treats this config as a shell command, not an argv pathname.
        let hook_command = OsString::from_vec(shell_quote(hook.as_os_str()));
        self.set_config(
            repo,
            mountpoint,
            &local_config,
            "core.fsmonitor",
            &hook_command,
        )?;
        if fresh {
            // Never checkout files or refresh the empty stat cache: that would hydrate every blob.
            self.git(
                repo,
                mountpoint,
                &[
                    "update-ref".into(),
                    "--no-deref".into(),
                    "HEAD".into(),
                    snapshot.commit.clone().into(),
                ],
                None,
            )?;
            self.git(
                repo,
                mountpoint,
                &[
                    "-c".into(),
                    "core.fsmonitor=false".into(),
                    "read-tree".into(),
                    snapshot.commit.clone().into(),
                ],
                None,
            )?;
            self.git(
                repo,
                mountpoint,
                &["update-index".into(), "--fsmonitor".into()],
                None,
            )?;
            let dirty = dirty_paths(&self.directory)?;
            let mut paths = Vec::new();
            for node in snapshot.nodes() {
                if node.kind.name() == "directory" {
                    continue;
                }
                let path = node.path.as_os_str().as_bytes();
                if dirty.iter().any(|changed| {
                    changed == b"/"
                        || changed == path
                        || (changed.ends_with(b"/") && path.starts_with(changed))
                }) {
                    continue;
                }
                paths.extend_from_slice(path);
                paths.push(0);
            }
            self.git(
                repo,
                mountpoint,
                &[
                    "update-index".into(),
                    "--fsmonitor-valid".into(),
                    "-z".into(),
                    "--stdin".into(),
                ],
                Some(paths),
            )?;
            fs::write(&ready, format!("{}\n", snapshot.commit))?;
        }
        Ok(())
    }

    fn set_config(
        &self,
        repo: &GitRepo,
        mountpoint: &Path,
        local_config: &[u8],
        key: &str,
        value: &OsStr,
    ) -> Result<()> {
        if config_value_matches(local_config, key, value) {
            return Ok(());
        }
        self.git(
            repo,
            mountpoint,
            &[
                "config".into(),
                "--local".into(),
                key.into(),
                value.to_owned(),
            ],
            None,
        )?;
        Ok(())
    }

    fn git(
        &self,
        repo: &GitRepo,
        mountpoint: &Path,
        args: &[OsString],
        input: Option<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        run(
            repo.git_binary(),
            Some(&self.gitdir),
            Some(mountpoint),
            args,
            input,
        )
    }
}

// `git config --null --list` separates each key from its optional value by
// newline and each record by NUL. Values may themselves contain newlines;
// valueless booleans must not compare equal to an explicit managed string.
// Only skip a write for one exact value, retaining Git's existing behavior
// (including errors) for duplicate settings.
fn config_value_matches(config: &[u8], key: &str, value: &OsStr) -> bool {
    let mut matches = config.split(|&byte| byte == 0).filter_map(|record| {
        let split = record.iter().position(|&byte| byte == b'\n');
        let name = split.map_or(record, |split| &record[..split]);
        name.eq_ignore_ascii_case(key.as_bytes())
            .then(|| split.map(|split| &record[split + 1..]))
    });
    matches.next() == Some(Some(value.as_bytes())) && matches.next().is_none()
}

fn run(
    binary: &Path,
    gitdir: Option<&Path>,
    worktree: Option<&Path>,
    args: &[OsString],
    input: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut command = Command::new(binary);
    command.arg("--no-replace-objects");
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
    ] {
        command.env_remove(name);
    }
    command.env("GIT_NO_LAZY_FETCH", "1");
    if let Some(gitdir) = gitdir {
        command.arg("--git-dir").arg(gitdir);
    }
    if let Some(worktree) = worktree {
        command.arg("--work-tree").arg(worktree);
    }
    command
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().context("start Git admin command")?;
    let writer = input.map(|data| {
        let mut pipe = child.stdin.take().expect("piped stdin");
        std::thread::spawn(move || pipe.write_all(&data))
    });
    let output = child.wait_with_output()?;
    if let Some(writer) = writer {
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("Git input writer failed"))??;
    }
    ensure!(
        output.status.success(),
        "Git working-tree initialization command failed ({})",
        output.status
    );
    Ok(output.stdout)
}

fn shell_quote(text: &OsStr) -> Vec<u8> {
    let mut result = vec![b'\''];
    for &byte in text.as_bytes() {
        if byte == b'\'' {
            result.extend_from_slice(b"'\\''");
        } else {
            result.push(byte);
        }
    }
    result.push(b'\'');
    result
}
