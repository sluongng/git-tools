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
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
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
        // A missing side marker never authorizes resetting existing Git state.
        // Only an empty directory or a provably pristine bare init can resume.
        if fresh && fs::read_dir(&self.gitdir)?.next().is_some() {
            self.validate_pristine_admin(repo).context(
                "Git admin exists without git-ready; preserve it and recover initialization before mounting",
            )?;
        }
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
        // Rebind on valid remounts and pristine interrupted initialization,
        // permitting a moved same-commit clone without resetting native state.
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
                    // A fresh admin must match the source's lack of a boundary.
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
        self.prepare_http_auth(repo, mountpoint, &local_config)?;
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
            // Publish readiness only after all initialization state is durable.
            // A crash can leave either a ready admin or a preserved incomplete
            // admin, never permission to rerun destructive initialization.
            sync_tree(&self.gitdir)?;
            let temporary = self.directory.join(".git-ready.tmp");
            let mut marker = File::create(&temporary)?;
            writeln!(marker, "{}", snapshot.commit)?;
            marker.sync_all()?;
            fs::rename(&temporary, &ready)?;
            File::open(&self.directory)?.sync_all()?;
        }
        Ok(())
    }

    fn validate_pristine_admin(&self, repo: &GitRepo) -> Result<()> {
        // Ref enumeration alone is insufficient: Git can ignore malformed loose
        // refs. First require the physical shape of a bare `git init`, with no
        // index, ref/log/reftable files, object data, locks, or unknown artifacts.
        fn empty_directories(path: &Path) -> Result<()> {
            ensure!(
                fs::symlink_metadata(path)?.is_dir(),
                "unexpected Git admin artifact"
            );
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                ensure!(entry.file_type()?.is_dir(), "Git admin contains state");
                empty_directories(&entry.path())?;
            }
            Ok(())
        }
        for entry in fs::read_dir(&self.gitdir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            match entry.file_name().as_bytes() {
                b"HEAD" | b"config" | b"description" => {
                    ensure!(kind.is_file(), "unexpected Git admin artifact");
                }
                b"refs" | b"objects" | b"branches" => empty_directories(&entry.path())?,
                b"hooks" | b"info" => {
                    ensure!(kind.is_dir(), "unexpected Git admin artifact");
                    for child in fs::read_dir(entry.path())? {
                        let child = child?;
                        ensure!(
                            child.file_type()?.is_file(),
                            "unexpected Git init template artifact"
                        );
                        let name = child.file_name();
                        ensure!(
                            (entry.file_name() == "hooks" && name.as_bytes().ends_with(b".sample"))
                                || (entry.file_name() == "info" && name == "exclude"),
                            "Git admin contains non-pristine template state"
                        );
                    }
                }
                _ => bail!("Git admin contains state beyond a pristine bare init"),
            }
        }
        let config = run(
            repo.git_binary(),
            Some(&self.gitdir),
            None,
            &[
                "config".into(),
                "--local".into(),
                "--null".into(),
                "--list".into(),
            ],
            None,
        )?;
        for record in config
            .split(|b| *b == 0)
            .filter(|record| !record.is_empty())
        {
            let name = record.split(|b| *b == b'\n').next().unwrap();
            ensure!(
                [
                    b"core.repositoryformatversion".as_slice(),
                    b"core.filemode",
                    b"core.bare",
                    b"core.ignorecase",
                    b"core.precomposeunicode"
                ]
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed)),
                "Git admin has non-pristine configuration"
            );
        }
        ensure!(
            config_value_matches(&config, "core.bare", OsStr::new("true"))
                && config_value_matches(&config, "core.repositoryformatversion", OsStr::new("0")),
            "Git admin is not a pristine bare repository"
        );
        let head = fs::read(self.gitdir.join("HEAD"))?;
        let target = head
            .strip_prefix(b"ref: ")
            .and_then(|head| head.strip_suffix(b"\n"))
            .context("Git admin HEAD is not symbolic unborn state")?;
        ensure!(
            target.starts_with(b"refs/heads/"),
            "Git admin HEAD is not a branch"
        );
        run(
            repo.git_binary(),
            None,
            None,
            &[
                "check-ref-format".into(),
                OsStr::from_bytes(target).to_owned(),
            ],
            None,
        )?;
        ensure!(
            run(
                repo.git_binary(),
                Some(&self.gitdir),
                None,
                &["for-each-ref".into()],
                None
            )?
            .is_empty(),
            "Git admin contains refs"
        );
        Ok(())
    }

    /// Promisor credentials belong in a private config include, never helper
    /// argv. Replace our include on every remount, so rotation/removal and a
    /// different source backend cannot retain stale managed credentials.
    fn prepare_http_auth(
        &self,
        repo: &GitRepo,
        mountpoint: &Path,
        local_config: &[u8],
    ) -> Result<()> {
        let source = repo.git_output(&["config", "--local", "--null", "--list"], false)?;
        let mut auth = Vec::new();
        for record in source.split(|b| *b == 0).filter(|r| !r.is_empty()) {
            let Some(split) = record.iter().position(|b| *b == b'\n') else {
                continue;
            };
            let (key, value) = (&record[..split], &record[split + 1..]);
            if key.len() <= 17
                || !key[..5].eq_ignore_ascii_case(b"http.")
                || !key[key.len() - 12..].eq_ignore_ascii_case(b".extraheader")
            {
                continue;
            }
            let scope = &key[5..key.len() - 12];
            ensure!(
                (scope.starts_with(b"http://") || scope.starts_with(b"https://"))
                    && !scope
                        .iter()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control()),
                "invalid scoped HTTP auth configuration"
            );
            ensure!(
                !value.contains(&b'\r') && !value.contains(&b'\n'),
                "HTTP extraHeader must not contain line breaks"
            );
            auth.extend_from_slice(b"[http ");
            config_quote(&mut auth, scope);
            auth.extend_from_slice(b"]\n\textraHeader = ");
            config_quote(&mut auth, value);
            auth.push(b'\n');
        }
        let path = self.gitdir.join("git-vfs-http-auth.config");
        let temporary = self.gitdir.join(format!(
            ".git-vfs-http-auth-{}-{}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let write = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&auth)?;
            file.sync_all()?;
            fs::rename(&temporary, &path)?;
            File::open(&self.gitdir)?.sync_all()?;
            Ok(())
        })();
        if write.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write?;
        // A first mount interrupted after installing the include has no ready
        // marker. Read actual config then rather than adding a duplicate path.
        let current;
        let config = if local_config.is_empty() {
            current = self.git(
                repo,
                mountpoint,
                &[
                    "config".into(),
                    "--local".into(),
                    "--null".into(),
                    "--list".into(),
                ],
                None,
            )?;
            current.as_slice()
        } else {
            local_config
        };
        let included = config.split(|b| *b == 0).any(|record| {
            record.strip_prefix(b"include.path\n") == Some(path.as_os_str().as_bytes())
        });
        if !included {
            self.git(
                repo,
                mountpoint,
                &[
                    "config".into(),
                    "--local".into(),
                    "--add".into(),
                    "include.path".into(),
                    path.as_os_str().to_owned(),
                ],
                None,
            )?;
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

// Fresh Git initialization contains only regular files and directories. Refuse
// unexpected links rather than following them while establishing durability.
fn sync_tree(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            ensure!(kind.is_file(), "unexpected Git initialization artifact");
            File::open(entry.path())?.sync_all()?;
        }
    }
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn config_quote(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'"');
    for byte in value {
        match byte {
            b'\\' | b'"' => {
                output.push(b'\\');
                output.push(*byte);
            }
            b'\t' => output.extend_from_slice(b"\\t"),
            b'\x08' => output.extend_from_slice(b"\\b"),
            b'\n' => output.extend_from_slice(b"\\n"),
            _ => output.push(*byte),
        }
    }
    output.push(b'"');
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SizeFallback;
    use std::io::{BufRead, BufReader};

    struct Server(std::process::Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn git(path: &Path, args: &[&str]) -> Vec<u8> {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(output.status.success(), "Git fixture command failed");
        output.stdout
    }

    #[test]
    fn projected_native_git_hydrates_with_private_scoped_auth_and_refreshes_it() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.git");
        let checkout = root.path().join("source");
        fs::create_dir(&checkout).unwrap();
        git(&checkout, &["init", "--quiet", "--initial-branch=main"]);
        git(&checkout, &["config", "user.name", "Fixture"]);
        git(
            &checkout,
            &["config", "user.email", "fixture@example.invalid"],
        );
        for name in ["first", "second", "third"] {
            fs::write(checkout.join(name), name).unwrap();
        }
        git(&checkout, &["add", "."]);
        git(&checkout, &["commit", "--quiet", "-m", "fixture"]);
        git(
            root.path(),
            &[
                "clone",
                "--quiet",
                "--bare",
                checkout.to_str().unwrap(),
                source.to_str().unwrap(),
            ],
        );
        git(&source, &["config", "uploadpack.allowFilter", "true"]);
        git(
            &source,
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
        );
        let token_file = root.path().join("expected-token");
        fs::write(&token_file, "fixture-one").unwrap();
        let script = r#"
import http.server,os,pathlib,subprocess,sys
root=pathlib.Path(sys.argv[1]); token=pathlib.Path(sys.argv[2])
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_GET(self): self.serve()
 def do_POST(self): self.serve()
 def serve(self):
  if self.headers.get('Authorization') != 'Bearer '+token.read_text():
   self.send_response(401); self.end_headers(); return
  path,_,query=self.path.partition('?')
  size=int(self.headers.get('Content-Length','0'))
  env=dict(os.environ,GIT_PROJECT_ROOT=str(root),GIT_HTTP_EXPORT_ALL='1',REQUEST_METHOD=self.command,PATH_INFO=path,QUERY_STRING=query,CONTENT_TYPE=self.headers.get('Content-Type',''),CONTENT_LENGTH=str(size),REMOTE_USER='fixture',SERVER_PROTOCOL='HTTP/1.1')
  data=subprocess.run(['git','http-backend'],input=self.rfile.read(size),stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env).stdout
  headers,_,body=data.partition(b'\r\n\r\n'); parsed=[]; status=200
  for line in headers.split(b'\r\n'):
   key,_,value=line.partition(b':'); key=key.decode(); value=value.strip().decode()
   if key.lower()=='status': status=int(value.split()[0])
   elif key: parsed.append((key,value))
  self.send_response(status)
  for key,value in parsed: self.send_header(key,value)
  self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
server=http.server.HTTPServer(('127.0.0.1',0),Handler)
print(server.server_port,flush=True); server.serve_forever()
"#;
        let child = Command::new("python3")
            .args(["-u", "-c", script])
            .arg(root.path())
            .arg(&token_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Server(child);
        let mut port = String::new();
        BufReader::new(server.0.stdout.take().unwrap())
            .read_line(&mut port)
            .unwrap();
        let origin = format!("http://127.0.0.1:{}/", port.trim());
        let url = format!("{origin}source.git");
        let bare = root.path().join("clone.git");
        let clone = Command::new("git")
            .args(["clone", "--quiet", "--bare", "--filter=blob:none"])
            .arg(&url)
            .arg(&bare)
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", format!("http.{origin}.extraHeader"))
            .env("GIT_CONFIG_VALUE_0", "Authorization: Bearer fixture-one")
            .output()
            .unwrap();
        assert!(
            clone.status.success(),
            "authenticated blobless fixture clone failed"
        );
        let configure_auth = |token: &str| {
            let mut file = OpenOptions::new()
                .append(true)
                .open(bare.join("config"))
                .unwrap();
            writeln!(
                file,
                "\n[http \"{origin}\"]\n\textraHeader = Authorization: Bearer {token}"
            )
            .unwrap();
        };
        configure_auth("fixture-one");
        let wrapper = root.path().join("git-wrapper");
        fs::write(&wrapper, r#"#!/usr/bin/env python3
import json,os,pathlib,sys
with pathlib.Path(__file__).with_suffix('.argv').open('a') as f: f.write(json.dumps(sys.argv[1:])+'\n')
os.execvp('git',['git',*sys.argv[1:]])
"#).unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        let repo = GitRepo::open(&bare, &wrapper, "origin", SizeFallback::Error).unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        let mount = root.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let working = WorkingTree::lock(&root.path().join("overlay")).unwrap();
        let _overlay =
            crate::overlay::Overlay::open(&working.directory, &snapshot, &working.gitdir).unwrap();
        // A pristine bare init is a supported interrupted first initialization.
        git(
            &working.gitdir,
            &["init", "--bare", "--object-format=sha1", "--template="],
        );
        // Conventional Git templates may include these inert artifacts.
        fs::create_dir_all(working.gitdir.join("branches")).unwrap();
        fs::create_dir_all(working.gitdir.join("hooks")).unwrap();
        fs::create_dir_all(working.gitdir.join("info")).unwrap();
        fs::write(working.gitdir.join("hooks/pre-commit.sample"), b"sample").unwrap();
        fs::write(working.gitdir.join("info/exclude"), b"# excludes\n").unwrap();
        let original_head = fs::read(working.gitdir.join("HEAD")).unwrap();
        let original_config = fs::read(working.gitdir.join("config")).unwrap();
        for (path, bytes) in [
            ("index", b"staged index".as_slice()),
            ("refs/heads/existing", b"malformed ref\n".as_slice()),
            ("packed-refs", b"malformed packed refs\n".as_slice()),
            ("HEAD", snapshot.commit.as_bytes()),
            ("HEAD", b"ref: refs/heads/invalid..branch\n".as_slice()),
        ] {
            fs::write(working.gitdir.join(path), bytes).unwrap();
            assert!(
                working
                    .prepare(&repo, &snapshot, &mount, Path::new("/bin/true"))
                    .is_err(),
                "{path}"
            );
            assert_eq!(fs::read(working.gitdir.join(path)).unwrap(), bytes);
            assert_eq!(
                fs::read(working.gitdir.join("config")).unwrap(),
                original_config
            );
            if path == "HEAD" {
                fs::write(working.gitdir.join(path), &original_head).unwrap();
            } else {
                fs::remove_file(working.gitdir.join(path)).unwrap();
            }
        }
        working
            .prepare(&repo, &snapshot, &mount, Path::new("/bin/true"))
            .unwrap();
        let auth_file = working.gitdir.join("git-vfs-http-auth.config");
        assert_eq!(
            fs::metadata(&auth_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let native_show = |name: &str| {
            Command::new(&wrapper)
                .arg("--git-dir")
                .arg(&working.gitdir)
                .args(["show", &format!("HEAD:{name}")])
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_TERMINAL_PROMPT", "0")
                .output()
                .unwrap()
        };
        assert_eq!(native_show("first").stdout, b"first");
        git(
            &bare,
            &[
                "config",
                "--unset-all",
                &format!("http.{origin}.extraHeader"),
            ],
        );
        configure_auth("fixture-two");
        fs::write(&token_file, "fixture-two").unwrap();
        // Preserve unrelated user config and includes while replacing managed auth.
        let extra = root.path().join("user.config");
        fs::write(&extra, "[user]\n\tname = Preserved\n").unwrap();
        git(
            &working.gitdir,
            &["config", "--add", "include.path", extra.to_str().unwrap()],
        );
        working
            .prepare(&repo, &snapshot, &mount, Path::new("/bin/true"))
            .unwrap();
        assert_eq!(native_show("second").stdout, b"second");
        assert_eq!(
            git(&working.gitdir, &["config", "--includes", "user.name"]),
            b"Preserved\n"
        );
        let includes = git(
            &working.gitdir,
            &["config", "--local", "--get-all", "include.path"],
        );
        assert_eq!(
            includes
                .split(|b| *b == b'\n')
                .filter(|p| *p == auth_file.as_os_str().as_bytes())
                .count(),
            1
        );
        git(
            &bare,
            &[
                "config",
                "--unset-all",
                &format!("http.{origin}.extraHeader"),
            ],
        );
        working
            .prepare(&repo, &snapshot, &mount, Path::new("/bin/true"))
            .unwrap();
        assert!(fs::read(&auth_file).unwrap().is_empty());
        assert!(
            !native_show("third").status.success(),
            "removed auth must not remain effective"
        );
        // Native Git may advance HEAD and independently stage a different tree.
        // Losing only the side marker must never authorize resetting either.
        git(
            &working.gitdir,
            &["config", "user.email", "fixture@example.invalid"],
        );
        let tree = git(&working.gitdir, &["rev-parse", "HEAD^{tree}"]);
        let commit = git(
            &working.gitdir,
            &[
                "commit-tree",
                std::str::from_utf8(&tree).unwrap().trim(),
                "-p",
                &snapshot.commit,
                "-m",
                "native commit",
            ],
        );
        git(
            &working.gitdir,
            &[
                "update-ref",
                "--no-deref",
                "HEAD",
                std::str::from_utf8(&commit).unwrap().trim(),
            ],
        );
        git(
            &working.gitdir,
            &[
                "-c",
                "core.fsmonitor=false",
                "update-index",
                "--force-remove",
                "first",
            ],
        );
        let index = fs::read(working.gitdir.join("index")).unwrap();
        let config = fs::read(working.gitdir.join("config")).unwrap();
        fs::remove_file(working.directory.join("git-ready")).unwrap();
        let error = working
            .prepare(&repo, &snapshot, &mount, Path::new("/bin/true"))
            .unwrap_err();
        assert!(error.to_string().contains("without git-ready"));
        assert_eq!(git(&working.gitdir, &["rev-parse", "HEAD"]), commit);
        assert_eq!(fs::read(working.gitdir.join("index")).unwrap(), index);
        assert_eq!(fs::read(working.gitdir.join("config")).unwrap(), config);
        assert!(
            !git(
                &working.gitdir,
                &[
                    "-c",
                    "core.fsmonitor=false",
                    "diff",
                    "--cached",
                    "--name-only"
                ]
            )
            .is_empty()
        );
        let argv = fs::read(wrapper.with_extension("argv")).unwrap();
        assert!(
            !argv
                .windows(b"fixture-one".len())
                .any(|x| x == b"fixture-one")
        );
        assert!(
            !argv
                .windows(b"fixture-two".len())
                .any(|x| x == b"fixture-two")
        );
    }
}
