#![cfg(target_os = "linux")]
use agent_session::{Settings, create, resume, status, stop};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

// A CLOEXEC flock fd is still inherited between another thread's fork and exec.
// Serialize this tiny process-fixture suite so unrelated test forks cannot pin a
// just-dropped session lock. The explicit concurrent-lock assertion stays intact.
static PROCESS_FIXTURES: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn script(directory: &Path, name: &str, contents: &str) -> PathBuf {
    let path = directory.join(name);
    fs::write(&path, contents).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

fn git(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[test]
fn daemon_mount_failure_retains_state_and_never_signals_the_daemon() {
    use agent_session::{MountController, MountRequest};
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "--template="]);
    git(
        &source,
        &[
            "-c",
            "user.name=Agent",
            "-c",
            "user.email=agent@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
    let revision = git(&source, &["rev-parse", "HEAD"]);
    let reference = git(&source, &["symbolic-ref", "--short", "HEAD"]);
    let wrapper = script(
        temp.path(),
        "git-wrapper",
        &format!(
            "#!/bin/sh\nset -eu\nif test \"$1\" = --no-replace-objects; then\n  for last do :; done\n  echo clone >> '{}'\n  echo \"$*\" > '{}'\n  exec git clone --bare -- '{}' \"$last\"\nfi\nexec git \"$@\"\n",
            temp.path().join("clones").display(),
            temp.path().join("clone-argv").display(),
            source.display()
        ),
    );
    let bazelrc = temp.path().join("external.bazelrc");
    fs::write(&bazelrc, "# provisioned externally\n").unwrap();
    let root = temp.path().join("session");
    struct Controller {
        requests: std::sync::Mutex<Vec<MountRequest>>,
        stops: std::sync::Mutex<Vec<PathBuf>>,
    }
    impl MountController for Controller {
        fn mount(&self, request: MountRequest) -> anyhow::Result<()> {
            self.requests.lock().unwrap().push(request);
            anyhow::bail!("injected mount failure")
        }
        fn unmount(&self, session: &Path) -> anyhow::Result<()> {
            self.stops.lock().unwrap().push(session.to_owned());
            Ok(())
        }
        fn is_mounted(&self, _: &Path) -> anyhow::Result<bool> {
            Ok(false)
        }
    }
    let controller = Controller {
        requests: Default::default(),
        stops: Default::default(),
    };
    let settings = Settings {
        proxy_url: format!("http://127.0.0.1:8082/{}", "a".repeat(64)),
        proxy_repo: "repo".into(),
        reference,
        revision: revision.clone(),
        bazelrc,
        git_vfs: "deliberately-not-installed".into(),
        git: wrapper,
        bazel: script(temp.path(), "bazel", "#!/bin/sh\nexit 0\n"),
        mount_timeout_seconds: 1,
        repo_contents_cache: None,
        remote_repo_contents_cache: true,
    };
    assert!(
        agent_session::create_with_controller(&root, settings, &controller)
            .unwrap_err()
            .to_string()
            .contains("injected mount failure")
    );
    let request = controller.requests.lock().unwrap()[0].clone();
    assert_eq!(request.session, root.canonicalize().unwrap());
    assert_eq!(request.revision, revision);
    assert_eq!(request.mountpoint, root.join("workspace"));
    let argv = fs::read_to_string(temp.path().join("clone-argv")).unwrap();
    assert!(argv.contains("http://127.0.0.1:8082/git/repo.git"));
    assert!(
        !argv.contains(&"a".repeat(64)),
        "local capability must not enter process argv"
    );
    let private_config = fs::read_to_string(root.join("repo.git/config")).unwrap();
    assert!(private_config.contains("[http \"http://127.0.0.1:8082/\"]"));
    assert!(private_config.contains(&format!(
        "extraHeader = authorization: Bearer {}",
        "a".repeat(64)
    )));
    fs::create_dir_all(root.join("overlay")).unwrap();
    fs::write(root.join("overlay/edit"), "retained").unwrap();
    assert!(
        stop(&root)
            .unwrap_err()
            .to_string()
            .contains("lifecycle owner differs")
    );
    assert!(agent_session::resume_with_controller(&root, &controller).is_err());
    assert_eq!(controller.requests.lock().unwrap().len(), 2);
    assert_eq!(
        fs::read_to_string(temp.path().join("clones")).unwrap(),
        "clone\n"
    );
    let report = agent_session::stop_with_controller(&root, &controller).unwrap();
    assert_eq!(report.phase, "stopped");
    assert!(report.daemon_managed);
    assert!(!report.process_verified);
    assert_eq!(
        controller.stops.lock().unwrap().as_slice(),
        &[root.canonicalize().unwrap()]
    );
    assert_eq!(
        fs::read_to_string(root.join("overlay/edit")).unwrap(),
        "retained"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("session.json")).unwrap()).unwrap();
    assert!(
        manifest["mount"].is_null(),
        "daemon PID must never become a signal target"
    );
}

#[test]
fn failed_mount_is_resumable_without_reclone_and_stop_preserves_private_state() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    git(&source, &["init", "--template="]);
    git(
        &source,
        &[
            "-c",
            "user.name=Agent",
            "-c",
            "user.email=agent@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
    let revision = git(&source, &["rev-parse", "HEAD"]);
    let reference = git(&source, &["symbolic-ref", "--short", "HEAD"]);
    // A failed backend exercises real clone/revision checks and persistent
    // recovery without requiring kernel FUSE privileges on the RBE worker.
    let backend = script(
        temp.path(),
        "vfs",
        &format!(
            "#!/bin/sh\nset -eu\ngit=$2\nshift 2\ncase \"$1\" in\nclone) shift; shift 4; \"$git\" clone --bare -- '{}' \"$2\" ;;\nwarm-blake3) exit 0 ;;\nmount) if test -f '{}'; then exec sleep 30; fi; exit 23 ;;\n*) exit 44 ;;\nesac\n",
            source.display(),
            temp.path().join("inject-startup-failure").display()
        ),
    );
    let bazel = script(temp.path(), "bazel", "#!/bin/sh\nexit 0\n");
    let bazelrc = temp.path().join("provisioned.bazelrc");
    fs::write(
        &bazelrc,
        "# external secret stays out of manifest\nbuild --remote_header=x-api-key=DO_NOT_COPY\n",
    )
    .unwrap();
    let root = temp.path().join("session");
    let shared_cache = temp.path().join("shared-repo-contents");
    fs::create_dir(&shared_cache).unwrap();
    fs::write(shared_cache.join("retained-cache-entry"), "shared content").unwrap();
    let cache_alias = temp.path().join("shared-cache-alias");
    std::os::unix::fs::symlink(&shared_cache, &cache_alias).unwrap();
    let settings = Settings {
        proxy_url: "http://127.0.0.1:8082".into(),
        proxy_repo: "repo".into(),
        reference,
        revision: revision.clone(),
        bazelrc,
        git_vfs: backend,
        git: "git".into(),
        bazel,
        mount_timeout_seconds: 1,
        repo_contents_cache: Some(cache_alias),
        remote_repo_contents_cache: false,
    };
    let error = create(&root, settings).unwrap_err().to_string();
    assert!(
        error.contains("VFS exited") || error.contains("VFS startup failed"),
        "{error}"
    );
    let manifest = fs::read_to_string(root.join("session.json")).unwrap();
    assert!(!manifest.contains("DO_NOT_COPY"));
    let report = status(&root).unwrap();
    assert_eq!(report.revision, revision);
    assert_eq!(
        report.repo_contents_cache,
        Some(shared_cache.canonicalize().unwrap())
    );
    assert!(!report.remote_repo_contents_cache);
    let pinned: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(
        pinned["settings"]["repo_contents_cache"],
        shared_cache.to_str().unwrap()
    );
    fs::create_dir_all(root.join("overlay/git")).unwrap();
    fs::write(root.join("overlay/edit"), "user edit").unwrap();
    fs::write(root.join("overlay/git/index"), "private index").unwrap();
    fs::write(root.join("repo.git/reclone-sentinel"), "preserve").unwrap();
    assert!(
        create(&root, status_settings(&manifest))
            .unwrap_err()
            .to_string()
            .contains("must not exist")
    );
    assert!(resume(&root).is_err());
    assert!(root.join("repo.git/reclone-sentinel").exists());
    let report = stop(&root).unwrap();
    assert_eq!(report.phase, "stopped");
    assert!(!report.mounted);
    assert_eq!(
        fs::read_to_string(root.join("overlay/edit")).unwrap(),
        "user edit"
    );
    assert!(root.join("overlay/git/index").exists());

    // Force only the ownership-bearing manifest write to exceed RLIMIT_FSIZE.
    // Earlier null-mount phase writes fit; the larger process record does not.
    // Run the limit in an isolated fixture process, never the whole test suite.
    fs::write(temp.path().join("inject-startup-failure"), "enabled").unwrap();
    let limit = fs::metadata(root.join("session.json")).unwrap().len() + 75;
    let fixture = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "startup_persistence_failure_fixture",
            "--ignored",
        ])
        .env("AGENT_SESSION_FAILURE_ROOT", &root)
        .env("AGENT_SESSION_FILE_LIMIT", limit.to_string())
        .output()
        .unwrap();
    assert!(
        fixture.status.success(),
        "{}",
        String::from_utf8_lossy(&fixture.stderr)
    );
    let error = fs::read_to_string(root.join("startup-failure-receipt")).unwrap();
    let pid: u32 = error
        .split("owned child PID ")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        !PathBuf::from(format!("/proc/{pid}")).exists(),
        "failed startup left its child alive/unreaped"
    );
    let durable: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("session.json")).unwrap()).unwrap();
    assert!(durable["mount"].is_null());
    assert_eq!(
        fs::read_to_string(root.join("overlay/edit")).unwrap(),
        "user edit"
    );
    assert_eq!(stop(&root).unwrap().phase, "stopped");
    assert_eq!(
        status(&root).unwrap().repo_contents_cache,
        Some(shared_cache.clone())
    );
    assert_eq!(
        fs::read_to_string(shared_cache.join("retained-cache-entry")).unwrap(),
        "shared content"
    );
    // Pin the canonical location, not an alias that can silently be retargeted.
    // Even when the cache needs repair, stop must remain usable for cleanup.
    let retained_cache = temp.path().join("retained-shared-cache");
    fs::rename(&shared_cache, &retained_cache).unwrap();
    let replacement = temp.path().join("replacement-cache");
    fs::create_dir(&replacement).unwrap();
    std::os::unix::fs::symlink(replacement, &shared_cache).unwrap();
    assert!(
        resume(&root)
            .unwrap_err()
            .to_string()
            .contains("canonical location changed")
    );
    assert_eq!(stop(&root).unwrap().phase, "stopped");
    assert!(retained_cache.join("retained-cache-entry").exists());
}

fn status_settings(manifest: &str) -> Settings {
    serde_json::from_value(
        serde_json::from_str::<serde_json::Value>(manifest).unwrap()["settings"].clone(),
    )
    .unwrap()
}

#[test]
fn rejects_unpinned_revision_credentials_and_unknown_manifest_versions() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut settings = Settings {
        proxy_url: "https://secret@host".into(),
        proxy_repo: "repo".into(),
        reference: "main".into(),
        revision: "a".repeat(40),
        bazelrc: "missing".into(),
        git_vfs: "missing".into(),
        git: "git".into(),
        bazel: "missing".into(),
        mount_timeout_seconds: 1,
        repo_contents_cache: None,
        remote_repo_contents_cache: true,
    };
    let root = temp.path().join("session");
    assert!(
        create(&root, settings.clone())
            .unwrap_err()
            .to_string()
            .contains("credentials")
    );
    settings.proxy_url = "https://host".into();
    settings.revision = "main".into();
    assert!(
        create(&root, settings)
            .unwrap_err()
            .to_string()
            .contains("immutable")
    );
    assert!(!root.exists());
    fs::create_dir(&root).unwrap();
    fs::write(root.join("session.json"), r#"{"version":99,"session_root":"/unused","settings":{"proxy_url":"http://host","proxy_repo":"repo","reference":"main","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bazelrc":"/rc","git_vfs":"/vfs","git":"/git","bazel":"/bazel","mount_timeout_seconds":1},"phase":"stopped","mount":null}"#).unwrap();
    assert!(
        status(&root)
            .unwrap_err()
            .to_string()
            .contains("unsupported session manifest version")
    );
}

#[test]
fn stale_pid_identity_is_not_signalled() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let manifest = serde_json::json!({
        "version": 1, "session_root": root,
        "settings": { "proxy_url":"http://host", "proxy_repo":"repo", "reference":"main", "revision":"a".repeat(40), "bazelrc":"/rc", "git_vfs":"/vfs", "git":"/git", "bazel":"/bazel", "mount_timeout_seconds":1 },
        "phase":"mounting", "mount": { "pid":std::process::id(), "start_ticks":0, "executable":"/not-this-process" }
    });
    fs::write(
        root.join("session.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(!status(root).unwrap().process_verified);
    assert_eq!(stop(root).unwrap().phase, "stopped");
}

#[test]
#[ignore = "subprocess fixture; invoked by owned_process_stop"]
fn process_fixture() {
    if std::env::var_os("AGENT_SESSION_PROCESS_FIXTURE").is_some() {
        loop {
            std::thread::park();
        }
    }
}

#[test]
fn owned_process_stop_signals_only_verified_child() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    use std::os::unix::process::ExitStatusExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_fixture", "--ignored"])
        .env("AGENT_SESSION_PROCESS_FIXTURE", "1")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let start_ticks: u64 = stat
        .rsplit_once(") ")
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap();
    let executable = fs::read_link(format!("/proc/{pid}/exe")).unwrap();
    let manifest = serde_json::json!({
        "version":1, "session_root": root,
        "settings": { "proxy_url":"http://host", "proxy_repo":"repo", "reference":"main", "revision":"a".repeat(40), "bazelrc":"/rc", "git_vfs":"/vfs", "git":"/git", "bazel":"/bazel", "mount_timeout_seconds":1 },
        "phase":"mounting", "mount": { "pid":pid, "start_ticks":start_ticks, "executable":executable }
    });
    fs::write(
        temp.path().join("session.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(status(temp.path()).unwrap().process_verified);
    let copied = tempfile::tempdir().unwrap();
    fs::write(
        copied.path().join("session.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let refused = stop(copied.path()).unwrap_err().to_string();
    assert!(refused.contains("different directory"), "{refused}");
    assert!(
        child.try_wait().unwrap().is_none(),
        "copied manifest signalled original process"
    );
    let result = stop(temp.path());
    // Always reap on assertion failure too, so failed validation cannot leak a fixture.
    if result.is_err() {
        let _ = child.kill();
    }
    let exit = child.wait().unwrap();
    assert_eq!(result.unwrap().phase, "stopped");
    assert_eq!(exit.signal(), Some(libc::SIGINT));
}

#[test]
fn concurrent_operation_fails_without_mutating_manifest() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    use fs2::FileExt;
    let temp = tempfile::tempdir().unwrap();
    let lock = fs::File::create(temp.path().join("session.lock")).unwrap();
    lock.lock_exclusive().unwrap();
    let error = status(temp.path()).unwrap_err().to_string();
    assert!(error.contains("another session operation"), "{error}");
    assert!(!temp.path().join("session.json").exists());
}

#[test]
fn stopped_mount_still_shuts_down_only_private_bazel_server() {
    let _process_fixture = PROCESS_FIXTURES.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let bazel = script(
        root,
        "bazel",
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > bazel-invocation\npwd > bazel-cwd\n",
    );
    fs::create_dir_all(root.join("bazel-output/server")).unwrap();
    fs::write(root.join("bazel-output/server/server.pid.txt"), "12345").unwrap();
    let manifest = serde_json::json!({
        "version":1, "session_root": root,
        "settings": { "proxy_url":"http://host", "proxy_repo":"repo", "reference":"main", "revision":"a".repeat(40), "bazelrc":"/run/provisioned.bazelrc", "git_vfs":"/vfs", "git":"/git", "bazel":bazel, "mount_timeout_seconds":1 },
        "phase":"edit_ready", "mount":null
    });
    fs::write(
        root.join("session.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let report = stop(root).unwrap();
    assert!(!report.edit_ready);
    assert!(report.last_successful_build_unix_ms.is_none());
    // Older manifests omitted both settings: remote stays enabled and local
    // cache location remains delegated to the existing Bazel/rc policy.
    assert!(report.remote_repo_contents_cache);
    assert!(report.repo_contents_cache.is_none());
    let args = fs::read_to_string(root.join("bazel-control/bazel-invocation")).unwrap();
    assert!(args.contains(&format!(
        "--output_base={}\n",
        root.join("bazel-output").display()
    )));
    assert!(args.ends_with("shutdown\n"));
    for flag in [
        "--experimental_remote_repo_contents_cache",
        "--digest_function=BLAKE3",
        "--unix_digest_hash_attribute_name=user.blake3",
    ] {
        assert!(args.find(flag).unwrap() < args.find("shutdown\n").unwrap());
    }
    assert_eq!(
        fs::read_to_string(root.join("bazel-control/bazel-cwd"))
            .unwrap()
            .trim(),
        root.join("bazel-control").to_str().unwrap()
    );
    assert_eq!(status(root).unwrap().timings_ms[0].0, "bazel_shutdown");
    assert!(!args.contains("--repo_contents_cache="));

    let shared_cache_owner = tempfile::tempdir().unwrap();
    let shared_cache = shared_cache_owner.path().to_owned();
    fs::write(shared_cache.join("retained"), "cache entry").unwrap();
    let mut updated: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("session.json")).unwrap()).unwrap();
    updated["settings"]["remote_repo_contents_cache"] = serde_json::json!(false);
    updated["settings"]["repo_contents_cache"] = serde_json::json!(shared_cache);
    fs::write(
        root.join("session.json"),
        serde_json::to_vec(&updated).unwrap(),
    )
    .unwrap();
    let disabled = stop(root).unwrap();
    assert!(!disabled.remote_repo_contents_cache);
    assert_eq!(disabled.repo_contents_cache, Some(shared_cache.clone()));
    let args = fs::read_to_string(root.join("bazel-control/bazel-invocation")).unwrap();
    assert!(args.contains("--noexperimental_remote_repo_contents_cache\n"));
    assert!(!args.contains("\n--experimental_remote_repo_contents_cache\n"));
    assert!(
        !args.contains("--repo_contents_cache="),
        "local path is a build option, not a shutdown/startup option"
    );
    assert!(shared_cache.join("retained").exists());
}

#[test]
#[ignore = "isolated resource-limit failure fixture"]
fn startup_persistence_failure_fixture() {
    let Some(root) = std::env::var_os("AGENT_SESSION_FAILURE_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let limit: u64 = std::env::var("AGENT_SESSION_FILE_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        let resource = libc::rlimit {
            rlim_cur: limit,
            rlim_max: limit,
        };
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &resource), 0);
    }
    let error = format!("{:#}", resume(&root).unwrap_err());
    assert!(error.contains("persisting VFS ownership"), "{error}");
    assert!(
        error.contains("owned child PID") && error.contains("reaped"),
        "{error}"
    );
    fs::write(root.join("startup-failure-receipt"), error).unwrap();
}
