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
    };
    let error = create(&root, settings).unwrap_err().to_string();
    assert!(
        error.contains("VFS exited") || error.contains("VFS startup failed"),
        "{error}"
    );
    let manifest = fs::read_to_string(root.join("session.json")).unwrap();
    assert!(!manifest.contains("DO_NOT_COPY"));
    assert_eq!(status(&root).unwrap().revision, revision);
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
