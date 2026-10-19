//! Real CLI + FUSE lifecycle coverage, deliberately not a mock filesystem.
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn run(root: &Path, program: impl AsRef<std::ffi::OsStr>, arguments: &[&str]) -> Output {
    let output = Command::new(program)
        .current_dir(root)
        .args(arguments)
        .env("JJ_CONFIG", "")
        .env("JJ_USER", "Fixture")
        .env("JJ_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn git(root: &Path, args: &[&str]) -> Output {
    run(root, "git", args)
}
fn jj(root: &Path, args: &[&str]) -> Output {
    run(
        root,
        Path::new(env!("CARGO_BIN_EXE_git-jj"))
            .canonicalize()
            .unwrap(),
        args,
    )
}
fn text(output: Output) -> String {
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn present(repo: &Path, oid: &str) -> bool {
    let mut child = Command::new("git")
        .arg("--git-dir")
        .arg(repo)
        .arg("cat-file")
        .arg("--batch-check")
        .env("GIT_NO_LAZY_FETCH", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(child.stdin.take().unwrap(), "{oid}").unwrap();
    !String::from_utf8(child.wait_with_output().unwrap().stdout)
        .unwrap()
        .contains("missing")
}
struct Mount {
    child: Child,
    root: PathBuf,
}
impl Drop for Mount {
    fn drop(&mut self) {
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg(&self.root)
            .status();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn mount_vfs(binary: &std::ffi::OsStr, bare: &Path, overlay: &Path, mount: &Path) -> Mount {
    let child = Command::new(binary)
        .args([
            "mount",
            "--repo",
            bare.to_str().unwrap(),
            "--overlay",
            overlay.to_str().unwrap(),
            "--size-source",
            "git",
            mount.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut mounted = Mount {
        child,
        root: mount.to_owned(),
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    while !mount.join(".git").exists() {
        assert!(
            mounted.child.try_wait().unwrap().is_none(),
            "mount daemon exited"
        );
        assert!(Instant::now() < deadline, "mount timeout");
        std::thread::sleep(Duration::from_millis(20));
    }
    mounted
}

#[test]
#[ignore = "requires /dev/fuse and GIT_VFS_BIN pointing to the current git-vfs binary"]
fn projected_jj_preserves_dirty_data_and_does_not_hash_untouched_blobs() {
    let binary = std::env::var_os("GIT_VFS_BIN")
        .or_else(|| option_env!("GIT_VFS_BIN").map(Into::into))
        .expect("set GIT_VFS_BIN");
    let binary = Path::new(&binary).canonicalize().unwrap().into_os_string();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let bare = temp.path().join("partial.git");
    let mount = temp.path().join("mount");
    let overlay = temp.path().join("overlay");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&mount).unwrap();
    git(&source, &["init", "--initial-branch=main"]);
    git(&source, &["config", "uploadpack.allowFilter", "true"]);
    git(
        &source,
        &["config", "uploadpack.allowAnySHA1InWant", "true"],
    );
    git(&source, &["config", "transfer.advertiseObjectInfo", "true"]);
    fs::write(source.join("tracked.txt"), "original\n").unwrap();
    fs::write(
        source.join("untouched.txt"),
        "this blob should remain absent\n",
    )
    .unwrap();
    fs::write(source.join(".gitignore"), "ignored/\n").unwrap();
    fs::create_dir(source.join("dir")).unwrap();
    fs::write(source.join("dir/tracked"), "tracked inside directory\n").unwrap();
    git(&source, &["add", "."]);
    git(&source, &["commit", "-m", "base"]);
    let untouched = text(git(&source, &["rev-parse", "HEAD:untouched.txt"]));
    run(
        temp.path(),
        &binary,
        &[
            "clone",
            &format!("file://{}", source.display()),
            bare.to_str().unwrap(),
        ],
    );
    assert!(
        !present(&bare, &untouched),
        "fixture must genuinely omit blobs"
    );
    let mounted = mount_vfs(&binary, &bare, &overlay, &mount);
    fs::create_dir(mount.join("subdirectory")).unwrap();
    let rejected = Command::new(
        Path::new(env!("CARGO_BIN_EXE_git-jj"))
            .canonicalize()
            .unwrap(),
    )
    .current_dir(mount.join("subdirectory"))
    .args(["init-vfs", "--overlay", overlay.to_str().unwrap()])
    .output()
    .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("mount root"));
    assert!(
        !mount.join("subdirectory/.jj").exists(),
        "rejection must precede state creation"
    );
    // Preexisting work must survive initialization, not be discarded by adoption.
    fs::write(mount.join("tracked.txt"), "edited before jj init\n").unwrap();
    jj(
        &mount,
        &["init-vfs", "--overlay", overlay.to_str().unwrap()],
    );
    jj(&mount, &["status"]);
    assert_eq!(
        fs::read_to_string(mount.join("tracked.txt")).unwrap(),
        "edited before jj init\n"
    );
    assert!(
        !present(&bare, &untouched),
        "jj initialization/status fetched an untouched file"
    );
    fs::write(mount.join("new.txt"), "new committed content\n").unwrap();
    fs::create_dir(mount.join("ignored")).unwrap();
    fs::write(mount.join("ignored/output"), "not tracked\n").unwrap();
    jj(&mount, &["describe", "-m", "projected edit"]);
    assert_eq!(
        text(jj(&mount, &["file", "show", "new.txt"])),
        "new committed content"
    );
    assert_eq!(
        text(jj(&mount, &["file", "show", "tracked.txt"])),
        "edited before jj init"
    );
    let files = text(jj(&mount, &["file", "list"]));
    assert!(!files.contains("ignored/output"));
    assert!(!files.contains(".jj/"));
    jj(&mount, &["new"]);
    fs::write(mount.join("tracked.txt"), "second change\n").unwrap();
    jj(&mount, &["status"]);
    assert_eq!(
        text(jj(&mount, &["file", "show", "tracked.txt"])),
        "second change"
    );
    jj(&mount, &["restore", "--from", "@-", "tracked.txt"]);
    assert_eq!(
        fs::read_to_string(mount.join("tracked.txt")).unwrap(),
        "edited before jj init\n"
    );
    // Restore a never-hydrated historical blob through native Git's promisor
    // transport, without fetching the unrelated file.
    jj(&mount, &["restore", "--from", "@--", "tracked.txt"]);
    assert_eq!(
        fs::read_to_string(mount.join("tracked.txt")).unwrap(),
        "original\n"
    );
    fs::rename(mount.join("new.txt"), mount.join("renamed.txt")).unwrap();
    fs::remove_file(mount.join("tracked.txt")).unwrap();
    std::os::unix::fs::symlink("renamed.txt", mount.join("link")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(mount.join("renamed.txt"), fs::Permissions::from_mode(0o755)).unwrap();
    jj(&mount, &["status"]);
    let files = text(jj(&mount, &["file", "list"]));
    assert!(files.contains("renamed.txt") && files.contains("link"));
    assert!(!files.contains("new.txt") && !files.contains("tracked.txt"));
    let commit = text(jj(
        &mount,
        &["log", "--no-graph", "-r", "@", "-T", "commit_id"],
    ));
    let mode = text(git(&mount, &["ls-tree", &commit, "renamed.txt"]));
    assert!(mode.starts_with("100755 blob"), "{mode}");
    assert!(
        !present(&overlay.join("git"), &untouched),
        "jj backend hydrated an unrelated blob"
    );
    drop(mounted);
    let remounted = mount_vfs(&binary, &bare, &overlay, &mount);
    jj(&mount, &["status"]);
    assert_eq!(
        text(jj(&mount, &["file", "show", "renamed.txt"])),
        "new committed content"
    );
    assert_eq!(
        fs::read_link(mount.join("link")).unwrap(),
        Path::new("renamed.txt")
    );
    assert!(
        !present(&bare, &untouched),
        "jj lifecycle fetched an unrelated base file"
    );
    // Replacing a tracked directory with a symlink must not read the target's
    // old child basenames, including through concurrent parent replacement.
    let outside = temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("tracked"), "OUTSIDE-SENTINEL-CONTENT\n").unwrap();
    let sentinel = text(git(
        &source,
        &["hash-object", outside.join("tracked").to_str().unwrap()],
    ));
    fs::remove_file(mount.join("dir/tracked")).unwrap();
    fs::remove_dir(mount.join("dir")).unwrap();
    std::os::unix::fs::symlink(&outside, mount.join("dir")).unwrap();
    jj(&mount, &["status"]);
    let commit = text(jj(
        &mount,
        &["log", "--no-graph", "-r", "@", "-T", "commit_id"],
    ));
    assert_eq!(
        text(git(&mount, &["show", &format!("{commit}:dir")])),
        outside.to_str().unwrap()
    );
    assert!(
        !present(&overlay.join("git"), &sentinel),
        "snapshot read and stored content outside its projection"
    );
    drop(remounted);
    // Reject gitlinks before creating jj state, rather than silently deleting
    // projected empty submodule directories during the first metadata scan.
    let parent = text(git(&source, &["rev-parse", "HEAD"]));
    git(
        &source,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{parent},submodule"),
        ],
    );
    git(&source, &["commit", "-m", "gitlink boundary"]);
    let other_bare = temp.path().join("submodule.git");
    run(
        temp.path(),
        &binary,
        &[
            "clone",
            &format!("file://{}", source.display()),
            other_bare.to_str().unwrap(),
        ],
    );
    let other_overlay = temp.path().join("submodule-overlay");
    let _submodule_mount = mount_vfs(&binary, &other_bare, &other_overlay, &mount);
    let rejected = Command::new(
        Path::new(env!("CARGO_BIN_EXE_git-jj"))
            .canonicalize()
            .unwrap(),
    )
    .current_dir(&mount)
    .args(["init-vfs", "--overlay", other_overlay.to_str().unwrap()])
    .output()
    .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("submodules"));
    assert!(
        !mount.join(".jj").exists(),
        "gitlink rejection must precede state creation"
    );
}
