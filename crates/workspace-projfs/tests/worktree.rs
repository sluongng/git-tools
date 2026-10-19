use std::{path::Path, process::Command};
fn git(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
#[test]
fn isolated_git_index_commits_survive_reopen_without_modifying_source() {
    if let Some(request) = std::env::var_os("GIT_PROJFS_PREPARE_TEST_REQUEST") {
        let (bare, root, state, commit): (
            std::path::PathBuf,
            std::path::PathBuf,
            std::path::PathBuf,
            String,
        ) = serde_json::from_str(&request.to_string_lossy()).unwrap();
        workspace_projfs::worktree::prepare(&bare, &root, &state, &commit, Path::new("git"))
            .unwrap();
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    for path in [&source, &root, &state] {
        std::fs::create_dir(path).unwrap();
    }
    git(&source, &["init", "--initial-branch=main"]);
    git(&source, &["config", "user.name", "Projection Test"]);
    git(
        &source,
        &["config", "user.email", "projection@example.invalid"],
    );
    std::fs::write(source.join("file"), b"original\n").unwrap();
    git(&source, &["add", "file"]);
    git(&source, &["commit", "-m", "initial"]);
    let commit = git(&source, &["rev-parse", "HEAD"]).trim().to_owned();
    let bare = temp.path().join("source.git");
    let cloned = Command::new("git")
        .args(["clone", "--bare"])
        .arg(&source)
        .arg(&bare)
        .output()
        .unwrap();
    assert!(cloned.status.success());
    let ambient = temp.path().join("ambient.git");
    let initialized = Command::new("git")
        .args(["init", "--bare"])
        .arg(&ambient)
        .output()
        .unwrap();
    assert!(initialized.status.success());
    git(
        &ambient,
        &[
            "config",
            "remote.origin.url",
            "https://unrelated.invalid/repository.git",
        ],
    );
    let request = serde_json::to_string(&(&bare, &root, &state, &commit)).unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_git_index_commits_survive_reopen_without_modifying_source",
            "--nocapture",
        ])
        .env("GIT_PROJFS_PREPARE_TEST_REQUEST", request)
        .env("GIT_COMMON_DIR", &ambient)
        .env("GIT_DIR", &ambient)
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "remote.origin.url")
        .env(
            "GIT_CONFIG_VALUE_0",
            "https://injected.invalid/repository.git",
        )
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{} {}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    assert_eq!(
        git(&root, &["config", "--local", "--get", "remote.origin.url"]),
        git(&bare, &["config", "--local", "--get", "remote.origin.url"])
    );
    assert!(
        !root.join("file").exists(),
        "index preparation must not checkout files"
    );
    assert!(git(&root, &["status", "--porcelain"]).contains(" D file"));
    // On Windows the provider supplies this file. This test isolates native Git
    // admin/index semantics, not hydration or ProjFS runtime behavior.
    std::fs::write(root.join("file"), b"original\n").unwrap();
    assert!(git(&root, &["status", "--porcelain"]).is_empty());
    git(&root, &["config", "user.name", "Projection Test"]);
    git(
        &root,
        &["config", "user.email", "projection@example.invalid"],
    );
    std::fs::write(root.join("file"), b"edited\n").unwrap();
    git(&root, &["add", "file"]);
    let staged = std::fs::read(state.join("git/index")).unwrap();
    workspace_projfs::worktree::prepare(&bare, &root, &state, &commit, Path::new("git")).unwrap();
    assert_eq!(staged, std::fs::read(state.join("git/index")).unwrap());
    git(&root, &["commit", "-m", "agent edit"]);
    assert!(git(&root, &["log", "--oneline", "-1"]).contains("agent edit"));
    let advanced = git(&root, &["rev-parse", "HEAD"]);
    assert_ne!(advanced.trim(), commit);
    workspace_projfs::worktree::prepare(&bare, &root, &state, &commit, Path::new("git")).unwrap();
    assert_eq!(git(&root, &["rev-parse", "HEAD"]), advanced);
    assert_eq!(git(&bare, &["rev-parse", "HEAD"]).trim(), commit);
    let pointer = std::fs::read(root.join(".git")).unwrap();
    std::fs::write(root.join(".git"), b"gitdir: unrelated\n").unwrap();
    assert!(
        workspace_projfs::worktree::prepare(&bare, &root, &state, &commit, Path::new("git"))
            .is_err()
    );
    assert_eq!(
        std::fs::read(root.join(".git")).unwrap(),
        b"gitdir: unrelated\n"
    );
    std::fs::write(root.join(".git"), pointer).unwrap();
    assert!(
        workspace_projfs::worktree::prepare(
            &bare,
            &root,
            &state,
            "0000000000000000000000000000000000000000",
            Path::new("git")
        )
        .is_err()
    );
}
