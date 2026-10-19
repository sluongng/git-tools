#![cfg(all(windows, feature = "native"))]
use std::{path::Path, process::Command};
fn git(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
#[test]
#[ignore = "requires Windows 2004+, NTFS and enabled Client-ProjFS"]
fn writable_projection_persists_edits_and_tombstones_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    git(&source, &["init", "--initial-branch=main"]);
    git(&source, &["config", "user.name", "Projection Test"]);
    git(
        &source,
        &["config", "user.email", "projection@example.invalid"],
    );
    std::fs::write(source.join("edit.txt"), b"original\n").unwrap();
    std::fs::write(source.join("rename.txt"), b"lazy rename\n").unwrap();
    std::fs::write(source.join("delete.txt"), b"delete\n").unwrap();
    std::fs::create_dir(source.join("directory")).unwrap();
    std::fs::write(source.join("directory/child.txt"), b"child\n").unwrap();
    git(&source, &["add", "."]);
    let link_target = temp.path().join("link-target");
    std::fs::write(&link_target, b"directory").unwrap();
    let oid = git(
        &source,
        &["hash-object", "-w", link_target.to_str().unwrap()],
    );
    git(
        &source,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("120000,{},directory-link", oid.trim()),
        ],
    );
    git(&source, &["commit", "-m", "fixture"]);
    let repository = temp.path().join("source.git");
    let output = Command::new("git")
        .args(["clone", "--bare"])
        .arg(&source)
        .arg(&repository)
        .output()
        .unwrap();
    assert!(output.status.success());
    let root = temp.path().join("root");
    let state = temp.path().join("state");
    let projection =
        workspace_projfs::windows::start(&repository, "HEAD", &root, &state, Path::new("git"))
            .unwrap();
    let names: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(names.contains(&"edit.txt".into()));
    assert_eq!(
        std::fs::read(root.join("directory/child.txt")).unwrap(),
        b"child\n"
    );
    assert_eq!(
        std::fs::read(root.join("directory-link/child.txt")).unwrap(),
        b"child\n"
    );
    assert_eq!(
        std::fs::read_link(root.join("directory-link")).unwrap(),
        Path::new("directory")
    );
    std::fs::create_dir(root.join("local-directory")).unwrap();
    std::fs::write(root.join("local-directory/child.txt"), b"local\n").unwrap();
    assert_eq!(
        std::fs::read_dir(root.join("local-directory"))
            .unwrap()
            .count(),
        1
    );
    std::fs::rename(root.join("rename.txt"), root.join("renamed.txt")).unwrap();
    assert_eq!(
        std::fs::read(root.join("renamed.txt")).unwrap(),
        b"lazy rename\n"
    );
    std::fs::write(root.join("edit.txt"), b"edited\n").unwrap();
    std::fs::remove_file(root.join("delete.txt")).unwrap();
    std::fs::write(root.join("new.txt"), b"new\n").unwrap();
    assert!(std::fs::rename(root.join("directory"), root.join("other")).is_err());
    let status = git(&root, &["status", "--porcelain"]);
    assert!(
        status.contains("edit.txt") && status.contains("delete.txt") && status.contains("new.txt")
    );
    git(&root, &["config", "user.name", "Projection Test"]);
    git(
        &root,
        &["config", "user.email", "projection@example.invalid"],
    );
    git(&root, &["add", "-A"]);
    git(&root, &["commit", "-m", "projected edit"]);
    let new_head = git(&root, &["rev-parse", "HEAD"]);
    assert!(git(&root, &["log", "--oneline", "-1"]).contains("projected edit"));
    drop(projection);
    let projection =
        workspace_projfs::windows::start(&repository, "HEAD", &root, &state, Path::new("git"))
            .unwrap();
    assert_eq!(git(&root, &["rev-parse", "HEAD"]), new_head);
    assert_eq!(std::fs::read(root.join("edit.txt")).unwrap(), b"edited\n");
    assert_eq!(
        std::fs::read(root.join("renamed.txt")).unwrap(),
        b"lazy rename\n"
    );
    assert!(!root.join("rename.txt").exists());
    assert!(!root.join("delete.txt").exists());
    assert_eq!(std::fs::read(root.join("new.txt")).unwrap(), b"new\n");
    assert_eq!(
        std::fs::read(root.join("local-directory/child.txt")).unwrap(),
        b"local\n"
    );
    assert!(
        workspace_projfs::windows::start(&repository, "HEAD", &root, &state, Path::new("git"))
            .is_err()
    );
    drop(projection);
}
