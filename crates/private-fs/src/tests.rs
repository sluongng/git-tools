use super::*;

fn root() -> tempfile::TempDir {
    // On Unix the test harness can select a private path underneath a symlinked
    // temporary root. Resolve only this fixture parent, not the path under test.
    let parent = std::env::temp_dir().canonicalize().unwrap();
    tempfile::Builder::new()
        .prefix("private-fs-")
        .tempdir_in(parent)
        .unwrap()
}
#[test]
fn creation_publication_and_bounded_reads() {
    let parent = root();
    let path = parent.path().join("state");
    let private = PrivateDir::create(&path).unwrap();
    assert_eq!(private.path(), path);
    let lock = private.open_lock("command.lock").unwrap();
    assert!(lock.metadata().unwrap().is_file());
    private.replace("state.json", b"first").unwrap();
    assert_eq!(private.read("state.json", 5).unwrap(), b"first");
    assert!(private.read("state.json", 4).is_err());
    private.replace("state.json", b"second").unwrap();
    assert_eq!(private.read("state.json", 6).unwrap(), b"second");
    assert!(private.replace("../escape", b"bad").is_err());
    assert!(private.read("../escape", 10).is_err());
    assert!(private.replace("", b"bad").is_err());
    drop(private);
    assert_eq!(
        PrivateDir::open_existing(&path)
            .unwrap()
            .read("state.json", 6)
            .unwrap(),
        b"second"
    );
}
#[test]
fn nonabsolute_and_non_normalized_paths_are_rejected() {
    assert!(PrivateDir::create("relative/state").is_err());
    let root = root();
    assert!(PrivateDir::create(root.path().join("../state")).is_err());
    let ordinary_file = root.path().join("file");
    std::fs::write(&ordinary_file, b"untouched").unwrap();
    assert!(PrivateDir::create(&ordinary_file).is_err());
    assert_eq!(std::fs::read(ordinary_file).unwrap(), b"untouched");
}
#[cfg(unix)]
#[test]
fn permissive_existing_directories_are_not_repaired() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = root();
    let path = root.path().join("state");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(PrivateDir::open_existing(&path).is_err());
    assert!(PrivateDir::create(&path).is_err());
    assert_eq!(std::fs::metadata(path).unwrap().mode() & 0o777, 0o755);
}
#[cfg(unix)]
#[test]
fn symlink_ancestors_and_files_are_rejected() {
    use std::os::unix::fs::symlink;
    let root = root();
    let real = PrivateDir::create(root.path().join("real")).unwrap();
    let alias = root.path().join("alias");
    symlink(real.path(), &alias).unwrap();
    assert!(PrivateDir::open_existing(&alias).is_err());
    assert!(PrivateDir::create(alias.join("child")).is_err());
    assert!(!real.path().join("child").exists());
    let outside = root.path().join("outside");
    std::fs::write(&outside, b"do not read").unwrap();
    symlink(&outside, real.path().join("link")).unwrap();
    assert!(real.read("link", 100).is_err());
}
#[cfg(windows)]
#[test]
fn windows_owner_only_acl_and_directory_lifetime_are_native() {
    use std::os::windows::io::AsRawHandle;
    let parent = root();
    let path = parent.path().join("private");
    let private = PrivateDir::create(&path).unwrap();
    // The live handle was checked against the kernel ACL, not string SID names.
    unsafe { windows::verify_private_handle(private._chain.last().unwrap().as_raw_handle()) }
        .unwrap();
    // Keeping the chain forbids replacement of a directory used by a live job.
    assert!(std::fs::rename(&path, parent.path().join("moved")).is_err());
    assert!(
        windows::CurrentUser::get()
            .unwrap()
            .same_process_user(std::process::id())
            .unwrap()
    );
    drop(private);
    std::fs::rename(&path, parent.path().join("moved")).unwrap();
}
