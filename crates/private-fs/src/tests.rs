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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(private.path().join("state.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
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
#[cfg(unix)]
#[test]
fn durable_nested_creation_uses_trusted_existing_anchor_without_repair() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = root();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = root.path().join("actor/daemon/native");
    let private = PrivateDir::create_all_durable(&path).unwrap();
    for name in ["actor", "actor/daemon", "actor/daemon/native"] {
        assert_eq!(
            std::fs::metadata(root.path().join(name)).unwrap().mode() & 0o777,
            0o700
        );
    }
    assert_eq!(
        std::fs::metadata(root.path()).unwrap().mode() & 0o777,
        0o755
    );
    private.replace("identity", b"durable").unwrap();
    drop(private);
    let reopened = PrivateDir::create_all_durable(&path).unwrap();
    assert_eq!(reopened.read("identity", 7).unwrap(), b"durable");
    assert!(PrivateDir::create_all_durable(path.join("identity/child")).is_err());
    assert!(PrivateDir::create_all_durable(root.path()).is_err());
    assert!(PrivateDir::create_all_durable("relative/state").is_err());
    assert!(PrivateDir::create_all_durable(root.path().join("../escape")).is_err());
    assert!(
        PrivateDir::create_all_durable(format!("{}/actor//alias", root.path().display())).is_err()
    );
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(PrivateDir::create_all_durable(root.path().join("must-not-exist/child")).is_err());
    assert!(!root.path().join("must-not-exist").exists());
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
    assert!(PrivateDir::create_all_durable(&path).is_err());
    assert_eq!(std::fs::metadata(path).unwrap().mode() & 0o777, 0o755);
    let private = PrivateDir::create(root.path().join("private")).unwrap();
    let child = private.path().join("jj-store");
    std::fs::create_dir(&child).unwrap();
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(child.join("type"), b"native").unwrap();
    assert!(PrivateDir::open_existing(&child).is_err());
    assert_eq!(
        private.read_descendant("jj-store/type", 6).unwrap(),
        b"native"
    );
    assert_eq!(
        std::fs::metadata(&child).unwrap().mode() & 0o777,
        0o755,
        "readonly descendant access never repairs or adopts the child"
    );
    assert!(private.read_descendant("jj-store/type", 5).is_err());
    assert!(private.read_descendant("../escape", 6).is_err());
    assert!(private.read_descendant(&child, 6).is_err());
    assert!(private.read_descendant("jj-store", 6).is_err());
    std::os::unix::fs::symlink(&child, private.path().join("alias")).unwrap();
    assert!(private.read_descendant("alias/type", 6).is_err());
    std::os::unix::fs::symlink(child.join("type"), child.join("type-link")).unwrap();
    assert!(private.read_descendant("jj-store/type-link", 6).is_err());
    private
        .create_descendant_file("jj-store/locator", b"configured")
        .unwrap();
    assert_eq!(
        private.read_descendant("jj-store/locator", 10).unwrap(),
        b"configured"
    );
    assert_eq!(
        std::fs::metadata(child.join("locator")).unwrap().mode() & 0o777,
        0o600
    );
    assert!(
        private
            .create_descendant_file("jj-store/locator", b"replacement")
            .is_err()
    );
    assert_eq!(std::fs::read(child.join("locator")).unwrap(), b"configured");
    assert!(
        private
            .create_descendant_file("alias/new", b"denied")
            .is_err()
    );
    assert!(!child.join("new").exists());
    assert!(
        private
            .create_descendant_file("jj-store/type-link", b"denied")
            .is_err()
    );
    assert_eq!(std::fs::read(child.join("type")).unwrap(), b"native");
    assert!(
        private
            .create_descendant_file("../escape", b"denied")
            .is_err()
    );
    assert!(private.create_descendant_file(&child, b"denied").is_err());
    std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(private.read_descendant("jj-store/type", 6).is_err());
    assert!(
        private
            .create_descendant_file("jj-store/unsafe", b"denied")
            .is_err()
    );
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
    assert!(PrivateDir::create_all_durable(alias.join("nested/child")).is_err());
    assert!(!real.path().join("child").exists());
    let outside = root.path().join("outside");
    std::fs::write(&outside, b"do not read").unwrap();
    symlink(&outside, real.path().join("link")).unwrap();
    assert!(real.read("link", 100).is_err());
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires native Linux Landlock"]
fn private_directory_operations_under_landlock() {
    const CHILD_ROOT: &str = "PRIVATE_FS_LANDLOCK_CHILD_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        confined_private_directory_operations(Path::new(&root));
        return;
    }

    let parent = root();
    let allowed = PrivateDir::create(parent.path().join("allowed")).unwrap();
    let real = PrivateDir::create(allowed.path().join("real")).unwrap();
    std::os::unix::fs::symlink(real.path(), allowed.path().join("alias")).unwrap();
    std::fs::write(parent.path().join("outside"), b"preserve").unwrap();
    // Restriction is irreversible: isolate it from the parent test harness and
    // leave fixture cleanup to that unrestricted parent after the child waits.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "tests::private_directory_operations_under_landlock",
        ])
        .env_clear()
        .env(CHILD_ROOT, parent.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "confined private-directory workflow failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(parent.path().join("outside")).unwrap(),
        b"preserve"
    );
    assert!(!real.path().join("child").exists());
}

#[cfg(target_os = "linux")]
fn confined_private_directory_operations(root: &Path) {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
    }
    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }
    // Stable Linux Landlock ABI v1 filesystem rights. No host-policy or resource
    // limit is changed; unsupported kernels fail this native acceptance test.
    let access = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8);
    let attr = RulesetAttr {
        handled_access_fs: access,
    };
    let raw = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &attr as *const RulesetAttr,
            std::mem::size_of_val(&attr),
            0,
        )
    };
    assert!(raw >= 0, "Landlock ruleset: {}", io::Error::last_os_error());
    let ruleset = unsafe { OwnedFd::from_raw_fd(raw as i32) };
    let allowed = root.join("allowed");
    let directory = File::open(&allowed).unwrap();
    let beneath = PathBeneathAttr {
        allowed_access: access,
        parent_fd: directory.as_raw_fd(),
    };
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset.as_raw_fd(),
                1,
                &beneath as *const PathBeneathAttr,
                0,
            )
        },
        0,
        "Landlock path rule: {}",
        io::Error::last_os_error()
    );
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
        0
    );
    assert_eq!(
        unsafe { libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0) },
        0,
        "Landlock restriction: {}",
        io::Error::last_os_error()
    );
    drop(directory);
    drop(ruleset);

    assert_eq!(
        std::fs::read_dir("/").unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        std::fs::read_dir(root).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        std::fs::read(root.join("outside")).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    let private = PrivateDir::create(allowed.join("state")).unwrap();
    private.open_lock("owner.lock").unwrap();
    private.replace("record", b"first").unwrap();
    assert_eq!(private.read("record", 5).unwrap(), b"first");
    let child = PrivateDir::create(private.path().join("child")).unwrap();
    child.replace("data", b"bounded").unwrap();
    drop(child);
    private.check_child_tree("child", 4, 2).unwrap();
    private.remove_child_tree("child", 4, 2).unwrap();
    drop(private);
    let reopened = PrivateDir::open_existing(allowed.join("state")).unwrap();
    assert_eq!(reopened.read("record", 5).unwrap(), b"first");
    let nested = allowed.join("actor/daemon/native");
    let durable = PrivateDir::create_all_durable(&nested).unwrap();
    durable
        .replace("sourceidentity.json", b"public-registry-identity")
        .unwrap();
    drop(durable);
    let reopened = PrivateDir::create_all_durable(&nested).unwrap();
    assert_eq!(
        reopened.read("sourceidentity.json", 24).unwrap(),
        b"public-registry-identity"
    );
    assert!(PrivateDir::create_all_durable(allowed.join("alias/nested/child")).is_err());
    assert_eq!(
        std::fs::read_dir("/").unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        std::fs::read_dir(root).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        std::fs::read(root.join("outside")).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(PrivateDir::create(allowed.join("alias/child")).is_err());
    assert!(!allowed.join("real/child").exists());
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

#[test]
fn existing_file_handle_is_bounded_readonly_and_never_creates() {
    let root = root();
    let private = PrivateDir::create(root.path().join("private")).unwrap();
    assert!(private.open_existing_file("missing", 100).is_err());
    assert!(!private.path().join("missing").exists());
    // The persistent private lock is a convenient supported private-file fixture.
    let mut writer = private.open_lock("payload").unwrap();
    writer.write_all(b"bounded").unwrap();
    drop(writer);
    let mut reader = private.open_existing_file("payload", 7).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"bounded");
    assert!(reader.write_all(b"bad").is_err());
    assert!(private.open_existing_file("payload", 6).is_err());
    assert!(private.open_existing_file("../payload", 7).is_err());
    assert!(private.open_existing_file(".", 7).is_err());
    #[cfg(unix)]
    {
        use std::{
            ffi::CString,
            os::unix::{
                ffi::OsStrExt,
                fs::{PermissionsExt, symlink},
            },
        };
        symlink(private.path().join("payload"), private.path().join("link")).unwrap();
        assert!(private.open_existing_file("link", 7).is_err());
        let fifo = CString::new(private.path().join("fifo").as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(
            private.open_existing_file("fifo", 7).is_err(),
            "must not block on FIFO"
        );
        std::fs::set_permissions(
            private.path().join("payload"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(private.open_existing_file("payload", 7).is_err());
        assert_eq!(
            std::fs::read(private.path().join("payload")).unwrap(),
            b"bounded"
        );
    }
}

#[cfg(unix)]
#[test]
fn disposable_cleanup_is_bounded_nofollow_and_replayable() {
    use std::os::unix::fs::symlink;
    let temp = root();
    let parent = PrivateDir::create(temp.path().join("private")).unwrap();
    let child = PrivateDir::create(parent.path().join("candidate")).unwrap();
    child.replace("data", b"candidate").unwrap();
    let foreign = temp.path().join("foreign");
    std::fs::write(&foreign, b"preserve").unwrap();
    symlink(&foreign, child.path().join("link")).unwrap();
    assert!(parent.remove_child_tree("candidate", 16, 4).is_err());
    assert_eq!(
        std::fs::read(child.path().join("data")).unwrap(),
        b"candidate"
    );
    assert_eq!(std::fs::read(&foreign).unwrap(), b"preserve");
    std::fs::remove_file(child.path().join("link")).unwrap();
    assert!(parent.remove_child_tree("candidate", 0, 4).is_err());
    assert!(parent.remove_child_tree("../foreign", 16, 4).is_err());
    parent.remove_child_tree("candidate", 16, 4).unwrap();
    parent.remove_child_tree("candidate", 16, 4).unwrap();
    assert_eq!(std::fs::read(foreign).unwrap(), b"preserve");
}
