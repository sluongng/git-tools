//! Declared subprocess fixture: occupy descriptors before execing the real CLI.
#[cfg(unix)]
fn main() {
    use std::{
        os::{fd::AsRawFd, unix::process::CommandExt},
        process::Command,
    };
    let mut args = std::env::args_os().skip(1);
    let program = args.next().expect("CLI fixture path");
    if program == "--credential-probe" {
        credential_probe(args.collect());
        return;
    }
    let files = (0..300)
        .map(|_| std::fs::File::open("/dev/null").unwrap())
        .collect::<Vec<_>>();
    for file in &files {
        let descriptor = file.as_raw_fd();
        unsafe {
            let flags = libc::fcntl(descriptor, libc::F_GETFD);
            assert!(flags >= 0);
            assert_eq!(
                libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC),
                0
            );
        }
    }
    let error = Command::new(program).args(args).exec();
    panic!("cannot exec watcher fixture: {error}");
}
#[cfg(not(unix))]
fn main() {
    panic!("descriptor-pressure fixture requires Unix");
}

/// Exercise real Git's credential protocol locally, never open a network socket.
#[cfg(unix)]
fn credential_probe(mut args: Vec<std::ffi::OsString>) {
    use std::{
        io::Write,
        os::unix::process::CommandExt,
        process::{Command, Stdio},
    };
    let git = args.remove(0);
    if let Some(index) = args.iter().position(|a| a == "ls-remote") {
        let mut child = Command::new(&git)
            .args(&args[..index])
            .args(["credential", "fill"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"protocol=https\nhost=example.invalid\n\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        // Deliberately malformed, credential-like transport output must never
        // enter push status/errors. The real helper output is not printed.
        if output.status.success() {
            println!("opaque-neutral-credential");
            std::process::exit(0);
        }
        std::process::exit(1);
    }
    let _ = Command::new(git).args(args).exec();
    std::process::exit(90);
}
