//! Declared subprocess fixture: occupy descriptors before execing the real CLI.
#[cfg(unix)]
fn main() {
    use std::{
        os::{fd::AsRawFd, unix::process::CommandExt},
        process::Command,
    };
    let mut args = std::env::args_os().skip(1);
    let program = args.next().expect("CLI fixture path");
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
