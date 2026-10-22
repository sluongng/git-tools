//! Native CLI startup under an adversarial ambient umask, not a kernel mount test.
#![cfg(unix)]
use std::{
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream, process::CommandExt},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn permissive_umask_still_binds_private_socket_and_preserves_replacement() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().canonicalize().unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        std::fs::metadata(&directory).unwrap().permissions().mode() & 0o077,
        0
    );
    let socket = directory.join("service.sock");
    let config = directory.join("server.toml");
    std::fs::write(&config,format!("storage = {:?}\nlisten = {:?}\nrepositories = [\"example\"]\ngc_interval_seconds = 0\n",directory.join("data").to_str().unwrap(),format!("unix://{}",socket.display()))).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_tinytree"));
    command
        .args(["--config", config.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    // This affects only the isolated child, never the parallel test runner.
    unsafe {
        command.pre_exec(|| {
            libc::umask(0);
            Ok(())
        });
    }
    let mut process = Process(command.spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut connection = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            let mut stderr = String::new();
            process
                .0
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("native server exited before ready ({status}): {stderr}");
        }
        if let Ok(connection) = UnixStream::connect(&socket) {
            break connection;
        }
        assert!(
            Instant::now() < deadline,
            "native Unix listener did not become ready"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    connection
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    connection
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
        .unwrap();
    let mut frame = [0; 9];
    connection.read_exact(&mut frame).unwrap();
    assert_eq!(frame[3], 4, "server must establish HTTP/2 before shutdown");
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    // The CLI must not unlink an endpoint it no longer owns during shutdown.
    std::fs::rename(&socket, directory.join("owned-socket-moved")).unwrap();
    std::fs::write(&socket, b"caller replacement").unwrap();
    drop(connection);
    assert_eq!(
        unsafe { libc::kill(process.0.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "native server did not drain");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "native CLI should drain cleanly: {status}"
    );
    assert_eq!(std::fs::read(&socket).unwrap(), b"caller replacement");
}
