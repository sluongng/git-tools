use std::{io::Write, time::Duration};
fn main() {
    let arguments: Vec<_> = std::env::args().collect();
    if arguments.get(1).map(String::as_str) == Some("--config")
        && arguments.get(3).map(String::as_str) == Some("native-candidate-mount")
        && std::fs::read_to_string(&arguments[2]).is_ok_and(|s| s.contains("blocking fixture"))
    {
        let mut heartbeat = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("control-heartbeat")
            .unwrap();
        loop {
            heartbeat.write_all(b"tick\n").unwrap();
            heartbeat.flush().unwrap();
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    match std::env::args().nth(1).as_deref() {
        Some("sleep") => std::thread::sleep(Duration::from_secs(10)),
        Some("output") => std::io::stdout().write_all(&vec![b'x'; 65536]).unwrap(),
        Some("resistant-child") => {
            #[cfg(unix)]
            unsafe {
                libc::signal(libc::SIGTERM, libc::SIG_IGN);
            }
            #[cfg(target_os = "linux")]
            unsafe {
                assert_eq!(
                    libc::prctl(libc::PR_SET_NAME, b"fixture-\xff)-name\0".as_ptr()),
                    0
                );
            }
            let path = std::env::args().nth(2).unwrap();
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                file.write_all(b"tick\n").unwrap();
                file.flush().unwrap();
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        Some("leader-with-resistant-child") => {
            println!("{}", std::process::id());
            std::io::stdout().flush().unwrap();
            let path = std::env::args().nth(2).unwrap();
            let _child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("resistant-child")
                .arg(&path)
                .spawn()
                .unwrap();
            while std::fs::metadata(&path).map_or(true, |m| m.len() == 0) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        _ => std::process::exit(2),
    }
}
