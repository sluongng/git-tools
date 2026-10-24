//! Declared protocol fixture only. It never mounts source or establishes RBE
//! acceptance; production acceptance uses the real pinned check runner.
use std::{
    io::{Read, Write},
    path::PathBuf,
    time::Duration,
};
use tinytree_check_runner::{Config, Evidence, Outcome, Receipt, Request};
fn main() {
    let arguments: Vec<_> = std::env::args().collect();
    assert_eq!(arguments.get(1).map(String::as_str), Some("--config"));
    let config = Config::load(&PathBuf::from(&arguments[2])).unwrap();
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(16385)
        .read_to_end(&mut bytes)
        .unwrap();
    let request = Request::decode(&bytes).unwrap();
    let profile = config.profile(&request.profile_name).unwrap();
    let mode = profile
        .flags
        .iter()
        .find_map(|s| s.strip_prefix("--define=fixture_mode="))
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        match std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&config.scratch)
        {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => panic!("{e}"),
        }
    }
    let state = config
        .scratch
        .join(format!("{}.json", request.workspace_id()));
    let mut executions = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config.scratch.join("executions"))
        .unwrap();
    writeln!(executions, "{}", arguments[3]).unwrap();
    executions.sync_all().unwrap();
    if arguments[3] == "recover" && state.exists() {
        let mut receipt: Receipt = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
        if mode == "uncertain" && config.scratch.join("release").exists() {
            receipt.cleanup_confirmed = true;
        }
        println!("{}", serde_json::to_string(&receipt).unwrap());
        return;
    }
    if mode == "late_pass" {
        #[cfg(unix)]
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
        std::fs::write(config.scratch.join("started"), b"late fixture active").unwrap();
        std::thread::sleep(Duration::from_secs(12));
    }
    if mode == "pause" {
        std::fs::write(config.scratch.join("started"), b"fixture check active").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !config.scratch.join("release").exists() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let outcome = if mode == "fail" {
        Outcome::Failed
    } else if mode == "uncertain" {
        Outcome::Unavailable
    } else {
        Outcome::Passed
    };
    let mut receipt = Receipt {
        schema: 1,
        workspace_id: request.workspace_id(),
        request: request.clone(),
        outcome,
        reason: "fixture_result".into(),
        cleanup_confirmed: mode != "uncertain",
        evidence: if outcome == Outcome::Unavailable {
            None
        } else {
            Some(Evidence {
                invocation_id: "00000000-0000-0000-0000-000000000001".into(),
                bep_blake3: "11".repeat(32),
                execution_log_blake3: if outcome == Outcome::Passed {
                    Some("22".repeat(32))
                } else {
                    None
                },
                requested_targets: profile.targets.clone(),
                tests_verified: profile.mode == tinytree_check_runner::Mode::Test,
                bazel_exit_code: if outcome == Outcome::Passed { 0 } else { 1 },
            })
        },
    };
    if mode == "mismatch" {
        receipt.request.root_tree = "00".repeat(32);
    }
    let bytes = serde_json::to_vec(&receipt).unwrap();
    let mut output = std::fs::File::create(&state).unwrap();
    output.write_all(&bytes).unwrap();
    output.sync_all().unwrap();
    if mode == "lost_reply" {
        std::process::exit(1)
    }
    std::io::stdout().write_all(&bytes).unwrap();
    std::io::stdout().write_all(b"\n").unwrap();
}
