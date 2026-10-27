use super::*;
use serde_json::json;
use std::{fs, path::Path, time::Duration};

fn private_fixture() -> (tempfile::TempDir, private_fs::PrivateDir) {
    let temporary = tempfile::tempdir().unwrap();
    let directory = private_fs::PrivateDir::create(temporary.path().join("private")).unwrap();
    (temporary, directory)
}

fn request() -> Request {
    Request {
        schema: 1,
        job_id: "11".repeat(32),
        attempt_id: "22".repeat(32),
        repository_id: "example".into(),
        candidate_commit: "33".repeat(32),
        root_tree: "44".repeat(32),
        profile_name: "unit".into(),
        profile_version: "55".repeat(32),
    }
}
fn profile(mode: Mode) -> Profile {
    Profile {
        name: "unit".into(),
        repositories: vec!["example".into()],
        store: "example".into(),
        mode,
        targets: vec!["//app:unit".into()],
        flags: vec![],
        timeout_seconds: 60,
        force_remote_execution: false,
    }
}
#[test]
fn request_and_receipt_bind_the_complete_physical_attempt() {
    let request = request();
    assert_eq!(
        Request::decode(&serde_json::to_vec(&request).unwrap()).unwrap(),
        request
    );
    let mut changed = request.clone();
    changed.root_tree = "66".repeat(32);
    assert_ne!(changed.workspace_id(), request.workspace_id());
    let mut receipt = Receipt {
        schema: 1,
        request: request.clone(),
        workspace_id: request.workspace_id(),
        outcome: Outcome::Passed,
        reason: "check_passed".into(),
        cleanup_confirmed: true,
        evidence: None,
    };
    assert!(receipt.validate_for(&request).is_err());
    receipt.outcome = Outcome::Unavailable;
    receipt.validate_for(&request).unwrap();
    assert!(receipt.validate_for(&changed).is_err());
    let mut bad = serde_json::to_value(&request).unwrap();
    bad["command"] = json!("arbitrary command");
    assert!(Request::decode(&serde_json::to_vec(&bad).unwrap()).is_err());
    assert!(Request::decode(&vec![b' '; MAX_REQUEST_BYTES + 1]).is_err());
}
fn events(
    mode: Mode,
    target: &str,
    test_status: Option<&str>,
    success: bool,
) -> Vec<serde_json::Value> {
    let mut events = vec![
        json!({"id":{"started":{}},"started":{"uuid":"01234567-89ab-cdef-0123-456789abcdef","command": mode.command()}}),
        json!({"id":{"targetCompleted":{"label":target}},"completed":{"success":success}}),
    ];
    if let Some(status) = test_status {
        events.push(json!({"id":{"testSummary":{"label":target}},"testSummary":{"overallStatus":status,"totalRunCount":1}}));
    }
    events.push(json!({"id":{"buildFinished":{}},"finished":{"overallSuccess":success,"exitCode":{"code":if success {0} else {3}}}}));
    events
}
fn write_events(path: &Path, events: &[serde_json::Value]) {
    fs::write(
        path,
        events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap() + "\n")
            .collect::<String>(),
    )
    .unwrap();
}
#[test]
fn bep_requires_exact_targets_and_real_test_results() {
    struct Case {
        name: &'static str,
        mode: Mode,
        target: &'static str,
        status: Option<&'static str>,
        success: bool,
        accepted: Option<bool>,
    }
    let cases = [
        Case {
            name: "build_success",
            mode: Mode::Build,
            target: "//app:unit",
            status: None,
            success: true,
            accepted: Some(true),
        },
        Case {
            name: "unrelated_target",
            mode: Mode::Build,
            target: "//other:unit",
            status: None,
            success: true,
            accepted: None,
        },
        Case {
            name: "test_build_only",
            mode: Mode::Test,
            target: "//app:unit",
            status: None,
            success: true,
            accepted: None,
        },
        Case {
            name: "test_passed",
            mode: Mode::Test,
            target: "//app:unit",
            status: Some("PASSED"),
            success: true,
            accepted: Some(true),
        },
        Case {
            name: "test_failed",
            mode: Mode::Test,
            target: "//app:unit",
            status: Some("FAILED"),
            success: false,
            accepted: Some(false),
        },
    ];
    for case in cases {
        let (_temporary, directory) = private_fixture();
        let bep = directory.path().join("bep");
        let compact = directory.path().join("compact");
        write_events(
            &bep,
            &events(case.mode, case.target, case.status, case.success),
        );
        fs::write(&compact, b"opaque compact execution log").unwrap();
        let result = bep::verify(
            &bep,
            &compact,
            &profile(case.mode),
            if case.success { 0 } else { 3 },
            65536,
            65536,
        );
        match case.accepted {
            Some(success) => assert_eq!(result.unwrap().success, success, "{}", case.name),
            None => assert!(result.is_err(), "{}", case.name),
        }
    }
}
#[test]
fn incomplete_or_oversized_bep_is_not_a_verdict() {
    let directory = tempfile::tempdir().unwrap();
    let bep = directory.path().join("bep");
    let compact = directory.path().join("compact");
    fs::write(&compact, b"log").unwrap();
    let mut values = events(Mode::Build, "//app:unit", None, true);
    values.pop();
    write_events(&bep, &values);
    assert!(bep::verify(&bep, &compact, &profile(Mode::Build), 0, 65536, 65536).is_err());
    fs::write(&bep, vec![b'x'; 65537]).unwrap();
    assert!(bep::verify(&bep, &compact, &profile(Mode::Build), 0, 65536, 65536).is_err());
}
#[tokio::test]
async fn subprocess_deadline_and_output_limit_are_owned() {
    let fixture = Path::new(env!("CHECK_PROCESS_FIXTURE"))
        .canonicalize()
        .unwrap();
    for (mode, timeout, limit) in [
        ("sleep", Duration::from_millis(10), 65536),
        ("output", Duration::from_secs(2), 1024),
    ] {
        let (_temporary, directory) = private_fixture();
        let stdout = directory.path().join("out");
        let stderr = directory.path().join("err");
        let mut command = tokio::process::Command::new(&fixture);
        command.arg(mode);
        let error = process::execute(
            command,
            &stdout,
            &stderr,
            timeout,
            &[(stdout.clone(), limit)],
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains(if mode == "sleep" {
                "process_timeout"
            } else {
                "process_output_limit"
            }),
            "unexpected failure: {error}"
        );
        process::require_quiescent(directory.path()).unwrap();
    }
}
#[test]
fn configuration_cannot_override_remote_or_source_policy() {
    assert!(config::target("//app:unit"));
    assert!(!config::target("//..."));
    assert!(!config::target("//app:all"));
    let bytes = include_str!("../example.toml");
    let config: Config = toml::from_str(bytes).unwrap();
    config.validate().unwrap();
    let mut oversized = config.clone();
    oversized.profiles[0].targets = (0..10)
        .map(|n| format!("//app:{}{n}", "x".repeat(900)))
        .collect();
    assert!(
        oversized.validate().is_err(),
        "accepted policy must fit encoded receipt budget"
    );
    for flag in [
        "--remote_executor=grpc://example.invalid",
        "--remote_cache=grpc://example.invalid",
        "--remote_proxy=unix:/tmp/other.sock",
        "--spawn_strategy=local",
        "--build_event_json_file=/tmp/other",
        "--config=hidden",
        "--remote_header=secret",
    ] {
        let mut candidate = config.clone();
        candidate.profiles[0].flags.push(flag.into());
        assert!(candidate.validate().is_err());
    }
}

#[tokio::test]
async fn unavailable_attempt_replays_and_capacity_fails_closed() {
    use sha2::{Digest, Sha256};
    let (_temporary, directory) = private_fixture();
    let fixture = Path::new(env!("CHECK_PROCESS_FIXTURE"))
        .canonicalize()
        .unwrap();
    let pin = format!("{:x}", Sha256::digest(fs::read(&fixture).unwrap()));
    let mut config: Config = toml::from_str(include_str!("../example.toml")).unwrap();
    config.scratch = directory.path().join("checks");
    config.daemon_config = directory.path().join("daemon.toml");
    fs::write(&config.daemon_config, b"# private fixture\n").unwrap();
    config.workspace = Binary {
        path: fixture.clone(),
        sha256: pin.clone(),
    };
    config.bazel = Binary {
        path: fixture,
        sha256: pin,
    };
    config.workspace_environment.clear();
    config.max_attempts = 1;
    let mut request = request();
    request.profile_version = config.profile_version("unit").unwrap();
    let (_cancel_sender, cancelled) = tokio::sync::watch::channel(false);
    let receipt = run(&config, request.clone(), cancelled.clone())
        .await
        .unwrap();
    assert_eq!(receipt.outcome, Outcome::Unavailable);
    assert!(
        !receipt.cleanup_confirmed,
        "fixture cannot confirm kernel/provider cleanup"
    );
    assert!(receipt.evidence.is_none());
    assert_eq!(
        run(&config, request.clone(), cancelled.clone())
            .await
            .unwrap(),
        receipt
    );
    let recovered = recover(&config, request.clone()).await.unwrap();
    assert_eq!(recovered.outcome, Outcome::Unavailable);
    assert!(!recovered.cleanup_confirmed);
    assert!(
        directory
            .path()
            .join("checks")
            .join(request.workspace_id())
            .join("attempt.json")
            .is_file()
    );
    let mut other = request.clone();
    other.attempt_id = "99".repeat(32);
    assert!(
        run(&config, other, cancelled).await.is_err(),
        "attempt quota must not overwrite existing recovery data"
    );
    let mut changed = config.clone();
    changed.profiles[0].targets = vec!["//app:other".into()];
    assert_ne!(
        changed.profile_version("unit").unwrap(),
        request.profile_version
    );
    // Cleanup authority comes from the retained private attempt, not a newer
    // operator profile. This does not recover unavailable rotated credentials.
    assert_eq!(
        recover(&changed, request).await.unwrap().outcome,
        Outcome::Unavailable
    );
}

#[tokio::test]
async fn leader_exit_does_not_abandon_a_term_resistant_descendant() {
    let fixture = Path::new(env!("CHECK_PROCESS_FIXTURE"))
        .canonicalize()
        .unwrap();
    let (_temporary, directory) = private_fixture();
    let heartbeat = directory.path().join("heartbeat");
    let stdout = directory.path().join("out");
    let stderr = directory.path().join("err");
    let mut command = tokio::process::Command::new(fixture);
    command.arg("leader-with-resistant-child").arg(&heartbeat);
    let observed_stdout = stdout.clone();
    let task = tokio::spawn(async move {
        process::execute(
            command,
            &stdout,
            &stderr,
            Duration::from_secs(10),
            &[],
            None,
        )
        .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !heartbeat.exists() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let leader = fs::read_to_string(&observed_stdout)
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    let state = || -> Option<u8> {
        let bytes = fs::read(format!("/proc/{leader}/stat")).ok()?;
        let end = bytes.iter().rposition(|b| *b == b')')?;
        bytes[end + 1..]
            .iter()
            .copied()
            .find(|b| !b.is_ascii_whitespace())
    };
    let mut anchored = false;
    while tokio::time::Instant::now() < deadline {
        match state() {
            Some(b'Z') => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                anchored = state() == Some(b'Z');
                break;
            }
            None => break,
            _ => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    assert_eq!(task.await.unwrap().unwrap(), 0);
    assert!(
        anchored,
        "exited leader must reserve its PID/PGID until descendant teardown completes"
    );
    let before = fs::metadata(&heartbeat).unwrap().len();
    assert!(before > 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fs::metadata(&heartbeat).unwrap().len(),
        before,
        "descendant must stop before command ownership returns"
    );
    process::require_quiescent(directory.path()).unwrap();
}

#[test]
fn early_completed_failure_explicitly_has_no_execution_log() {
    let directory = tempfile::tempdir().unwrap();
    let bep = directory.path().join("bep");
    let compact = directory.path().join("absent");
    let mut values = events(Mode::Build, "//app:unit", None, false);
    values.remove(1);
    values.last_mut().unwrap()["finished"]["exitCode"]["code"] = json!(1);
    write_events(&bep, &values);
    let result = bep::verify(&bep, &compact, &profile(Mode::Build), 1, 65536, 65536).unwrap();
    assert!(!result.success);
    assert!(result.evidence.execution_log_blake3.is_none());
    write_events(&bep, &events(Mode::Build, "//app:unit", None, true));
    assert!(
        bep::verify(&bep, &compact, &profile(Mode::Build), 0, 65536, 65536).is_err(),
        "passing check still requires execution evidence"
    );
}

#[tokio::test]
async fn dropping_a_check_handle_cancels_without_releasing_live_ownership() {
    use fs2::FileExt;
    use sha2::{Digest, Sha256};
    let (_temporary, directory) = private_fixture();
    let fixture = Path::new(env!("CHECK_PROCESS_FIXTURE"))
        .canonicalize()
        .unwrap();
    let pin = format!("{:x}", Sha256::digest(fs::read(&fixture).unwrap()));
    let mut config: Config = toml::from_str(include_str!("../example.toml")).unwrap();
    config.scratch = directory.path().join("checks");
    config.daemon_config = directory.path().join("daemon.toml");
    fs::write(&config.daemon_config, b"# blocking fixture\n").unwrap();
    config.workspace = Binary {
        path: fixture.clone(),
        sha256: pin.clone(),
    };
    config.bazel = Binary {
        path: fixture,
        sha256: pin,
    };
    config.workspace_environment.clear();
    let mut request = request();
    request.profile_version = config.profile_version("unit").unwrap();
    let attempt = config.scratch.join(request.workspace_id());
    let heartbeat = attempt.join("control-heartbeat");
    let (_sender, cancelled) = tokio::sync::watch::channel(false);
    let handle = start(config, request, cancelled);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !heartbeat.exists() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(attempt.join("owner.lock"))
        .unwrap();
    assert!(
        lock.try_lock_exclusive().is_err(),
        "live physical check must own its lock"
    );
    drop(handle);
    while lock.try_lock_exclusive().is_err() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(attempt.join("attempt.json")).unwrap()).unwrap();
    assert_eq!(journal["receipt"]["outcome"], "unavailable");
    let before = fs::metadata(&heartbeat).unwrap().len();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fs::metadata(&heartbeat).unwrap().len(),
        before,
        "cancelled physical child must stop before owner releases"
    );
}
