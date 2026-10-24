//! Real abrupt-process tests with a declared, owned child executable.
//! SIGKILL/process termination is not a simulated machine/power failure.
mod common;

use std::io::{self, BufRead, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tinytree_store::{Config, Store};

const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(20);
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);

struct Driver {
    child: Option<Child>,
    events: Option<mpsc::Receiver<io::Result<String>>>,
    output: Option<JoinHandle<()>>,
}
impl Driver {
    fn start(root: &Path, mode: &str) -> Self {
        let executable = Path::new(env!("CARGO_BIN_EXE_tinytree-durability-driver"))
            .canonicalize()
            .unwrap();
        let mut child = Command::new(executable)
            .arg(mode)
            .arg(root)
            .env("PATH", "")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(8);
        let output = thread::spawn(move || {
            let mut reader = io::BufReader::new(stdout);
            loop {
                let mut line = Vec::new();
                match reader.by_ref().take(1025).read_until(b'\n', &mut line) {
                    Ok(0) => break,
                    Ok(_) if line.len() > 1024 || !line.ends_with(b"\n") => {
                        let _ =
                            tx.send(Err(io::Error::other("invalid/oversized driver checkpoint")));
                        break;
                    }
                    Ok(_) => {
                        let result = String::from_utf8(line)
                            .map(|s| s.trim_end_matches('\n').to_owned())
                            .map_err(|_| io::Error::other("non-UTF8 driver checkpoint"));
                        if tx.send(result).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error));
                        break;
                    }
                }
            }
        });
        Self {
            child: Some(child),
            events: Some(rx),
            output: Some(output),
        }
    }
    fn checkpoint(&self, expected: &str) {
        let observed = self
            .events
            .as_ref()
            .unwrap()
            .recv_timeout(CHECKPOINT_TIMEOUT)
            .expect("driver did not reach checkpoint before timeout")
            .expect("driver checkpoint I/O");
        assert_eq!(observed, expected);
    }
    fn kill_and_reap(&mut self) {
        let child = self.child.as_mut().unwrap();
        assert!(
            child.try_wait().unwrap().is_none(),
            "driver exited before abrupt termination"
        );
        child.kill().unwrap();
        let status =
            wait_until_exit(child, EXIT_TIMEOUT).expect("owned driver did not exit after kill");
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        }
        self.child.take();
        self.events.take();
        self.output
            .take()
            .unwrap()
            .join()
            .expect("checkpoint reader panicked");
    }
}
fn wait_until_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "driver exit timeout",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.events.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            if wait_until_exit(&mut child, EXIT_TIMEOUT).is_err() {
                // Failure-only best effort: keep ownership in a reaper rather
                // than block unwinding forever or leave an eventual zombie.
                let _ = thread::spawn(move || {
                    let _ = child.wait();
                });
            }
        }
        if let Some(output) = self.output.take() {
            if output.is_finished() {
                let _ = output.join();
            }
        }
    }
}

fn verify_fixture(store: &Store, fixture: &common::Fixture) {
    let mut file = store.read(common::REPOSITORY, fixture.file.key).unwrap();
    assert_eq!(file.descriptor(), fixture.file);
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, fixture.file_bytes);
    for object in &fixture.metadata {
        let (descriptor, bytes) = store
            .read_metadata(common::REPOSITORY, object.descriptor.key)
            .unwrap();
        assert_eq!(descriptor, object.descriptor);
        assert_eq!(bytes, object.bytes);
    }
}
fn verify_initial(store: &Store) {
    let initial = common::initial();
    verify_fixture(store, &initial);
    let reference = store
        .references(common::REPOSITORY, &[common::REFERENCE.into()])
        .unwrap()
        .remove(0);
    assert_eq!(reference.generation, 1);
    assert_eq!(reference.commit, Some(initial.head));
    let receipt = store
        .publication(common::REPOSITORY, "initial")
        .unwrap()
        .unwrap();
    assert_eq!(receipt.reference, reference);
    assert_eq!(receipt.idempotency_key, "initial");
}

#[test]
fn acknowledged_native_publication_survives_abrupt_process_termination() {
    // Cover both acknowledged logical publication and the same repository
    // after its loose payloads have been replaced by a durable physical pack.
    struct Case {
        name: &'static str,
        mode: &'static str,
        acknowledgement: &'static str,
    }
    for case in [
        Case {
            name: "loose published objects",
            mode: "after-ack",
            acknowledgement: "DURABLE_ACK",
        },
        Case {
            name: "packed published objects",
            mode: "after-pack",
            acknowledgement: "PACK_DURABLE_ACK",
        },
    ] {
        eprintln!("checking acknowledged crash recovery: {}", case.name);
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("store");
        let mut driver = Driver::start(&root, case.mode);
        driver.checkpoint("BASELINE_DURABLE");
        driver.checkpoint(case.acknowledgement);
        driver.kill_and_reap();
        let store = Store::open_existing(&root, Config::default()).unwrap();
        verify_initial(&store);
    }
}

#[test]
fn unacknowledged_partial_upload_is_not_indexed_after_abrupt_process_termination() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let mut driver = Driver::start(&root, "mid-upload");
    driver.checkpoint("BASELINE_DURABLE");
    driver.checkpoint("MID_UPLOAD");
    driver.kill_and_reap();
    let store = Store::open_existing(&root, Config::default()).unwrap();
    verify_initial(&store);
    let incomplete = tinytree_model::describe_file(common::UNACKNOWLEDGED_BYTES);
    assert!(
        store
            .descriptor(common::REPOSITORY, incomplete.key)
            .unwrap()
            .is_none()
    );
}

#[test]
#[cfg(unix)]
fn real_child_write_failure_poisoning_recovers_atomic_reference_and_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let mut driver = Driver::start(&root, "write-failure");
    driver.checkpoint("BASELINE_DURABLE");
    driver.checkpoint("REPLACEMENT_PREPARED");
    driver.checkpoint("WRITE_FAILED_POISONED");
    driver.kill_and_reap();
    let store = Store::open_existing(&root, Config::default()).unwrap();
    let initial = common::initial();
    let replacement = common::replacement();
    verify_fixture(&store, &initial);
    verify_fixture(&store, &replacement);
    let reference = store
        .references(common::REPOSITORY, &[common::REFERENCE.into()])
        .unwrap()
        .remove(0);
    let receipt = store
        .publication(common::REPOSITORY, "replacement")
        .unwrap();
    if reference.commit == Some(initial.head) {
        assert_eq!(reference.generation, 1);
        assert!(receipt.is_none());
    } else {
        assert_eq!(reference.commit, Some(replacement.head));
        assert_eq!(reference.generation, 2);
        let receipt = receipt.expect("new reference must have its atomic receipt");
        assert_eq!(receipt.reference, reference);
        assert_eq!(receipt.idempotency_key, "replacement");
    }
    let original_receipt = store
        .publication(common::REPOSITORY, "initial")
        .unwrap()
        .unwrap();
    assert_eq!(original_receipt.reference.generation, 1);
    assert_eq!(original_receipt.reference.commit, Some(initial.head));
}

#[test]
#[cfg(unix)]
fn real_private_hold_write_failure_recovers_atomic_accounting_and_reserved_release() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let mut driver = Driver::start(&root, "retention-write-failure");
    driver.checkpoint("BASELINE_DURABLE");
    driver.checkpoint("REPLACEMENT_PREPARED");
    driver.checkpoint("WRITE_FAILED_POISONED");
    driver.kill_and_reap();
    let store = Store::open_existing(&root, Config::default()).unwrap();
    verify_initial(&store);
    let initial = common::initial();
    let owner = tinytree_store::RetentionOwner {
        principal: [1; 32],
        client: [2; 16],
    };
    let acquire = common::retention_publication("retention-attempt", 0, initial.head, true);
    let current = store
        .references(common::REPOSITORY, &[acquire.reference_name.clone()])
        .unwrap()
        .remove(0);
    let receipt = store
        .publication(common::REPOSITORY, "retention-attempt")
        .unwrap();
    if current.commit.is_some() {
        assert_eq!(current.generation, 1);
        assert_eq!(current.commit, Some(initial.head));
        assert_eq!(receipt.unwrap().reference, current);
    } else {
        assert_eq!(current.generation, 0);
        assert!(receipt.is_none());
    }
    let mut limits = tinytree_store::RetentionLimits {
        max_known_clients_per_repository: 1,
        max_known_clients_per_principal: 1,
        max_root_records_per_repository: 1,
        max_root_records_per_principal: 1,
        max_root_records_per_client: 1,
        max_live_roots_per_client: 1,
        max_receipts_per_repository: 2,
        max_receipts_per_principal: 2,
        max_receipts_per_client: 2,
        ..tinytree_store::RetentionLimits::default()
    };
    store
        .publish_retention(common::REPOSITORY, owner, &acquire, &limits, 100)
        .unwrap();
    store
        .publish_retention(
            common::REPOSITORY,
            owner,
            &common::retention_publication("retention-release", 1, initial.head, false),
            &limits,
            100,
        )
        .unwrap();
    limits.max_receipts_per_repository = 4;
    limits.max_receipts_per_principal = 4;
    limits.max_receipts_per_client = 4;
    // The generation tombstone and exactly one known namespace/root survive;
    // a stale live counter or partial first-admission counter would reject this.
    store
        .publish_retention(
            common::REPOSITORY,
            owner,
            &common::retention_publication("retention-again", 2, initial.head, true),
            &limits,
            100,
        )
        .unwrap();
    store
        .publish_retention(
            common::REPOSITORY,
            owner,
            &common::retention_publication("retention-release-again", 3, initial.head, false),
            &limits,
            100,
        )
        .unwrap();
    assert!(matches!(
        store.publish_retention(
            common::REPOSITORY,
            owner,
            &common::retention_publication("retention-budget-exhausted", 4, initial.head, true),
            &limits,
            100
        ),
        Err(tinytree_store::Error::Limit(_))
    ));
}

#[test]
fn acknowledged_landing_preparation_checks_and_atomic_receipt_survive_process_termination() {
    use tinytree_store::LandingState;
    for (mode, ack, state) in [
        (
            "landing-prepared",
            "LANDING_PREPARED_ACK",
            LandingState::Prepared,
        ),
        (
            "landing-checked",
            "LANDING_CHECKED_ACK",
            LandingState::Checked,
        ),
        (
            "landing-committed",
            "LANDING_COMMITTED_ACK",
            LandingState::Committed,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("store");
        let mut driver = Driver::start(&root, mode);
        driver.checkpoint("BASELINE_DURABLE");
        driver.checkpoint(ack);
        driver.kill_and_reap();
        let store = Store::open_existing(&root, Config::default()).unwrap();
        let job = store
            .landing_job(common::REPOSITORY, common::LANDING_OWNER, "landing")
            .unwrap()
            .unwrap();
        assert_eq!(job.state, state);
        let candidate = job.candidate.as_ref().unwrap();
        store.gc(common::REPOSITORY, 100000).unwrap();
        assert!(
            store
                .read(
                    common::REPOSITORY,
                    tinytree_model::ObjectKey {
                        kind: tinytree_model::Kind::Commit,
                        id: tinytree_model::ObjectId(candidate.head.0)
                    }
                )
                .is_ok()
        );
        if state == LandingState::Prepared {
            store
                .record_landing_check(
                    common::REPOSITORY,
                    common::LANDING_OWNER,
                    "landing",
                    &common::landing_identity(&job),
                    tinytree_store::LandingVerdict {
                        passed: true,
                        evidence: [8; 32],
                    },
                    101,
                )
                .unwrap();
        }
        let done = store
            .finalize_landing(common::REPOSITORY, common::LANDING_OWNER, "landing", 101)
            .unwrap();
        assert_eq!(done.state, LandingState::Committed);
        let receipt = done.receipt.as_ref().unwrap();
        assert_eq!(receipt.reference.generation, 2);
        assert_eq!(receipt.reference.commit.unwrap().key.id.0, candidate.head.0);
        assert_eq!(receipt.rewrites.len(), 1);
        assert_eq!(receipt.evidence, [8; 32]);
        assert_eq!(
            store
                .references(common::REPOSITORY, &[common::REFERENCE.into()])
                .unwrap()[0],
            receipt.reference
        );
        assert_eq!(
            store
                .begin_landing(
                    common::REPOSITORY,
                    common::LANDING_OWNER,
                    &common::landing_request(),
                    100000
                )
                .unwrap(),
            done
        );
    }
}

#[test]
#[cfg(unix)]
fn real_landing_journal_write_failure_recovers_reference_and_rewrite_receipt_atomically() {
    use tinytree_store::LandingState;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("store");
    let mut driver = Driver::start(&root, "landing-write-failure");
    driver.checkpoint("BASELINE_DURABLE");
    driver.checkpoint("LANDING_CHECKED_ACK");
    driver.checkpoint("WRITE_FAILED_POISONED");
    driver.kill_and_reap();
    let store = Store::open_existing(&root, Config::default()).unwrap();
    let job = store
        .landing_job(common::REPOSITORY, common::LANDING_OWNER, "landing")
        .unwrap()
        .unwrap();
    let reference = store
        .references(common::REPOSITORY, &[common::REFERENCE.into()])
        .unwrap()
        .remove(0);
    match job.state {
        LandingState::Checked => {
            assert_eq!(reference.generation, 1);
            assert_eq!(reference.commit, Some(common::initial().head));
            assert!(job.receipt.is_none());
        }
        LandingState::Committed => {
            let receipt = job.receipt.as_ref().unwrap();
            assert_eq!(reference, receipt.reference);
            assert_eq!(reference.generation, 2);
            assert_eq!(
                reference.commit.unwrap().key.id.0,
                job.candidate.as_ref().unwrap().head.0
            );
            assert_eq!(receipt.rewrites.len(), 1);
            assert_eq!(receipt.evidence, [8; 32]);
        }
        other => panic!("non-atomic recovered landing state: {other:?}"),
    }
}

#[test]
fn acknowledged_external_check_layout_survives_owned_process_termination() {
    #[cfg(unix)]
    let root = tempfile::Builder::new()
        .prefix("tinytree-layout-ack-")
        .tempdir_in("/tmp")
        .unwrap();
    #[cfg(not(unix))]
    let root = tempfile::tempdir().unwrap();
    let mut child = Driver::start(root.path(), "external-layout-ack");
    child.checkpoint("BASELINE_DURABLE");
    child.checkpoint("EXTERNAL_LAYOUT_ACK");
    child.kill_and_reap();
    let store = Store::open_existing(root.path(), Config::default()).unwrap();
    verify_initial(&store);
    assert_eq!(
        store.external_checks_directory().unwrap(),
        Some(root.path().join("external-checks"))
    );
}
#[cfg(unix)]
#[test]
fn external_check_layout_marker_failure_latches_and_recovers_complete_prefix() {
    let root = tempfile::Builder::new()
        .prefix("tinytree-layout-failure-")
        .tempdir_in("/tmp")
        .unwrap();
    let mut child = Driver::start(root.path(), "external-layout-write-failure");
    child.checkpoint("BASELINE_DURABLE");
    child.checkpoint("WRITE_FAILED_POISONED");
    child.kill_and_reap();
    let store = Store::open_existing(root.path(), Config::default()).unwrap();
    verify_initial(&store);
    assert!(root.path().join("external-checks/.header.json").is_file());
    let path = store.enable_external_checks().unwrap();
    assert_eq!(path, root.path().join("external-checks"));
    drop(store);
    assert_eq!(
        Store::open_existing(root.path(), Config::default())
            .unwrap()
            .external_checks_directory()
            .unwrap(),
        Some(path)
    );
}
