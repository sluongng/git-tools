use futures::executor::block_on;
use native_retention::{
    CommandToken, Error, Limits, OpStoreId, Registration, Registry, RegistryId, WorkspaceId,
    WorkspaceState,
};
use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
use tinytree_model::{Kind, ObjectId, ObjectKey};
struct OwnedChild(Child);
impl OwnedChild {
    fn terminate(&mut self) {
        self.0.kill().expect("terminate owned driver");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.0.try_wait().unwrap().is_some() {
                return;
            }
            assert!(Instant::now() < deadline, "driver did not terminate");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn driver() -> PathBuf {
    let path = PathBuf::from(env!("NATIVE_RETENTION_DRIVER"));
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir().unwrap().join(path)
    }
}
#[test]
fn killed_owner_preserves_only_syncall_acknowledged_checkpoints_and_invalidates_tokens() {
    for mode in ["dirty", "clean", "capture"] {
        let clean = mode == "clean";
        let directory = tempfile::tempdir().unwrap();
        let mut child = OwnedChild(
            Command::new(driver())
                .arg(directory.path())
                .arg(mode)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (send, receive) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let mut reader = BufReader::new(stdout);
            // The checkpoint receipt is bounded; never consume an unbounded line.
            let result = std::io::Read::take(&mut reader, 4096)
                .read_until(b'\n', &mut bytes)
                .map(|_| bytes);
            let _ = send.send(result);
        });
        let line = receive
            .recv_timeout(Duration::from_secs(20))
            .expect("driver receipt deadline")
            .unwrap();
        let acknowledgement: serde_json::Value = serde_json::from_slice(&line).unwrap();
        let token: [u8; 32] = serde_json::from_value(acknowledgement["token"].clone()).unwrap();
        let generation = acknowledgement["generation"].as_u64().unwrap();
        let expected_fingerprint: Option<[u8; 32]> =
            serde_json::from_value(acknowledgement["fingerprint"].clone()).unwrap();
        block_on(async {
            assert!(matches!(
                Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                    .await,
                Err(Error::Busy)
            ));
        });
        child.terminate();
        reader.join().unwrap();
        block_on(async {
            let registry =
                Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                    .await
                    .unwrap();
            let registration = Registration {
                workspace: WorkspaceId([2; 16]),
                op_store: OpStoreId([3; 16]),
                generation,
            };
            let info = registry.workspace(registration).await.unwrap();
            assert_eq!(info.command_epoch, 1);
            assert_eq!(info.fingerprint, expected_fingerprint);
            assert!(matches!(
                registry.resolve(CommandToken::from_bytes(token)),
                Err(Error::ClosedCommand)
            ));
            let pending = registry.pending_captures().await.unwrap();
            if mode == "capture" {
                assert_eq!(pending.len(), 1);
                assert_eq!(pending[0].fingerprint, expected_fingerprint.unwrap());
                assert_eq!(
                    pending[0].checkpoint.roots,
                    vec![ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId([4; 32])
                    }]
                );
            } else {
                assert!(pending.is_empty());
            }
            if clean {
                assert_eq!(info.state, WorkspaceState::Clean);
                let collector = registry.acquire_collector().await.unwrap();
                assert!(collector.roots().contains(&ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId([4; 32])
                }));
            } else {
                assert_eq!(info.state, WorkspaceState::Dirty);
                assert!(matches!(
                    registry.acquire_collector().await,
                    Err(Error::DirtyWorkspace)
                ));
            }
        });
    }
}
