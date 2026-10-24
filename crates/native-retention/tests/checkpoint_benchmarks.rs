//! Public-API component measurement, not a latency regression threshold.
use futures::executor::block_on;
use native_retention::{
    BundleDescriptor, CaptureBundle, Limits, OpStoreId, Registry, RegistryId, RootCheckpoint,
    Tracking, WorkspaceId, WorkspaceState,
};
use std::{io::Write, time::Instant};
use tinytree_model::{Kind, ObjectId, ObjectKey};

#[derive(Clone, Copy)]
enum Completion {
    Checkpoint,
    Capture,
}
impl Completion {
    fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "clean_checkpoint",
            Self::Capture => "dirty_capture_queue",
        }
    }
}

#[test]
fn stable_root_command_write_amplification() {
    block_on(async {
        for root_count in [100usize, 1000, 10000] {
            for completion in [Completion::Checkpoint, Completion::Capture] {
                let directory = tempfile::tempdir().unwrap();
                let registry = Registry::init(
                    directory.path().join("ledger"),
                    RegistryId([1; 16]),
                    Tracking::ManagedFresh,
                    Limits::default(),
                )
                .await
                .unwrap();
                let registration = registry
                    .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                    .await
                    .unwrap();
                let input = RootCheckpoint {
                    roots: (0..root_count)
                        .map(|index| {
                            let mut id = [0; 32];
                            id[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
                            ObjectKey {
                                kind: Kind::Commit,
                                id: ObjectId(id),
                            }
                        })
                        .collect(),
                    op_heads: vec![vec![5; 64]],
                    working_copy_operation: Some(vec![6; 64]),
                };
                let owner = registry.begin_command(registration).await.unwrap();
                registry
                    .checkpoint(owner.token(), input.clone())
                    .await
                    .unwrap();
                drop(owner);
                // The paired measurement isolates ledger work. It excludes capture
                // enumeration/export, bundle staging and network backup. Both modes
                // start from the same stable root claim and run on this worker.
                let bytes = [42u8; 64];
                let mut bundle_file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.path().join("bundle"))
                    .unwrap();
                bundle_file.write_all(&bytes).unwrap();
                bundle_file.sync_all().unwrap();
                std::fs::File::open(directory.path())
                    .unwrap()
                    .sync_all()
                    .unwrap();
                let bundle = BundleDescriptor {
                    blake3: *blake3::hash(&bytes).as_bytes(),
                    size_bytes: bytes.len() as u64,
                };
                let before = registry.storage_stats();
                let mut begin_us = 0u128;
                let mut completion_us = 0u128;
                const ITERATIONS: u64 = 10;
                for _ in 0..ITERATIONS {
                    let start = Instant::now();
                    let owner = registry.begin_command(registration).await.unwrap();
                    begin_us += start.elapsed().as_micros();
                    let start = Instant::now();
                    match completion {
                        Completion::Checkpoint => {
                            registry
                                .checkpoint(owner.token(), input.clone())
                                .await
                                .unwrap();
                        }
                        Completion::Capture => {
                            let checkpoint = input.clone();
                            registry
                                .capture_dirty(owner.token(), move || async move {
                                    Ok(CaptureBundle { checkpoint, bundle })
                                })
                                .await
                                .unwrap();
                        }
                    }
                    completion_us += start.elapsed().as_micros();
                    drop(owner);
                }
                let after = registry.storage_stats();
                assert_eq!(after.transactions - before.transactions, 2 * ITERATIONS);
                let pending = registry.pending_captures().await.unwrap();
                let expected_pending = match completion {
                    Completion::Checkpoint => 0,
                    Completion::Capture => ITERATIONS as usize,
                };
                assert_eq!(pending.len(), expected_pending);
                assert_eq!(
                    registry.workspace(registration).await.unwrap().state,
                    match completion {
                        Completion::Checkpoint => WorkspaceState::Clean,
                        Completion::Capture => WorkspaceState::Dirty,
                    }
                );
                println!(
                    "{}",
                    serde_json::json!({"mode":completion.label(),"roots":root_count,"iterations":ITERATIONS,"begin_us_total":begin_us,"completion_us_total":completion_us,"successful_transactions":after.transactions-before.transactions,"encoded_write_bytes":after.encoded_write_bytes-before.encoded_write_bytes,"pending_tickets":pending.len()})
                );
            }
        }
    });
}
