//! Public-API component measurement, not a latency regression threshold.
use futures::executor::block_on;
use native_retention::{
    Limits, OpStoreId, Registry, RegistryId, RootCheckpoint, Tracking, WorkspaceId,
};
use std::time::Instant;
use tinytree_model::{Kind, ObjectId, ObjectKey};
#[test]
fn stable_root_command_write_amplification() {
    block_on(async {
        for root_count in [100usize, 1000, 10000] {
            let directory = tempfile::tempdir().unwrap();
            let registry = Registry::init(
                directory.path(),
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
            let before = registry.storage_stats();
            let mut begin_us = 0u128;
            let mut checkpoint_us = 0u128;
            const ITERATIONS: u64 = 10;
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                let owner = registry.begin_command(registration).await.unwrap();
                begin_us += start.elapsed().as_micros();
                let start = Instant::now();
                registry
                    .checkpoint(owner.token(), input.clone())
                    .await
                    .unwrap();
                checkpoint_us += start.elapsed().as_micros();
                drop(owner);
            }
            let after = registry.storage_stats();
            assert_eq!(after.transactions - before.transactions, 2 * ITERATIONS);
            println!(
                "{}",
                serde_json::json!({"roots":root_count,"iterations":ITERATIONS,"begin_us_total":begin_us,"checkpoint_us_total":checkpoint_us,"successful_transactions":after.transactions-before.transactions,"encoded_write_bytes":after.encoded_write_bytes-before.encoded_write_bytes})
            );
        }
    });
}
