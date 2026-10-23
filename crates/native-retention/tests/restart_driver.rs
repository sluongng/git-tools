use futures::executor::block_on;
use native_retention::{
    Limits, OpStoreId, Registry, RegistryId, RootCheckpoint, Tracking, WorkspaceId,
};
use std::io::{self, Write};
use tinytree_model::{Kind, ObjectId, ObjectKey};
fn main() {
    let mut arguments = std::env::args_os().skip(1);
    let path = arguments.next().expect("registry directory");
    let clean = arguments.next().expect("mode") == "clean";
    block_on(async {
        let registry = Registry::init(
            path,
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
        let owner = registry.begin_command(registration).await.unwrap();
        let fingerprint = if clean {
            Some(
                registry
                    .checkpoint(
                        owner.token(),
                        RootCheckpoint {
                            roots: vec![ObjectKey {
                                kind: Kind::Commit,
                                id: ObjectId([4; 32]),
                            }],
                            op_heads: vec![vec![5; 64]],
                            working_copy_operation: Some(vec![6; 64]),
                        },
                    )
                    .await
                    .unwrap()
                    .fingerprint,
            )
        } else {
            None
        };
        println!(
            "{}",
            serde_json::json!({"token": owner.token().as_bytes(), "generation":registration.generation,"epoch":owner.epoch(),"fingerprint":fingerprint})
        );
        io::stdout().flush().unwrap();
        // Parent terminates this owned process after the durable acknowledgement.
        // Keep all ownership objects alive until termination, not graceful drop.
        let mut line = String::new();
        loop {
            std::thread::park_timeout(std::time::Duration::from_secs(30));
            std::hint::black_box((&registry, &owner, &mut line));
        }
    });
}
