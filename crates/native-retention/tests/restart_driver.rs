use futures::executor::block_on;
use native_retention::{
    BundleDescriptor, CaptureBundle, Limits, OpStoreId, Registry, RegistryId, RootCheckpoint,
    Tracking, WorkspaceId,
};
use std::io::{self, Write};
use tinytree_model::{Kind, ObjectId, ObjectKey};
fn main() {
    let mut arguments = std::env::args_os().skip(1);
    let path = arguments.next().expect("registry directory");
    let mode = arguments.next().expect("mode");
    let clean = mode == "clean";
    let retirement = mode == "retire";
    let capture = mode == "capture" || retirement;
    block_on(async {
        let registry = Registry::init(
            &path,
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
        let mut fingerprint = if capture {
            let bundle_path = std::path::PathBuf::from(&path).join("immutable-bundle");
            Some(
                registry
                    .capture_dirty(owner.token(), move || async move {
                        let bytes = b"immutable capture fixture";
                        let mut file = std::fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&bundle_path)
                            .unwrap();
                        file.write_all(bytes).unwrap();
                        file.sync_all().unwrap();
                        std::fs::File::open(bundle_path.parent().unwrap())
                            .unwrap()
                            .sync_all()
                            .unwrap();
                        Ok(CaptureBundle {
                            checkpoint: RootCheckpoint {
                                roots: vec![ObjectKey {
                                    kind: Kind::Commit,
                                    id: ObjectId([4; 32]),
                                }],
                                op_heads: vec![vec![5; 64]],
                                working_copy_operation: Some(vec![6; 64]),
                            },
                            bundle: BundleDescriptor {
                                blake3: *blake3::hash(bytes).as_bytes(),
                                size_bytes: bytes.len() as u64,
                            },
                        })
                    })
                    .await
                    .unwrap()
                    .fingerprint,
            )
        } else if clean {
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
        let (token, epoch, owner) = if retirement {
            let old = registry.pending_captures().await.unwrap().pop().unwrap();
            drop(owner);
            let owner = registry.begin_command(registration).await.unwrap();
            let token = owner.token();
            let epoch = owner.epoch();
            let path = std::path::PathBuf::from(&path);
            let successor = registry
                .capture_dirty(token, move || async move {
                    let bytes = b"distinct successor capture";
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path.join("successor-bundle"))
                        .unwrap();
                    file.write_all(bytes).unwrap();
                    file.sync_all().unwrap();
                    std::fs::File::open(&path).unwrap().sync_all().unwrap();
                    Ok(CaptureBundle {
                        checkpoint: RootCheckpoint {
                            roots: vec![ObjectKey {
                                kind: Kind::Commit,
                                id: ObjectId([4; 32]),
                            }],
                            op_heads: vec![vec![7; 64]],
                            working_copy_operation: Some(vec![8; 64]),
                        },
                        bundle: BundleDescriptor {
                            blake3: *blake3::hash(bytes).as_bytes(),
                            size_bytes: bytes.len() as u64,
                        },
                    })
                })
                .await
                .unwrap();
            fingerprint = Some(successor.fingerprint);
            drop(owner);
            registry
                .retire_redundant_capture(
                    old,
                    successor,
                    native_retention::CaptureUploadCleanup {
                        job: None,
                        published: Some(BundleDescriptor {
                            blake3: [9; 32],
                            size_bytes: 64,
                        }),
                    },
                )
                .await
                .unwrap();
            (token, epoch, None)
        } else {
            (owner.token(), owner.epoch(), Some(owner))
        };
        println!(
            "{}",
            serde_json::json!({"token": token.as_bytes(), "generation":registration.generation,"epoch":epoch,"fingerprint":fingerprint})
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
