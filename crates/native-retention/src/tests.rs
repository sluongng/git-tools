use super::*;
use futures::executor::block_on;
use std::{path::PathBuf, time::Duration};
fn checkpoint(seed: u8) -> RootCheckpoint {
    RootCheckpoint {
        roots: vec![
            ObjectKey {
                kind: Kind::Commit,
                id: tinytree_model::ObjectId([seed; 32]),
            },
            ObjectKey {
                kind: Kind::Commit,
                id: tinytree_model::ObjectId([0; 32]),
            },
        ],
        op_heads: vec![vec![seed; 64]],
        working_copy_operation: Some(vec![seed.wrapping_add(1); 64]),
    }
}
async fn registry() -> (tempfile::TempDir, Registry) {
    let directory = tempfile::tempdir().unwrap();
    let registry = Registry::init(
        directory.path(),
        RegistryId([1; 16]),
        Tracking::ManagedFresh,
        Limits::default(),
    )
    .await
    .unwrap();
    (directory, registry)
}
#[test]
fn durable_registration_checkpoint_and_restart_preserve_exact_identities_and_roots() {
    block_on(async {
        let (directory, registry) = registry().await;
        let registration = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        assert_eq!(
            registry
                .register(registration.workspace, registration.op_store)
                .await
                .unwrap(),
            registration
        );
        assert!(matches!(
            registry.acquire_collector().await,
            Err(Error::DirtyWorkspace)
        ));
        let owner = registry.begin_command(registration).await.unwrap();
        let token = owner.token();
        let mut input = checkpoint(4);
        input.roots.reverse();
        input.roots.push(input.roots[0]);
        input.op_heads.push(input.op_heads[0].clone());
        let receipt = registry.checkpoint(token, input).await.unwrap();
        assert_eq!(
            registry.checkpoint(token, checkpoint(4)).await.unwrap(),
            receipt
        );
        assert!(matches!(registry.resolve(token), Err(Error::ClosedCommand)));
        assert_eq!(
            registry.checkpoint(token, checkpoint(5)).await.unwrap_err(),
            Error::CheckpointConflict
        );
        drop(owner);
        let collector = registry.acquire_collector().await.unwrap();
        assert!(collector.roots().contains(&ObjectKey {
            kind: Kind::Commit,
            id: tinytree_model::ObjectId([0; 32])
        }));
        assert!(
            collector
                .roots()
                .contains(&tinytree_model::empty_tree().descriptor.key)
        );
        drop(collector);
        drop(registry);
        let registry =
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                .await
                .unwrap();
        let info = registry.workspace(registration).await.unwrap();
        assert_eq!(info.state, WorkspaceState::Clean);
        assert_eq!(info.command_epoch, 1);
        assert_eq!(info.fingerprint, Some(receipt.fingerprint));
        assert!(matches!(registry.resolve(token), Err(Error::ClosedCommand)));
        let collector = registry.acquire_collector().await.unwrap();
        assert!(collector.roots().contains(&ObjectKey {
            kind: Kind::Commit,
            id: tinytree_model::ObjectId([4; 32])
        }));
    });
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_freezes_new_jobs_drains_clones_and_keeps_collector_fenced() {
    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let job = registry.resolve(token).unwrap();
    let physical_job = job.clone();
    drop(job);
    assert!(matches!(
        registry.begin_command(registration).await,
        Err(Error::Busy)
    ));
    let pending_registry = registry.clone();
    let pending =
        tokio::spawn(async move { pending_registry.checkpoint(token, checkpoint(4)).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match registry.resolve(token) {
                Err(Error::ClosedCommand) => break,
                Ok(probe) => drop(probe),
                Err(other) => panic!("unexpected resolve: {other}"),
            };
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!pending.is_finished());
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Dirty
    );
    let collection_registry = registry.clone();
    let collection = tokio::spawn(async move { collection_registry.acquire_collector().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!collection.is_finished());
    drop(physical_job);
    pending.await.unwrap().unwrap();
    assert!(
        !collection.is_finished(),
        "clean owner still retains its read fence"
    );
    drop(owner);
    let collector = collection.await.unwrap().unwrap();
    drop(collector);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_drop_closes_token_but_draining_job_blocks_new_commands_and_collection() {
    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let job = registry.resolve(token).unwrap();
    drop(owner);
    assert!(matches!(registry.resolve(token), Err(Error::ClosedCommand)));
    assert!(matches!(
        registry.begin_command(registration).await,
        Err(Error::Busy)
    ));
    let collection_registry = registry.clone();
    let collection = tokio::spawn(async move { collection_registry.acquire_collector().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!collection.is_finished());
    drop(job);
    assert!(matches!(
        collection.await.unwrap(),
        Err(Error::DirtyWorkspace)
    ));
    let next = registry.begin_command(registration).await.unwrap();
    assert_eq!(next.epoch(), 2);
    drop(next);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_checkpoint_worker_keeps_roots_and_fence_until_durable_clean() {
    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let job = registry.resolve(token).unwrap();
    let checkpoint_registry = registry.clone();
    let request =
        tokio::spawn(async move { checkpoint_registry.checkpoint(token, checkpoint(6)).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(registry.resolve(token), Err(Error::ClosedCommand)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    let _ = request.await;
    drop(owner);
    drop(job);
    let collector = tokio::time::timeout(Duration::from_secs(2), registry.acquire_collector())
        .await
        .unwrap()
        .unwrap();
    assert!(collector.roots().contains(&ObjectKey {
        kind: Kind::Commit,
        id: tinytree_model::ObjectId([6; 32])
    }));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collector_serializes_registration_forget_and_no_implicit_untracking() {
    let (_directory, registry) = registry().await;
    let collector = registry.acquire_collector().await.unwrap();
    let registering = registry.clone();
    let queued = tokio::spawn(async move {
        registering
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!queued.is_finished());
    queued.abort();
    let _ = queued.await;
    drop(collector);
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    registry
        .checkpoint(owner.token(), checkpoint(4))
        .await
        .unwrap();
    drop(owner);
    let forgotten = registry.forget(registration).await.unwrap();
    assert!(forgotten.generation > registration.generation);
    assert_eq!(registry.forget(registration).await.unwrap(), forgotten);
    assert_eq!(
        registry.workspace(forgotten).await.unwrap().state,
        WorkspaceState::Forgotten
    );
    assert_eq!(
        registry
            .register(registration.workspace, registration.op_store)
            .await
            .unwrap_err(),
        Error::Forgotten
    );
    let collector = registry.acquire_collector().await.unwrap();
    assert_eq!(
        collector.roots(),
        &[tinytree_model::empty_tree().descriptor.key]
    );
    drop(collector);
    registry.pin_untracked_access().await.unwrap();
    assert!(matches!(
        registry.acquire_collector().await,
        Err(Error::UntrackedAccess)
    ));
    assert!(matches!(
        registry.try_acquire_collector().await,
        Err(Error::UntrackedAccess)
    ));
}
#[test]
fn quotas_reject_before_durable_clean_and_tombstones_keep_namespace_admission_bounded() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut limits = Limits::default();
        limits.max_workspaces = 1;
        limits.max_roots_per_workspace = 2;
        limits.max_jobs_per_command = 1;
        limits.max_op_heads_per_workspace = 1;
        let registry = Registry::init(
            directory.path(),
            RegistryId([1; 16]),
            Tracking::ManagedFresh,
            limits,
        )
        .await
        .unwrap();
        let reg = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let owner = registry.begin_command(reg).await.unwrap();
        let job = registry.resolve(owner.token()).unwrap();
        assert!(matches!(
            registry.resolve(owner.token()),
            Err(Error::Limit(_))
        ));
        drop(job);
        let mut too_many = checkpoint(4);
        too_many.roots.push(ObjectKey {
            kind: Kind::File,
            id: tinytree_model::ObjectId([8; 32]),
        });
        assert!(matches!(
            registry.checkpoint(owner.token(), too_many).await,
            Err(Error::Limit(_))
        ));
        assert_eq!(
            registry.workspace(reg).await.unwrap().state,
            WorkspaceState::Dirty
        );
        let mut invalid = checkpoint(4);
        invalid.op_heads = vec![vec![0; 129]];
        assert!(matches!(
            registry.checkpoint(owner.token(), invalid).await,
            Err(Error::Invalid(_))
        ));
        registry
            .checkpoint(owner.token(), checkpoint(4))
            .await
            .unwrap();
        drop(owner);
        registry.forget(reg).await.unwrap();
        assert!(matches!(
            registry
                .register(WorkspaceId([9; 16]), OpStoreId([9; 16]))
                .await,
            Err(Error::Limit(_))
        ));
    });
}
#[test]
fn process_owner_is_retained_by_command_jobs_and_identity_reopen_is_strict() {
    block_on(async {
        let (directory, registry) = registry().await;
        let reg = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let owner = registry.begin_command(reg).await.unwrap();
        let job = registry.resolve(owner.token()).unwrap();
        drop(owner);
        drop(registry);
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default()).await,
            Err(Error::Busy)
        ));
        drop(job);
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([9; 16]), Limits::default()).await,
            Err(Error::IdentityMismatch)
        ));
        let registry =
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                .await
                .unwrap();
        assert_eq!(
            registry.workspace(reg).await.unwrap().state,
            WorkspaceState::Dirty
        );
        assert_eq!(
            registry
                .register(reg.workspace, OpStoreId([4; 16]))
                .await
                .unwrap_err(),
            Error::IdentityMismatch
        );
    });
}
#[test]
fn unrelated_missing_and_final_symlink_registry_paths_are_not_initialized() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("missing");
        assert!(
            Registry::open_existing(&root, RegistryId([1; 16]), Limits::default())
                .await
                .is_err()
        );
        assert!(!root.exists());
        std::fs::write(directory.path().join("unrelated"), b"keep").unwrap();
        assert!(
            Registry::init(
                directory.path(),
                RegistryId([1; 16]),
                Tracking::ManagedFresh,
                Limits::default()
            )
            .await
            .is_err()
        );
        assert_eq!(
            std::fs::read(directory.path().join("unrelated")).unwrap(),
            b"keep"
        );
        #[cfg(unix)]
        {
            let target = directory.path().join("target");
            std::fs::create_dir(&target).unwrap();
            let link = directory.path().join("link");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            for path in [
                link.clone(),
                PathBuf::from(format!("{}/", link.display())),
                link.join("."),
            ] {
                assert!(
                    Registry::init(
                        path,
                        RegistryId([1; 16]),
                        Tracking::ManagedFresh,
                        Limits::default()
                    )
                    .await
                    .is_err()
                );
            }
            assert_eq!(std::fs::read_dir(target).unwrap().count(), 0);
        }
    });
}
#[test]
fn reopen_rechecks_logical_quotas_and_rejects_corrupt_format_without_repair() {
    block_on(async {
        let (directory, registry) = registry().await;
        let reg = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let owner = registry.begin_command(reg).await.unwrap();
        registry
            .checkpoint(owner.token(), checkpoint(4))
            .await
            .unwrap();
        drop(owner);
        drop(registry);
        let limits = Limits {
            max_total_roots: 1,
            ..Limits::default()
        };
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([1; 16]), limits).await,
            Err(Error::Limit(_))
        ));
        let limits = Limits {
            max_ledger_bytes: 1,
            ..Limits::default()
        };
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([1; 16]), limits).await,
            Err(Error::Limit(_))
        ));
        let registry =
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                .await
                .unwrap();
        assert_eq!(
            registry.workspace(reg).await.unwrap().state,
            WorkspaceState::Clean
        );
        drop(registry);
        let marker = directory.path().join("registry.format");
        std::fs::write(&marker, b"unsupported\n").unwrap();
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default()).await,
            Err(Error::Corrupt(_))
        ));
        assert_eq!(std::fs::read(marker).unwrap(), b"unsupported\n");
    });
}

// Corruption fixtures deliberately bypass the registry after its owner closes.
// They leave its manifest untouched so reopen must prove the exact inventory.
fn raw_workspace_row(path: &std::path::Path, workspace: WorkspaceId) -> Option<Vec<u8>> {
    use fjall::Readable;
    let db = fjall::SingleWriterTxDatabase::builder(path.join("metadata"))
        .manual_journal_persist(true)
        .open()
        .unwrap();
    assert!(db.keyspace_exists("workspaces"));
    let rows = db
        .keyspace("workspaces", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let value = db
        .read_tx()
        .get(&rows, workspace.0)
        .unwrap()
        .map(|value| value.to_vec());
    value
}
fn corrupt_workspace_row(
    path: &std::path::Path,
    workspace: WorkspaceId,
    replacement: Option<&[u8]>,
) {
    let db = fjall::SingleWriterTxDatabase::builder(path.join("metadata"))
        .manual_journal_persist(true)
        .open()
        .unwrap();
    assert!(db.keyspace_exists("workspaces"));
    let rows = db
        .keyspace("workspaces", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
    if let Some(value) = replacement {
        tx.insert(&rows, workspace.0, value);
    } else {
        tx.remove(&rows, workspace.0);
    }
    tx.commit().unwrap();
}
#[test]
fn reopen_rejects_missing_acknowledged_workspace_rows_including_tombstones() {
    block_on(async {
        for (name, second, remove_tombstone) in [
            ("sole clean record", false, false),
            ("one of multiple clean records", true, false),
            ("forgotten registration tombstone", true, true),
        ] {
            let (directory, registry) = registry().await;
            let first = registry
                .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                .await
                .unwrap();
            let owner = registry.begin_command(first).await.unwrap();
            registry
                .checkpoint(owner.token(), checkpoint(4))
                .await
                .unwrap();
            drop(owner);
            let mut removed = first.workspace;
            if second {
                let other = registry
                    .register(WorkspaceId([7; 16]), OpStoreId([8; 16]))
                    .await
                    .unwrap();
                let owner = registry.begin_command(other).await.unwrap();
                registry
                    .checkpoint(owner.token(), checkpoint(9))
                    .await
                    .unwrap();
                drop(owner);
                if remove_tombstone {
                    registry.forget(other).await.unwrap();
                    removed = other.workspace;
                }
            }
            drop(registry);
            assert!(raw_workspace_row(directory.path(), removed).is_some());
            corrupt_workspace_row(directory.path(), removed, None);
            assert!(
                matches!(
                    Registry::open_existing(
                        directory.path(),
                        RegistryId([1; 16]),
                        Limits::default()
                    )
                    .await,
                    Err(Error::Corrupt(_))
                ),
                "{name}"
            );
            assert!(
                raw_workspace_row(directory.path(), removed).is_none(),
                "reopen must not repair {name}"
            );
        }
    });
}
#[test]
fn reopen_rejects_valid_old_clean_row_under_newer_manifest_without_repair() {
    block_on(async {
        let (directory, registry) = registry().await;
        let registration = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let owner = registry.begin_command(registration).await.unwrap();
        registry
            .checkpoint(owner.token(), checkpoint(4))
            .await
            .unwrap();
        drop(owner);
        drop(registry);
        let old_row = raw_workspace_row(directory.path(), registration.workspace).unwrap();
        let registry =
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                .await
                .unwrap();
        let owner = registry.begin_command(registration).await.unwrap();
        registry
            .checkpoint(owner.token(), checkpoint(5))
            .await
            .unwrap();
        drop(owner);
        drop(registry);
        assert_ne!(
            raw_workspace_row(directory.path(), registration.workspace).unwrap(),
            old_row
        );
        corrupt_workspace_row(directory.path(), registration.workspace, Some(&old_row));
        assert!(matches!(
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default()).await,
            Err(Error::Corrupt(_))
        ));
        assert_eq!(
            raw_workspace_row(directory.path(), registration.workspace).unwrap(),
            old_row
        );
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_reads_after_physical_jobs_and_survives_caller_cancellation() {
    struct CheckpointOwner {
        registry: Registry,
        dropped: Option<tokio::sync::oneshot::Sender<u64>>,
    }
    impl Drop for CheckpointOwner {
        fn drop(&mut self) {
            let _ = self
                .dropped
                .take()
                .unwrap()
                .send(self.registry.storage_stats().transactions);
        }
    }

    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let physical_job = registry.resolve(token).unwrap();
    let state = Arc::new(std::sync::atomic::AtomicU8::new(4));
    let final_state = state.clone();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let before_commit = registry.storage_stats().transactions;
    let (dropped, owner_dropped) = tokio::sync::oneshot::channel();
    let physical_owner = Arc::new(CheckpointOwner {
        registry: registry.clone(),
        dropped: Some(dropped),
    });
    let capture_registry = registry.clone();
    let callback_registry = registry.clone();
    let pending = tokio::spawn(async move {
        capture_registry
            .capture_checkpoint_owned(token, move || async move {
                assert!(matches!(
                    callback_registry.resolve(token),
                    Err(Error::ClosedCommand)
                ));
                let value = final_state.load(Ordering::Acquire);
                entered.send(value).unwrap();
                release_rx.await.unwrap();
                Ok(OwnedRootCheckpoint::new(checkpoint(value), physical_owner))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match registry.resolve(token) {
                Err(Error::ClosedCommand) => break,
                Ok(probe) => drop(probe),
                Err(other) => panic!("unexpected resolve: {other}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        registry
            .capture_checkpoint(token, || async { Ok(checkpoint(9)) })
            .await
            .unwrap_err(),
        Error::CheckpointConflict
    );
    assert!(!pending.is_finished());
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(owner);
    let collection_registry = registry.clone();
    let collection = tokio::spawn(async move { collection_registry.acquire_collector().await });
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Dirty
    );
    state.store(7, Ordering::Release);
    drop(physical_job);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap(),
        7,
        "capture must observe the completed physical mutation"
    );
    assert!(!collection.is_finished(), "capture still owns the fence");
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), owner_dropped)
            .await
            .unwrap()
            .unwrap(),
        before_commit + 1,
        "external capture ownership must survive callback return and SyncAll after cancellation"
    );
    let collector = tokio::time::timeout(Duration::from_secs(2), collection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(collector.roots().contains(&ObjectKey {
        kind: Kind::Commit,
        id: tinytree_model::ObjectId([7; 32]),
    }));
    drop(collector);
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Clean
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_capture_preserves_dirty_state_and_allows_a_new_command() {
    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    assert_eq!(
        registry
            .capture_checkpoint(owner.token(), || async { Err(Error::Storage) })
            .await
            .unwrap_err(),
        Error::Storage
    );
    assert!(matches!(
        registry.resolve(owner.token()),
        Err(Error::ClosedCommand)
    ));
    drop(owner);
    assert!(matches!(
        registry.acquire_collector().await,
        Err(Error::DirtyWorkspace)
    ));
    let owner = registry.begin_command(registration).await.unwrap();
    registry
        .capture_checkpoint(owner.token(), || async { Ok(checkpoint(8)) })
        .await
        .unwrap();
    drop(owner);
    registry.acquire_collector().await.unwrap();
}

#[tokio::test]
async fn dirty_drain_seals_physical_jobs_without_cleaning_or_losing_fence_on_cancel() {
    let (_directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let physical = registry.resolve(token).unwrap();
    let waiter_registry = registry.clone();
    let waiter = tokio::spawn(async move { waiter_registry.drain_dirty(token).await });
    loop {
        match registry.resolve(token) {
            Err(Error::ClosedCommand) => break,
            Ok(job) => drop(job),
            other => panic!("unexpected resolve: {other:?}"),
        }
        tokio::task::yield_now().await;
    }
    assert!(!waiter.is_finished());
    waiter.abort();
    assert!(matches!(
        registry.begin_command(registration).await,
        Err(Error::Busy)
    ));
    drop(physical);
    registry.drain_dirty(token).await.unwrap();
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Dirty
    );
    drop(owner);
    assert!(matches!(
        registry.acquire_collector().await,
        Err(Error::DirtyWorkspace)
    ));
    let next = registry.begin_command(registration).await.unwrap();
    assert_eq!(next.epoch(), 2);
}

fn staged_capture(directory: &std::path::Path, seed: u8) -> CaptureBundle {
    use std::io::Write;
    let bytes = vec![seed; 64];
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(format!("bundle-{seed}")))
        .unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    std::fs::File::open(directory).unwrap().sync_all().unwrap();
    CaptureBundle {
        checkpoint: checkpoint(seed),
        bundle: BundleDescriptor {
            blake3: *blake3::hash(&bytes).as_bytes(),
            size_bytes: bytes.len() as u64,
        },
    }
}

#[test]
fn dirty_captures_are_atomic_and_keep_independent_roots_through_newer_checkpoints_forget_and_reopen()
 {
    block_on(async {
        let (directory, registry) = registry().await;
        let registration = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let mut expected = Vec::new();
        for seed in [4, 5] {
            let owner = registry.begin_command(registration).await.unwrap();
            let path = directory.path().to_owned();
            let before = registry.storage_stats().transactions;
            let receipt = registry
                .capture_dirty(owner.token(), move || async move {
                    Ok(staged_capture(&path, seed))
                })
                .await
                .unwrap();
            assert_eq!(
                registry.storage_stats().transactions,
                before + 1,
                "root claims and ticket share one durable transaction"
            );
            assert_eq!(
                registry.workspace(registration).await.unwrap().state,
                WorkspaceState::Dirty
            );
            assert_eq!(
                registry
                    .capture_dirty(owner.token(), || async { Err(Error::Storage) })
                    .await
                    .unwrap_err(),
                Error::CheckpointConflict
            );
            assert!(matches!(
                registry.resolve(owner.token()),
                Err(Error::ClosedCommand)
            ));
            expected.push(receipt);
            drop(owner);
            assert!(matches!(
                registry.acquire_collector().await,
                Err(Error::DirtyWorkspace)
            ));
        }
        let owner = registry.begin_command(registration).await.unwrap();
        registry
            .checkpoint(owner.token(), checkpoint(9))
            .await
            .unwrap();
        drop(owner);
        let collector = registry.acquire_collector().await.unwrap();
        for seed in [4, 5, 9] {
            assert!(collector.roots().contains(&checkpoint(seed).roots[0]));
        }
        drop(collector);
        registry.forget(registration).await.unwrap();
        let collector = registry.acquire_collector().await.unwrap();
        for seed in [4, 5] {
            assert!(collector.roots().contains(&checkpoint(seed).roots[0]));
        }
        assert!(!collector.roots().contains(&checkpoint(9).roots[0]));
        drop(collector);
        expected.sort_by_key(|ticket| ticket.id);
        assert_eq!(registry.pending_captures().await.unwrap(), expected);
        let before = registry.storage_stats();
        assert!(matches!(
            registry.read_capture(CaptureId([0; 32])).await,
            Err(Error::UnknownCapture)
        ));
        let read = registry.read_capture(expected[0].id).await.unwrap();
        assert_eq!(
            read.pending(),
            &expected[0],
            "exact historical ticket survives forget and newer epochs"
        );
        let collecting = registry.clone();
        let blocked = storage::runtime().unwrap().spawn(async move {
            tokio::time::timeout(Duration::from_millis(20), collecting.acquire_collector()).await
        });
        assert!(
            blocked.await.unwrap().is_err(),
            "independent read guard blocks collection"
        );
        assert_eq!(
            registry.storage_stats(),
            before,
            "lookup and unknown ID do not mutate the ledger"
        );
        drop(registry);
        assert!(
            matches!(
                Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                    .await,
                Err(Error::Busy)
            ),
            "guard retains the process owner even without Registry handles"
        );
        drop(read);
        let reopened =
            Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
                .await
                .unwrap();
        assert_eq!(reopened.pending_captures().await.unwrap(), expected);
        let collector = reopened.acquire_collector().await.unwrap();
        for seed in [4, 5] {
            assert!(collector.roots().contains(&checkpoint(seed).roots[0]));
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dirty_capture_drains_physical_jobs_and_commits_after_the_caller_is_cancelled() {
    struct CommitOwner {
        registry: Registry,
        dropped: Option<tokio::sync::oneshot::Sender<u64>>,
    }
    impl Drop for CommitOwner {
        fn drop(&mut self) {
            let _ = self
                .dropped
                .take()
                .unwrap()
                .send(self.registry.storage_stats().transactions);
        }
    }
    let (directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let owner = registry.begin_command(registration).await.unwrap();
    let token = owner.token();
    let job = registry.resolve(token).unwrap();
    let (entered, arrival) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (dropped, owner_dropped) = tokio::sync::oneshot::channel();
    let before_commit = registry.storage_stats().transactions;
    let physical_owner = Arc::new(CommitOwner {
        registry: registry.clone(),
        dropped: Some(dropped),
    });
    let path = directory.path().to_owned();
    let captured_registry = registry.clone();
    let waiter = tokio::spawn(async move {
        captured_registry
            .capture_dirty_owned(token, move || async move {
                let bundle = staged_capture(&path, 7);
                entered.send(()).unwrap();
                released.await.unwrap();
                Ok(OwnedCaptureBundle::new(bundle, physical_owner))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match registry.resolve(token) {
                Err(Error::ClosedCommand) => break,
                Ok(lease) => drop(lease),
                Err(error) => panic!("{error}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!waiter.is_finished());
    waiter.abort();
    let _ = waiter.await;
    drop(owner);
    assert!(
        registry.pending_captures().await.unwrap().is_empty(),
        "capture waits for accepted jobs"
    );
    drop(job);
    tokio::time::timeout(Duration::from_secs(3), arrival)
        .await
        .unwrap()
        .unwrap();
    let collecting_registry = registry.clone();
    let collection = tokio::spawn(async move { collecting_registry.acquire_collector().await });
    tokio::task::yield_now().await;
    assert!(
        !collection.is_finished(),
        "accepted capture owns collector fence"
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), owner_dropped)
            .await
            .unwrap()
            .unwrap(),
        before_commit + 1,
        "physical capture ownership survives cancellation and callback return through SyncAll"
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), collection)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::DirtyWorkspace)
    ));
    let tickets = registry.pending_captures().await.unwrap();
    assert_eq!(tickets.len(), 1);
    let mut expected = checkpoint(7);
    expected.roots.reverse(); // Canonical intrinsic ROOT precedes nonzero IDs.
    assert_eq!(tickets[0].checkpoint, expected);
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Dirty
    );
    registry.forget(registration).await.unwrap();
    let before_read = registry.storage_stats();
    // Pause the actual bounded lookup on the existing registry mutex. No test
    // production hook or caller-only future stands in for physical work.
    let locked_inner = registry.inner.clone();
    let (locked, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let blocking = std::thread::spawn(move || {
        let _data = locked_inner.data.lock().unwrap();
        locked.send(()).unwrap();
        released.recv().unwrap();
    });
    arrived.await.unwrap();
    let reading = registry.clone();
    let ticket_id = tickets[0].id;
    let read = tokio::spawn(async move { reading.read_capture(ticket_id).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if registry.inner.fence.try_write().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    assert!(
        registry.inner.fence.try_write().is_err(),
        "cancelled lookup retains the read fence until the real blocking job finishes"
    );
    let collecting = registry.clone();
    let collection = tokio::spawn(async move { collecting.acquire_collector().await });
    release.send(()).unwrap();
    blocking.join().unwrap();
    let collector = tokio::time::timeout(Duration::from_secs(2), collection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(collector.roots().contains(&tickets[0].checkpoint.roots[1]));
    drop(collector);
    assert_eq!(registry.storage_stats(), before_read);
    assert_eq!(registry.pending_captures().await.unwrap(), tickets);
    drop(registry);
    let reopened =
        Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
            .await
            .unwrap();
    assert_eq!(reopened.pending_captures().await.unwrap(), tickets);
}

#[test]
fn redundant_capture_retirement_advances_beyond_capacity_and_replays_durable_cleanup_after_reopen()
{
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_pending_captures: 2,
            ..Limits::default()
        };
        let mut registry = Registry::init(
            directory.path(),
            RegistryId([1; 16]),
            Tracking::ManagedFresh,
            limits.clone(),
        )
        .await
        .unwrap();
        let registration = registry
            .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
            .await
            .unwrap();
        let mut previous: Option<PendingCapture> = None;
        for seed in 4..11 {
            let command = registry.begin_command(registration).await.unwrap();
            let path = directory.path().to_owned();
            let current = registry
                .capture_dirty(command.token(), move || async move {
                    let mut captured = staged_capture(&path, seed);
                    captured
                        .checkpoint
                        .roots
                        .extend((4..seed).map(|old| checkpoint(old).roots[0]));
                    Ok(captured)
                })
                .await
                .unwrap();
            drop(command);
            if let Some(old) = previous {
                let before = registry.workspace(registration).await.unwrap();
                let cleanup = registry
                    .retire_redundant_capture(old.clone(), current.clone(), cleanup_upload(seed))
                    .await
                    .unwrap();
                assert_eq!(
                    registry.workspace(registration).await.unwrap(),
                    before,
                    "retirement never changes Dirty state or current operation"
                );
                assert!(cleanup.remove_bundle);
                assert!(registry.capture_cleanup_pending().unwrap());
                assert_eq!(cleanup.retired_binding, old.binding());
                assert_eq!(cleanup.successor_binding, current.binding());
                assert_eq!(
                    registry.pending_captures().await.unwrap(),
                    vec![current.clone()]
                );
                assert_eq!(
                    registry
                        .retire_redundant_capture(
                            old.clone(),
                            current.clone(),
                            cleanup_upload(seed)
                        )
                        .await
                        .unwrap(),
                    cleanup,
                    "exact lost-reply replay returns the same fixed obligation"
                );
                assert!(matches!(
                    registry.read_capture(old.id).await,
                    Err(Error::UnknownCapture)
                ));
                assert!(
                    directory
                        .path()
                        .join(format!("bundle-{}", seed - 1))
                        .exists(),
                    "registry retirement never unlinks capsule or native data"
                );
                drop(registry);
                registry =
                    Registry::open_existing(directory.path(), RegistryId([1; 16]), limits.clone())
                        .await
                        .unwrap();
                assert_eq!(
                    registry.capture_cleanup().await.unwrap(),
                    Some(cleanup.clone())
                );
                assert!(registry.capture_cleanup_pending().unwrap());
                let guard = registry.try_capture_cleanup().await.unwrap().unwrap();
                assert_eq!(guard.obligation(), &cleanup);
                assert!(
                    matches!(registry.try_host_access(), Err(Error::Busy)),
                    "physical cleanup excludes writers/readers through fsync and ACK"
                );
                guard.acknowledge_capture_cleanup().await.unwrap();
                assert_eq!(registry.capture_cleanup().await.unwrap(), None);
                assert!(!registry.capture_cleanup_pending().unwrap());
                assert!(registry.try_capture_cleanup().await.unwrap().is_none());
                assert_eq!(
                    registry.pending_captures().await.unwrap(),
                    vec![current.clone()]
                );
            }
            previous = Some(current);
        }
        assert_eq!(
            registry.workspace(registration).await.unwrap().state,
            WorkspaceState::Dirty
        );
        assert!(matches!(
            registry.acquire_collector().await,
            Err(Error::DirtyWorkspace)
        ));
        drop(registry);
        let registry = Registry::open_existing(directory.path(), RegistryId([1; 16]), limits)
            .await
            .unwrap();
        assert_eq!(
            registry.pending_captures().await.unwrap(),
            vec![previous.unwrap()]
        );
        assert!(registry.capture_cleanup().await.unwrap().is_none());
    });
}

fn cleanup_upload(seed: u8) -> CaptureUploadCleanup {
    CaptureUploadCleanup {
        job: Some(BundleDescriptor {
            blake3: [seed; 32],
            size_bytes: 64,
        }),
        published: BundleDescriptor {
            blake3: [seed.wrapping_add(1); 32],
            size_bytes: 64,
        },
    }
}

#[test]
fn canonical_capture_coverage_proves_anchored_typed_paths_without_file_payloads() {
    use tinytree_model as m;
    let file = m::describe_file(b"selected file body stays outside the proof");
    let leaf = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "file".into(),
                value: m::TreeValue::File {
                    id: m::FileId(file.key.id.0),
                    executable: false,
                    size_bytes: file.size_bytes,
                    copy_id: None,
                },
            }],
        }),
        m::Limits::default(),
    )
    .unwrap();
    let tree = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "dir".into(),
                value: m::TreeValue::Tree(m::TreeId(leaf.descriptor.key.id.0)),
            }],
        }),
        m::Limits::default(),
    )
    .unwrap();
    let commit_for = |tree: ObjectKey| {
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([9; 16]);
        commit.root_tree.terms = vec![m::TreeId(tree.id.0)];
        m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap()
    };
    let commit = commit_for(tree.descriptor.key);
    let one_hop = commit_for(leaf.descriptor.key);
    let mut wrong_hash = commit.clone();
    wrong_hash.bytes[0] ^= 1;
    let mut wrong_kind = commit.clone();
    wrong_kind.descriptor.key.kind = Kind::Tree;
    struct Case {
        name: &'static str,
        roots: Vec<ObjectKey>,
        successor: Vec<ObjectKey>,
        proof: Vec<m::EncodedObject>,
        success: bool,
    }
    let many_edges = m::encode(
        &m::Object::Tree(m::Tree {
            entries: (0..100_000)
                .map(|i| m::TreeEntry {
                    name: format!("file-{i:06}"),
                    value: m::TreeValue::File {
                        id: m::FileId(file.key.id.0),
                        executable: false,
                        size_bytes: file.size_bytes,
                        copy_id: Some(m::CopyId([8; 32])),
                    },
                })
                .collect(),
        }),
        m::Limits::default(),
    )
    .unwrap();
    let cases = vec![
        Case {
            name: "old working-copy tree retained through historical commit",
            roots: vec![leaf.descriptor.key],
            successor: vec![one_hop.descriptor.key],
            proof: vec![one_hop],
            success: true,
        },
        Case {
            name: "multi-hop typed file edge with order-independent metadata",
            roots: vec![leaf.descriptor.key, file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![leaf.clone(), commit.clone(), tree.clone()],
            success: true,
        },
        Case {
            name: "wrong witness hash",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![wrong_hash],
            success: false,
        },
        Case {
            name: "wrong witness kind",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![wrong_kind],
            success: false,
        },
        Case {
            name: "unanchored but otherwise valid proof",
            roots: vec![file.key],
            successor: vec![m::empty_tree().descriptor.key],
            proof: vec![commit.clone(), tree.clone(), leaf.clone()],
            success: false,
        },
        Case {
            name: "missing intermediate metadata",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![commit.clone(), leaf.clone()],
            success: false,
        },
        Case {
            name: "same digest wrong typed destination",
            roots: vec![ObjectKey {
                kind: Kind::Symlink,
                id: file.key.id,
            }],
            successor: vec![commit.descriptor.key],
            proof: vec![commit.clone(), tree.clone(), leaf.clone()],
            success: false,
        },
        Case {
            name: "FILE payload is never metadata proof",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![m::EncodedObject {
                descriptor: file,
                bytes: b"selected file body stays outside the proof".to_vec(),
            }],
            success: false,
        },
        Case {
            name: "oversized raw proof",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![m::EncodedObject {
                descriptor: tree.descriptor,
                bytes: vec![0; MAX_CAPTURE_COVERAGE_BYTES + 1],
            }],
            success: false,
        },
        Case {
            name: "too many proof records",
            roots: vec![file.key],
            successor: vec![commit.descriptor.key],
            proof: vec![commit.clone(); MAX_CAPTURE_COVERAGE_OBJECTS + 1],
            success: false,
        },
        Case {
            name: "too many canonical graph edges",
            roots: vec![file.key],
            successor: vec![many_edges.descriptor.key],
            proof: vec![many_edges],
            success: false,
        },
    ];
    block_on(async {
        for case in cases {
            let (directory, registry) = registry().await;
            let registration = registry
                .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                .await
                .unwrap();
            let mut tickets = Vec::new();
            for (seed, roots) in [(4, case.roots), (5, case.successor)] {
                let command = registry.begin_command(registration).await.unwrap();
                let path = directory.path().to_owned();
                tickets.push(
                    registry
                        .capture_dirty(command.token(), move || async move {
                            let mut bundle = staged_capture(&path, seed);
                            bundle.checkpoint.roots = roots;
                            Ok(bundle)
                        })
                        .await
                        .unwrap(),
                );
                drop(command);
            }
            let result = registry
                .retire_redundant_capture_with_coverage(
                    tickets[0].clone(),
                    tickets[1].clone(),
                    cleanup_upload(1),
                    OwnedCaptureCoverage::new(case.proof, Arc::new(())),
                )
                .await;
            assert_eq!(result.is_ok(), case.success, "{}: {result:?}", case.name);
            if case.success {
                assert_eq!(
                    registry.pending_captures().await.unwrap(),
                    vec![tickets[1].clone()]
                );
                assert!(registry.capture_cleanup_pending().unwrap());
            } else {
                tickets.sort_by_key(|ticket| ticket.id);
                assert_eq!(registry.pending_captures().await.unwrap(), tickets);
                assert!(!registry.capture_cleanup_pending().unwrap());
            }
            assert_eq!(
                registry.workspace(registration).await.unwrap().state,
                WorkspaceState::Dirty
            );
        }
    });
}

#[tokio::test]
async fn capture_retirement_rejects_stale_uncovered_and_foreign_rows_and_preserves_shared_packs() {
    let (directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let mut tickets = Vec::new();
    for seed in [4, 5, 6] {
        let command = registry.begin_command(registration).await.unwrap();
        let path = directory.path().to_owned();
        let ticket = registry
            .capture_dirty(command.token(), move || async move {
                let mut captured = staged_capture(&path, seed);
                if seed == 6 {
                    captured.checkpoint.roots.push(checkpoint(4).roots[0]);
                }
                Ok(captured)
            })
            .await
            .unwrap();
        drop(command);
        tickets.push(ticket);
    }
    let old = tickets[0].clone();
    let successor = tickets[2].clone();
    let read = registry.read_capture(old.id).await.unwrap();
    assert!(matches!(
        registry
            .retire_redundant_capture(old.clone(), successor.clone(), cleanup_upload(1))
            .await,
        Err(Error::Busy)
    ));
    drop(read);
    assert!(
        matches!(
            registry
                .retire_redundant_capture(old.clone(), tickets[1].clone(), cleanup_upload(1))
                .await,
            Err(Error::Invalid(_))
        ),
        "a later timestamp is not root coverage"
    );
    assert!(
        matches!(
            registry
                .retire_redundant_capture(successor.clone(), old.clone(), cleanup_upload(1))
                .await,
            Err(Error::Invalid(_))
        ),
        "latest cannot be retired into an older capsule"
    );
    assert!(matches!(
        registry
            .retire_redundant_capture(old.clone(), old.clone(), cleanup_upload(1))
            .await,
        Err(Error::Invalid(_))
    ));
    let mut stale = successor.clone();
    stale.bundle.size_bytes += 1;
    assert_eq!(
        registry
            .retire_redundant_capture(old.clone(), stale, cleanup_upload(1))
            .await
            .unwrap_err(),
        Error::CheckpointConflict
    );
    let mut missing = old.clone();
    missing.id = CaptureId([0; 32]);
    assert_eq!(
        registry
            .retire_redundant_capture(missing, successor.clone(), cleanup_upload(1))
            .await
            .unwrap_err(),
        Error::UnknownCapture
    );
    let other = registry
        .register(WorkspaceId([7; 16]), OpStoreId([8; 16]))
        .await
        .unwrap();
    let command = registry.begin_command(other).await.unwrap();
    let shared_bundle = old.bundle;
    let shared = registry
        .capture_dirty(command.token(), move || async move {
            Ok(CaptureBundle {
                checkpoint: checkpoint(4),
                bundle: shared_bundle,
            })
        })
        .await
        .unwrap();
    drop(command);
    assert!(matches!(
        registry
            .retire_redundant_capture(old.clone(), shared.clone(), cleanup_upload(1))
            .await,
        Err(Error::Invalid(_))
    ));
    let cleanup = registry
        .retire_redundant_capture(old.clone(), successor.clone(), cleanup_upload(1))
        .await
        .unwrap();
    assert!(
        !cleanup.remove_bundle,
        "a shared digest is never a pack unlink permit"
    );
    assert!(
        matches!(
            registry
                .retire_redundant_capture(tickets[1].clone(), successor, cleanup_upload(1))
                .await,
            Err(Error::Busy)
        ),
        "fixed cleanup slot cannot accumulate obligations"
    );
    assert!(registry.read_capture(shared.id).await.is_ok());
    let guard = registry.try_capture_cleanup().await.unwrap().unwrap();
    assert_eq!(guard.obligation(), &cleanup);
    drop(guard);
    assert_eq!(registry.capture_cleanup().await.unwrap(), Some(cleanup));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_retirement_keeps_its_exclusive_fence_until_the_transaction_finishes() {
    struct CoverageOwner {
        registry: Registry,
        dropped: Option<tokio::sync::oneshot::Sender<u64>>,
    }
    impl Drop for CoverageOwner {
        fn drop(&mut self) {
            self.dropped
                .take()
                .unwrap()
                .send(self.registry.storage_stats().transactions)
                .unwrap();
        }
    }
    let (directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let mut tickets = Vec::new();
    for seed in [4, 5] {
        let command = registry.begin_command(registration).await.unwrap();
        let path = directory.path().to_owned();
        tickets.push(
            registry
                .capture_dirty(command.token(), move || async move {
                    let mut bundle = staged_capture(&path, seed);
                    bundle.checkpoint.roots.push(checkpoint(4).roots[0]);
                    bundle
                        .checkpoint
                        .roots
                        .push(tinytree_model::empty_tree().descriptor.key);
                    Ok(bundle)
                })
                .await
                .unwrap(),
        );
        drop(command);
    }
    let locked = registry.inner.clone();
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mutex_owner = std::thread::spawn(move || {
        let _data = locked.data.lock().unwrap();
        entered.send(()).unwrap();
        released.recv().unwrap();
    });
    arrived.await.unwrap();
    let retiring = registry.clone();
    assert_eq!(registry.capture_cleanup_pending(), Err(Error::Busy));
    let old = tickets[0].clone();
    let successor = tickets[1].clone();
    let before = registry.storage_stats().transactions;
    let (proof_dropped, dropped) = tokio::sync::oneshot::channel();
    let proof = OwnedCaptureCoverage::new(
        vec![tinytree_model::empty_tree()],
        Arc::new(CoverageOwner {
            registry: registry.clone(),
            dropped: Some(proof_dropped),
        }),
    );
    let waiter = tokio::spawn(async move {
        retiring
            .retire_redundant_capture_with_coverage(old, successor, cleanup_upload(1), proof)
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while registry.inner.fence.try_read().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(matches!(registry.try_host_access(), Err(Error::Busy)));
    let admission_registry = registry.clone();
    let mut admission = tokio::spawn(async move {
        admission_registry
            .acquire_outer_host_access_until(tokio::time::Instant::now() + Duration::from_secs(3))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut admission)
            .await
            .is_err(),
        "cancelled maintenance waiter must not clear the physical maintenance marker"
    );
    release.send(()).unwrap();
    mutex_owner.join().unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), dropped)
            .await
            .unwrap()
            .unwrap(),
        before + 1,
        "coverage payload admission survives canonical verification and actual SyncAll after cancellation"
    );
    drop(
        tokio::time::timeout(Duration::from_secs(1), admission)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    let cleanup = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(cleanup) = registry.capture_cleanup().await.unwrap() {
                break cleanup;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(cleanup.retired.id, tickets[0].id);
    assert_eq!(
        registry.pending_captures().await.unwrap(),
        vec![tickets[1].clone()]
    );
    assert_eq!(
        registry.workspace(registration).await.unwrap().state,
        WorkspaceState::Dirty
    );
    let guard = registry.try_capture_cleanup().await.unwrap().unwrap();
    let locked = registry.inner.clone();
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mutex_owner = std::thread::spawn(move || {
        let _data = locked.data.lock().unwrap();
        entered.send(()).unwrap();
        released.recv().unwrap();
    });
    arrived.await.unwrap();
    let (accepted, acceptance) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let mut ack = std::pin::pin!(guard.acknowledge_capture_cleanup());
        assert!(futures::poll!(&mut ack).is_pending());
        accepted.send(()).unwrap();
        ack.await
    });
    acceptance.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        matches!(registry.try_host_access(), Err(Error::Busy)),
        "ACK cancellation retains the real exclusive cleanup owner"
    );
    release.send(()).unwrap();
    mutex_owner.join().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while registry.capture_cleanup().await.unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn retired_capture_reopen_rejects_missing_or_rebound_cleanup_state() {
    use fjall::Readable;
    #[derive(Clone, Copy, Debug)]
    enum Corruption {
        MissingCleanup,
        MissingCommitment,
        ReboundUpload,
        MissingSuccessor,
        Downgrade,
    }
    block_on(async {
        for corruption in [
            Corruption::MissingCleanup,
            Corruption::MissingCommitment,
            Corruption::ReboundUpload,
            Corruption::MissingSuccessor,
            Corruption::Downgrade,
        ] {
            let (directory, registry) = registry().await;
            let registration = registry
                .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                .await
                .unwrap();
            let mut tickets = Vec::new();
            for seed in [4, 5] {
                let command = registry.begin_command(registration).await.unwrap();
                let path = directory.path().to_owned();
                tickets.push(
                    registry
                        .capture_dirty(command.token(), move || async move {
                            let mut bundle = staged_capture(&path, seed);
                            bundle.checkpoint.roots.push(checkpoint(4).roots[0]);
                            Ok(bundle)
                        })
                        .await
                        .unwrap(),
                );
                drop(command);
            }
            registry
                .retire_redundant_capture(tickets[0].clone(), tickets[1].clone(), cleanup_upload(1))
                .await
                .unwrap();
            drop(registry);
            {
                let db = fjall::SingleWriterTxDatabase::builder(directory.path().join("metadata"))
                    .manual_journal_persist(true)
                    .open()
                    .unwrap();
                let meta = db
                    .keyspace("meta", fjall::KeyspaceCreateOptions::default)
                    .unwrap();
                let captures = db
                    .keyspace("captures", fjall::KeyspaceCreateOptions::default)
                    .unwrap();
                let raw = db.read_tx().get(&meta, b"manifest").unwrap().unwrap();
                let mut manifest: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                assert_eq!(
                    manifest["version"], 3,
                    "pre-retirement readers explicitly reject this version"
                );
                let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
                match corruption {
                    Corruption::MissingCleanup => {
                        manifest.as_object_mut().unwrap().remove("cleanup");
                    }
                    Corruption::MissingCommitment => {
                        manifest
                            .as_object_mut()
                            .unwrap()
                            .remove("cleanup_commitment");
                    }
                    Corruption::ReboundUpload => {
                        manifest["cleanup"]["upload"]["published"]["size_bytes"] = 65.into();
                    }
                    Corruption::MissingSuccessor => tx.remove(&captures, tickets[1].id.0),
                    Corruption::Downgrade => {
                        manifest["version"] = 2.into();
                    }
                }
                tx.insert(&meta, b"manifest", serde_json::to_vec(&manifest).unwrap());
                tx.commit().unwrap();
            }
            assert!(
                matches!(
                    Registry::open_existing(
                        directory.path(),
                        RegistryId([1; 16]),
                        Limits::default()
                    )
                    .await,
                    Err(Error::Corrupt(_))
                ),
                "{corruption:?}"
            );
        }
    });
}

#[test]
fn dirty_capture_errors_and_aggregate_limits_never_clean_or_erase_queued_tickets() {
    block_on(async {
        for limits in [
            Limits {
                max_pending_captures: 1,
                ..Limits::default()
            },
            Limits {
                max_capture_roots: 2,
                ..Limits::default()
            },
            Limits {
                // Two typed roots and two 64-byte operation identities.
                max_capture_root_bytes: 2 * 33 + 2 * (4 + 64),
                ..Limits::default()
            },
            Limits {
                max_capture_bundle_bytes: 64,
                ..Limits::default()
            },
        ] {
            let directory = tempfile::tempdir().unwrap();
            let registry = Registry::init(
                directory.path(),
                RegistryId([1; 16]),
                Tracking::ManagedFresh,
                limits,
            )
            .await
            .unwrap();
            let registration = registry
                .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                .await
                .unwrap();
            let first = registry.begin_command(registration).await.unwrap();
            let path = directory.path().to_owned();
            let ticket = registry
                .capture_dirty(first.token(), move || async move {
                    Ok(staged_capture(&path, 4))
                })
                .await
                .unwrap();
            drop(first);
            let second = registry.begin_command(registration).await.unwrap();
            let path = directory.path().to_owned();
            assert!(matches!(
                registry
                    .capture_dirty(second.token(), move || async move {
                        Ok(staged_capture(&path, 5))
                    })
                    .await,
                Err(Error::Limit(_))
            ));
            assert_eq!(
                registry.workspace(registration).await.unwrap().state,
                WorkspaceState::Dirty
            );
            drop(second);
            assert_eq!(
                registry.pending_captures().await.unwrap(),
                vec![ticket.clone()]
            );
            let failed = registry.begin_command(registration).await.unwrap();
            assert_eq!(
                registry
                    .capture_dirty(failed.token(), || async { Err(Error::Storage) })
                    .await
                    .unwrap_err(),
                Error::Storage
            );
            drop(failed);
            let next = registry.begin_command(registration).await.unwrap();
            drop(next);
            assert_eq!(registry.pending_captures().await.unwrap(), vec![ticket]);
            assert!(matches!(
                registry.acquire_collector().await,
                Err(Error::DirtyWorkspace)
            ));
        }
    });
}

#[test]
fn reopen_rejects_missing_or_rebound_capture_inventory_and_old_readers_fail_closed() {
    use fjall::Readable;
    #[derive(Clone, Copy, Debug)]
    enum Corruption {
        MissingRow,
        ReboundBundle,
        MissingInventory,
    }
    block_on(async {
        for corrupt in [
            Corruption::MissingRow,
            Corruption::ReboundBundle,
            Corruption::MissingInventory,
        ] {
            let (directory, registry) = registry().await;
            let registration = registry
                .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
                .await
                .unwrap();
            let owner = registry.begin_command(registration).await.unwrap();
            let path = directory.path().to_owned();
            let ticket = registry
                .capture_dirty(owner.token(), move || async move {
                    Ok(staged_capture(&path, 4))
                })
                .await
                .unwrap();
            drop(owner);
            drop(registry);
            {
                let db = fjall::SingleWriterTxDatabase::builder(directory.path().join("metadata"))
                    .manual_journal_persist(true)
                    .open()
                    .unwrap();
                let captures = db
                    .keyspace("captures", fjall::KeyspaceCreateOptions::default)
                    .unwrap();
                let meta = db
                    .keyspace("meta", fjall::KeyspaceCreateOptions::default)
                    .unwrap();
                let manifest = db.read_tx().get(&meta, b"manifest").unwrap().unwrap();
                let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
                assert!(manifest.get("captures").is_some());
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct OldManifest {
                    #[serde(rename = "version")]
                    _version: u32,
                    #[serde(rename = "id")]
                    _id: RegistryId,
                    #[serde(rename = "epoch")]
                    _epoch: u64,
                    #[serde(rename = "untracked")]
                    _untracked: bool,
                    #[serde(rename = "workspace_count")]
                    _workspace_count: u64,
                    #[serde(rename = "workspace_commitment")]
                    _workspace_commitment: [u8; 32],
                }
                assert!(
                    serde_json::from_value::<OldManifest>(manifest.clone()).is_err(),
                    "old readers cannot silently omit ticket roots"
                );
                let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
                match corrupt {
                    Corruption::MissingRow => tx.remove(&captures, ticket.id.0),
                    Corruption::ReboundBundle => {
                        let bytes = db.read_tx().get(&captures, ticket.id.0).unwrap().unwrap();
                        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                        value["bundle"]["size_bytes"] = 63.into();
                        tx.insert(&captures, ticket.id.0, serde_json::to_vec(&value).unwrap());
                    }
                    Corruption::MissingInventory => {
                        let mut value = manifest;
                        value.as_object_mut().unwrap().remove("captures");
                        tx.insert(&meta, b"manifest", serde_json::to_vec(&value).unwrap());
                    }
                }
                tx.commit().unwrap();
            }
            assert!(
                matches!(
                    Registry::open_existing(
                        directory.path(),
                        RegistryId([1; 16]),
                        Limits::default()
                    )
                    .await,
                    Err(Error::Corrupt(_))
                ),
                "{corrupt:?}"
            );
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outer_host_admission_waits_only_for_actual_capture_maintenance_with_bounded_cancellation()
{
    let (directory, registry) = registry().await;
    let registration = registry
        .register(WorkspaceId([2; 16]), OpStoreId([3; 16]))
        .await
        .unwrap();
    let mut tickets = Vec::new();
    for seed in [4, 5] {
        let command = registry.begin_command(registration).await.unwrap();
        let path = directory.path().to_owned();
        tickets.push(
            registry
                .capture_dirty(command.token(), move || async move {
                    let mut bundle = staged_capture(&path, seed);
                    bundle.checkpoint.roots.push(checkpoint(4).roots[0]);
                    Ok(bundle)
                })
                .await
                .unwrap(),
        );
        drop(command);
    }
    registry
        .retire_redundant_capture(tickets[0].clone(), tickets[1].clone(), cleanup_upload(1))
        .await
        .unwrap();
    let maintenance = registry.try_capture_cleanup().await.unwrap().unwrap();
    assert!(
        matches!(registry.try_host_access(), Err(Error::Busy)),
        "nested acquisition remains nonqueued"
    );
    // Exercise the same async API from a thread with no caller Tokio runtime.
    let outside = registry.clone();
    let timeout = std::thread::spawn(move || {
        block_on(outside.acquire_outer_host_access_until(
            tokio::time::Instant::now() + Duration::from_millis(20),
        ))
    });
    assert!(matches!(
        timeout.join().unwrap(),
        Err(Error::DeadlineExceeded)
    ));
    let cancelled_registry = registry.clone();
    let cancelled = tokio::spawn(async move {
        cancelled_registry
            .acquire_outer_host_access_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::task::yield_now().await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    // Force maintenance to finish AFTER try_read fails but BEFORE admission
    // reads its marker. A completed Notify must not turn into transient Busy.
    let (probed, probe) = tokio::sync::oneshot::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    *registry.inner.after_host_busy.lock().unwrap() = Some(Box::new(move || {
        probed.send(()).unwrap();
        resumed.recv().unwrap();
    }));
    let transitioning = registry.clone();
    let transition = tokio::spawn(async move {
        transitioning
            .acquire_outer_host_access_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    probe.await.unwrap();
    drop(maintenance);
    resume.send(()).unwrap();
    drop(
        tokio::time::timeout(Duration::from_secs(1), transition)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    let maintenance = registry.try_capture_cleanup().await.unwrap().unwrap();
    let waiting_registry = registry.clone();
    let mut waiting = tokio::spawn(async move {
        waiting_registry
            .acquire_outer_host_access_until(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    assert!(
        matches!(registry.try_capture_cleanup().await, Err(Error::Busy)),
        "a failed concurrent maintenance attempt does not clear the live owner"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    maintenance.acknowledge_capture_cleanup().await.unwrap();
    let access = Arc::new(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
    assert_eq!(access.registry_id(), registry.id());
    registry
        .register_with_host_access(WorkspaceId([6; 16]), OpStoreId([7; 16]), access.clone())
        .await
        .unwrap();
    drop(access);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match registry.try_capture_cleanup().await {
                Ok(None) => break,
                Err(Error::Busy) => tokio::task::yield_now().await,
                other => panic!("unexpected cleanup admission: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let (_other_directory, other) = self::registry().await;
    let collector = other.try_acquire_collector().await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(100),
            other.acquire_outer_host_access_until(
                tokio::time::Instant::now() + Duration::from_secs(5)
            )
        )
        .await
        .unwrap()
        .unwrap_err(),
        Error::Busy,
        "a genuine collector is not capture maintenance and is not retried"
    );
    drop(collector);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_access_scoped_registration_does_not_queue_behind_its_own_collector() {
    let (directory, registry) = registry().await;
    let host = Arc::new(registry.try_host_access().unwrap());
    assert_eq!(host.registry_id(), registry.id());
    assert!(matches!(
        registry.try_acquire_collector().await,
        Err(Error::Busy)
    ));
    // A failed try must not queue an exclusive writer ahead of nested host I/O.
    drop(registry.try_host_access().unwrap());
    drop(host);
    let collector = registry.try_acquire_collector().await.unwrap();
    assert!(
        collector
            .roots()
            .contains(&tinytree_model::empty_tree().descriptor.key)
    );
    assert!(matches!(registry.try_host_access(), Err(Error::Busy)));
    assert!(matches!(
        registry.try_acquire_collector().await,
        Err(Error::Busy)
    ));
    drop(collector);
    let host = Arc::new(registry.try_host_access().unwrap());
    let (_other_directory, other) = self::registry().await;
    let before = other.storage_stats();
    assert!(matches!(
        other
            .register_with_host_access(WorkspaceId([2; 16]), OpStoreId([3; 16]), host.clone())
            .await,
        Err(Error::IdentityMismatch)
    ));
    assert_eq!(other.storage_stats(), before);
    let collecting = registry.clone();
    let collection = tokio::spawn(async move { collecting.acquire_collector().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while registry.try_host_access().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let registered = tokio::time::timeout(
        Duration::from_secs(2),
        registry.register_with_host_access(WorkspaceId([2; 16]), OpStoreId([3; 16]), host.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        registry.workspace(registered).await.unwrap().state,
        WorkspaceState::Dirty
    );
    assert!(!collection.is_finished());
    drop(host);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), collection)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::DirtyWorkspace)
    ));
    assert!(matches!(
        registry.try_acquire_collector().await,
        Err(Error::DirtyWorkspace)
    ));
    let host = registry.try_host_access().unwrap();
    drop(registry);
    assert!(matches!(
        Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default()).await,
        Err(Error::Busy)
    ));
    drop(host);
    let reopened =
        Registry::open_existing(directory.path(), RegistryId([1; 16]), Limits::default())
            .await
            .unwrap();
    assert_eq!(
        reopened.workspace(registered).await.unwrap().state,
        WorkspaceState::Dirty
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_dirty_invalidation_preserves_claim_and_refuses_live_command_owners() {
    let (_directory, registry) = registry().await;
    let host = Arc::new(registry.try_host_access().unwrap());
    let registration = registry
        .register_with_host_access(WorkspaceId([2; 16]), OpStoreId([3; 16]), host.clone())
        .await
        .unwrap();
    let command = registry.begin_command(registration).await.unwrap();
    registry
        .checkpoint(command.token(), checkpoint(4))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .mark_dirty_with_host_access(registration, host.clone())
            .await,
        Err(Error::Busy)
    ));
    drop(command);
    let clean = registry.workspace(registration).await.unwrap();
    let before = registry.storage_stats();
    assert_eq!(
        registry
            .register_with_host_access(registration.workspace, registration.op_store, host.clone())
            .await
            .unwrap(),
        registration
    );
    assert_eq!(
        registry.storage_stats(),
        before,
        "idempotent registration must not silently invalidate a checkpoint"
    );
    registry
        .mark_dirty_with_host_access(registration, host.clone())
        .await
        .unwrap();
    let dirty = registry.workspace(registration).await.unwrap();
    assert_eq!(dirty.state, WorkspaceState::Dirty);
    assert_eq!(dirty.command_epoch, clean.command_epoch);
    assert_eq!(
        dirty.fingerprint, clean.fingerprint,
        "host invalidation preserves the old root claim"
    );
    let before = registry.storage_stats();
    registry
        .mark_dirty_with_host_access(registration, host.clone())
        .await
        .unwrap();
    assert_eq!(
        registry.storage_stats(),
        before,
        "already Dirty remains an idempotent safe state"
    );
    drop(host);
    assert!(matches!(
        registry.acquire_collector().await,
        Err(Error::DirtyWorkspace)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_host_registration_retains_its_fence_until_the_durable_dirty_write() {
    let (_directory, registry) = registry().await;
    let access = Arc::new(registry.try_host_access().unwrap());
    let inner = registry.inner.clone();
    let (locked, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let blocking = std::thread::spawn(move || {
        let _data = inner.data.lock().unwrap();
        locked.send(()).unwrap();
        released.recv().unwrap();
    });
    arrived.await.unwrap();
    let weak = Arc::downgrade(&access);
    {
        let mut registration = Box::pin(registry.register_with_host_access(
            WorkspaceId([2; 16]),
            OpStoreId([3; 16]),
            access,
        ));
        // Poll through actual blocking-job acceptance. The held ledger mutex
        // prevents completion, so dropping this Pending future cancels only its
        // waiter, not an arbitrary task that might never have started.
        assert!(futures::poll!(registration.as_mut()).is_pending());
    }
    assert!(
        weak.upgrade().is_some(),
        "cancelled registration keeps physical access ownership"
    );
    let collecting = registry.clone();
    let collection = tokio::spawn(async move { collecting.acquire_collector().await });
    tokio::task::yield_now().await;
    assert!(!collection.is_finished());
    release.send(()).unwrap();
    blocking.join().unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), collection)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::DirtyWorkspace)
    ));
}
