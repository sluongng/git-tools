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
    let capture_registry = registry.clone();
    let callback_registry = registry.clone();
    let pending = tokio::spawn(async move {
        capture_registry
            .capture_checkpoint(token, move || async move {
                assert!(matches!(
                    callback_registry.resolve(token),
                    Err(Error::ClosedCommand)
                ));
                let value = final_state.load(Ordering::Acquire);
                entered.send(value).unwrap();
                release_rx.await.unwrap();
                Ok(checkpoint(value))
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
