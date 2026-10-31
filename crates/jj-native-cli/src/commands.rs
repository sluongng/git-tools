//! Per-process native command sessions. Dirty is durable before returning an
//! admitted token; process-exit drain never claims Clean state or cloud backup.
pub(crate) enum FinishOutcome {
    Ordinary,
    PrivateBackup {
        machine: bool,
        result: Result<native_vfs_rpc::PrivateBackupReceipt, String>,
    },
}

#[cfg(not(feature = "protocol-fixture"))]
mod managed {
    use anyhow::{Context, Result, ensure};
    use fs2::FileExt;
    use native_vfs_rpc::CommandClient;
    use native_workspace_admin::Binding;
    use private_fs::PrivateDir;
    use std::{
        collections::BTreeMap,
        fs::File,
        path::{Path, PathBuf},
        sync::{
            Mutex, OnceLock,
            atomic::{AtomicU8, Ordering},
        },
        time::Instant,
    };
    #[derive(Clone, PartialEq, Eq)]
    struct BackupSelection {
        machine: bool,
        commit: String,
        operation: String,
    }
    struct Entry {
        binding: Binding,
        token: [u8; 32],
        lease: File,
        backup: Option<BackupSelection>,
    }
    static ENTRIES: OnceLock<Mutex<BTreeMap<PathBuf, Entry>>> = OnceLock::new();
    static BACKUP_MODE: AtomicU8 = AtomicU8::new(0);
    fn backup_failure() -> super::FinishOutcome {
        super::FinishOutcome::PrivateBackup {
            machine: BACKUP_MODE.load(Ordering::SeqCst) == 2,
            result: Err(
                "private backup acknowledgement unavailable; source and pending state retained"
                    .into(),
            ),
        }
    }
    pub fn ensure_binding(binding: &Binding) -> Result<[u8; 32]> {
        let mut entries = ENTRIES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| anyhow::anyhow!("native command registry unavailable"))?;
        if let Some(entry) = entries.get(&binding.repo) {
            ensure!(entry.binding == *binding, "native command binding changed");
            return Ok(entry.token);
        }
        ensure!(
            entries.len() < 64,
            "native command workspace limit exceeded"
        );
        let path = native_vfs_rpc::command_lock_path(&binding.admin, &binding.rpc_mount_id)?;
        let directory = PrivateDir::open_existing(
            path.parent()
                .context("native command lease has no parent")?,
        )?;
        ensure!(
            path.try_exists()?,
            "native command lease is not provisioned"
        );
        let lease = directory.open_lock(
            path.file_name()
                .context("native command lease has no name")?,
        )?;
        let client = CommandClient::new(
            binding.endpoint.local(),
            binding.rpc_mount_id.clone(),
            Default::default(),
        )?;
        let mut replay = [0; 32];
        replay[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        replay[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let preparation = futures::executor::block_on(client.prepare(replay))?;
        lease
            .try_lock_exclusive()
            .context("another native command still owns this workspace")?;
        let started = futures::executor::block_on(client.begin(replay, preparation))?;
        entries.insert(
            binding.repo.clone(),
            Entry {
                binding: binding.clone(),
                token: started.token,
                lease,
                backup: None,
            },
        );
        Ok(started.token)
    }
    pub fn token(repo: &Path) -> Result<Option<[u8; 32]>> {
        let entries = ENTRIES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| anyhow::anyhow!("native command registry unavailable"))?;
        Ok(Some(
            entries
                .get(repo)
                .context("native repository load has no admitted command fence")?
                .token,
        ))
    }
    pub fn request_private_backup(
        repo: &Path,
        machine: bool,
        commit: String,
        operation: String,
    ) -> Result<()> {
        let mut entries = ENTRIES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| anyhow::anyhow!("native command registry unavailable"))?;
        ensure!(
            !entries
                .iter()
                .any(|(path, entry)| path != repo && entry.backup.is_some()),
            "one explicit backup per command is supported"
        );
        let entry = entries
            .get_mut(repo)
            .context("private backup requires an admitted native command")?;
        let selection = BackupSelection {
            machine,
            commit,
            operation,
        };
        if let Some(saved) = &entry.backup {
            ensure!(
                saved == &selection,
                "private backup finish mode cannot change"
            );
            return Ok(());
        }
        let client = CommandClient::new(
            entry.binding.endpoint.local(),
            entry.binding.rpc_mount_id.clone(),
            Default::default(),
        )?;
        // Mark the server's immutable finish mode while the writer lease is
        // STILL HELD. Its watcher must never race into ordinary dirty finish.
        futures::executor::block_on(client.select_private_backup(entry.token))?;
        BACKUP_MODE.store(if machine { 2 } else { 1 }, Ordering::SeqCst);
        entry.backup = Some(selection);
        Ok(())
    }
    pub fn finish(deadline: Instant, successful: bool) -> super::FinishOutcome {
        let entries = match ENTRIES.get_or_init(Default::default).lock() {
            Ok(mut entries) => std::mem::take(&mut *entries),
            Err(_) => {
                eprintln!("Warning: native command drain remains pending; workspace stays dirty.");
                return if successful && BACKUP_MODE.load(Ordering::SeqCst) != 0 {
                    backup_failure()
                } else {
                    super::FinishOutcome::Ordinary
                };
            }
        };
        let mut failed = false;
        let mut outcome = super::FinishOutcome::Ordinary;
        for (_, entry) in entries {
            // CliRunner has dropped every helper. Release only now, after the
            // server has acknowledged any explicit mode selected above.
            let _ = FileExt::unlock(&entry.lease);
            drop(entry.lease);
            if let Some(selected) = entry.backup.filter(|_| successful) {
                let mut limits = native_vfs_rpc::Limits::default();
                limits.operation_timeout = std::time::Duration::from_secs(300);
                let result = CommandClient::new(entry.binding.endpoint.local(), entry.binding.rpc_mount_id, limits)
                    .and_then(|client| futures::executor::block_on(client.finish_private_backup(entry.token)))
                    .and_then(|receipt| {
                        receipt.validate()?;
                        ensure!(receipt.snapshot_commit == selected.commit && receipt.snapshot_operation == selected.operation,
                            "private backup receipt differs from snapshotted working copy: expected commit={} operation={}; received commit={} operation={}",
                            selected.commit, selected.operation, receipt.snapshot_commit, receipt.snapshot_operation);
                        Ok(receipt)
                    })
                    .map_err(|error| {
                        // Detailed diagnostics stay in the admitted owner's private
                        // working-copy directory, never machine output or stderr.
                        let mut diagnostic = format!("{error:#}");
                        let mut end = diagnostic.len().min(16 * 1024);
                        while !diagnostic.is_char_boundary(end) { end -= 1; }
                        diagnostic.truncate(end);
                        if let Ok(directory) = PrivateDir::open_existing(&entry.binding.working_copy) {
                            let _ = directory.replace("last-private-backup-error.txt", diagnostic.as_bytes());
                        }
                        "private backup did not reach an exact durable capsule acknowledgement; source and pending state retained".to_owned()
                    });
                outcome = super::FinishOutcome::PrivateBackup {
                    machine: selected.machine,
                    result,
                };
                continue;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                failed = true;
                continue;
            }
            let mut limits = native_vfs_rpc::Limits::default();
            limits.operation_timeout = left.min(limits.operation_timeout);
            let result = CommandClient::new(
                entry.binding.endpoint.local(),
                entry.binding.rpc_mount_id,
                limits,
            )
            .and_then(|client| futures::executor::block_on(client.finish_dirty(entry.token)));
            failed |= result.is_err();
        }
        if failed {
            eprintln!("Warning: native command drain remains pending; workspace stays dirty.");
        }
        if successful
            && BACKUP_MODE.load(Ordering::SeqCst) != 0
            && matches!(outcome, super::FinishOutcome::Ordinary)
        {
            return backup_failure();
        }
        outcome
    }
}
#[cfg(not(feature = "protocol-fixture"))]
pub(crate) use managed::{ensure_binding, finish, request_private_backup, token};
#[cfg(feature = "protocol-fixture")]
pub(crate) fn token(_: &std::path::Path) -> anyhow::Result<Option<[u8; 32]>> {
    Ok(None)
}
#[cfg(feature = "protocol-fixture")]
pub(crate) fn finish(_: std::time::Instant, _: bool) -> FinishOutcome {
    FinishOutcome::Ordinary
}
#[cfg(feature = "protocol-fixture")]
pub(crate) fn request_private_backup(
    _: &std::path::Path,
    _: bool,
    _: String,
    _: String,
) -> anyhow::Result<()> {
    anyhow::bail!("private backup requires a native lazy workspace")
}
