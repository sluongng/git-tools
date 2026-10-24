//! Per-process native command sessions. Dirty is durable before returning an
//! admitted token; process-exit drain never claims Clean state or cloud backup.
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
        sync::{Mutex, OnceLock},
        time::Instant,
    };
    struct Entry {
        binding: Binding,
        token: [u8; 32],
        lease: File,
    }
    static ENTRIES: OnceLock<Mutex<BTreeMap<PathBuf, Entry>>> = OnceLock::new();
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
    pub fn finish(deadline: Instant) {
        let entries = match ENTRIES.get_or_init(Default::default).lock() {
            Ok(mut entries) => std::mem::take(&mut *entries),
            Err(_) => {
                eprintln!("Warning: native command drain remains pending; workspace stays dirty.");
                return;
            }
        };
        let mut failed = false;
        for (_, entry) in entries {
            let left = deadline.saturating_duration_since(Instant::now());
            // Every jj helper has already been dropped by CliRunner::run. The
            // kernel lease may now release; EOF was never the completion proof.
            let _ = FileExt::unlock(&entry.lease);
            drop(entry.lease);
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
    }
}
#[cfg(not(feature = "protocol-fixture"))]
pub(crate) use managed::{ensure_binding, finish, token};
#[cfg(feature = "protocol-fixture")]
pub(crate) fn token(_: &std::path::Path) -> anyhow::Result<Option<[u8; 32]>> {
    Ok(None)
}
#[cfg(feature = "protocol-fixture")]
pub(crate) fn finish(_: std::time::Instant) {}
