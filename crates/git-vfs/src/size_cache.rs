//! Durable Fjall blob sizes; raw SHA1 keys and big-endian sizes remain stable.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use fjall::Readable;

use super::{CacheStats, canonical_oid};
use crate::storage::open_fjall;

pub(super) struct SizeCache {
    // Drop the keyspace before its shared database owner.
    database: fjall::SingleWriterTxKeyspace,
    owner: Arc<fjall::SingleWriterTxDatabase>,
}

impl SizeCache {
    pub(super) fn open(repository: &Path) -> Result<Self> {
        let owner = open_fjall(&repository.join("git-vfs-sizes.fjall"))?;
        let database = owner.keyspace("blob_sizes", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self { database, owner })
    }

    pub(super) fn get_many(&self, oids: &[String]) -> Result<HashMap<String, u64>> {
        let mut result = HashMap::new();
        let transaction = self.owner.read_tx();
        for oid in oids {
            let key = oid_key(oid)?;
            if let Some(value) = transaction.get(&self.database, key)? {
                result.insert(oid.clone(), decode_size(&value)?);
            }
        }
        Ok(result)
    }

    pub(super) fn persist(&mut self, sizes: &HashMap<String, u64>) -> Result<()> {
        let mut transaction = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        for (oid, size) in sizes {
            let key = oid_key(oid)?;
            ensure!(
                *size <= i64::MAX as u64,
                "blob size exceeds supported signed 64-bit range"
            );
            if let Some(existing) = transaction.get(&self.database, key)? {
                ensure!(
                    decode_size(&existing)? == *size,
                    "conflicting size for Git blob {oid}; blob-size cache was not updated"
                );
            } else {
                transaction.insert(&self.database, key, size.to_be_bytes());
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn stats(&self, repository: &Path) -> Result<CacheStats> {
        let mut entries = 0u64;
        let mut total_blob_bytes = 0u64;
        let transaction = self.owner.read_tx();
        for item in transaction.iter(&self.database) {
            let (key, value) = item.into_inner()?;
            ensure!(key.len() == 20, "invalid SHA1 key in blob-size cache");
            entries += 1;
            total_blob_bytes = total_blob_bytes
                .checked_add(decode_size(&value)?)
                .context("cached blob sizes exceed u64")?;
        }
        Ok(CacheStats {
            entries,
            total_blob_bytes,
            database_path: repository.join("git-vfs-sizes.fjall"),
        })
    }
}

fn oid_key(oid: &str) -> Result<[u8; 20]> {
    ensure!(
        canonical_oid(oid)? == oid,
        "cache keys must be canonical SHA1 IDs"
    );
    let mut key = [0u8; 20];
    hex::decode_to_slice(oid, &mut key)?;
    Ok(key)
}

fn decode_size(value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value
        .try_into()
        .context("invalid size encoding in blob-size cache")?;
    let size = u64::from_be_bytes(bytes);
    ensure!(
        size <= i64::MAX as u64,
        "invalid size range in blob-size cache"
    );
    Ok(size)
}
