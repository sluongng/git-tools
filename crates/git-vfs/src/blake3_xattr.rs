//! Opt-in, cache-only content digests and per-mount consumption counters.
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

pub const XATTRS: &[u8] = b"user.gitsha1\0user.gitsha1.hex\0user.blake3\0user.blake3.hex\0";
#[derive(Default)]
pub struct Blake3XattrStats {
    base_hits: AtomicU64,
    local_hashes: AtomicU64,
    local_cache_hits: AtomicU64,
    misses: AtomicU64,
}
#[derive(Serialize)]
pub struct Blake3XattrReport {
    base_hits: u64,
    local_hashes: u64,
    local_cache_hits: u64,
    misses: u64,
}
impl Blake3XattrStats {
    pub fn report(&self) -> Blake3XattrReport {
        Blake3XattrReport {
            base_hits: self.base_hits.load(Ordering::Relaxed),
            local_hashes: self.local_hashes.load(Ordering::Relaxed),
            local_cache_hits: self.local_cache_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
    pub fn record_local_cache_hit(&self) {
        self.local_cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_local_hash(&self) {
        self.local_hashes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn local(&self, data: &[u8]) -> [u8; 32] {
        self.local_hashes.fetch_add(1, Ordering::Relaxed);
        *blake3::hash(data).as_bytes()
    }
    pub fn base(&self, repo: &crate::backend::GitRepo, oid: &str) -> anyhow::Result<Vec<u8>> {
        match repo.cached_blake3(oid)? {
            Some(hash) => {
                self.base_hits.fetch_add(1, Ordering::Relaxed);
                Ok(hash.to_vec())
            }
            None => {
                self.miss();
                Err(std::io::Error::from_raw_os_error(libc::ENODATA).into())
            }
        }
    }
    pub fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}
