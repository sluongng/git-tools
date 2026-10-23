use tonic::Status;
#[derive(Clone, Debug)]
pub struct Config {
    pub max_frame_bytes: usize,
    pub max_metadata_bytes: usize,
    /// Encoded request/reply bound for immutable local queries.
    pub max_query_bytes: usize,
    /// Aggregate declared persistent query/SDK pools across distinct adapters.
    pub max_query_persistent_bytes: u64,
    pub max_file_bytes: u64,
    pub max_catalog_bytes: usize,
    pub max_catalog_objects: usize,
    pub max_catalog_edges: usize,
    pub max_inflight: usize,
    pub max_memory_bytes: usize,
    /// Client verified-spool bytes and server transient write staging across
    /// all configured stores. Permanent objects and external CAS are separate.
    pub max_temporary_bytes: u64,
    pub policy: reapi::policy::Policy,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_bytes: 64 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_query_bytes: 512 * 1024,
            max_query_persistent_bytes: 512 * 1024 * 1024,
            max_file_bytes: 256 * 1024 * 1024,
            max_catalog_bytes: 64 * 1024 * 1024,
            max_catalog_objects: 100_000,
            max_catalog_edges: 1_000_000,
            max_inflight: 16,
            max_memory_bytes: 512 * 1024 * 1024,
            max_temporary_bytes: 1024 * 1024 * 1024,
            policy: Default::default(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Status> {
        self.policy
            .validate()
            .map_err(|_| Status::invalid_argument("invalid broker policy"))?;
        if !(1024..=1024 * 1024).contains(&self.max_frame_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_metadata_bytes)
            || !(1024..=8 * 1024 * 1024).contains(&self.max_query_bytes)
            || self.max_query_persistent_bytes > 16 * 1024 * 1024 * 1024
            || !(1..=1024 * 1024 * 1024).contains(&self.max_file_bytes)
            || !(1..=256 * 1024 * 1024).contains(&self.max_catalog_bytes)
            || !(1..=100_000).contains(&self.max_catalog_objects)
            || !(1..=1_000_000).contains(&self.max_catalog_edges)
            || !(1..=1024).contains(&self.max_inflight)
            || self.max_memory_bytes > 1024 * 1024 * 1024
            || self.max_temporary_bytes > u32::MAX as u64
            || self
                .max_file_bytes
                .checked_mul(2)
                .is_none_or(|bytes| self.max_temporary_bytes < bytes)
            || self.max_temporary_bytes < self.max_metadata_bytes as u64
            || self.max_memory_bytes < self.metadata_memory()
            || self.max_memory_bytes < self.catalog_memory()
            || self.max_memory_bytes < self.file_memory()
        {
            return Err(Status::invalid_argument("invalid broker limits"));
        }
        Ok(())
    }
    pub(crate) fn query_memory(&self) -> usize {
        // Decoded nested protobuf nodes/empty rows can be much larger than
        // their wire representation. Include native/wire copies and encoding.
        self.max_query_bytes
            .saturating_mul(128)
            .saturating_add(4096)
    }
    pub(crate) fn metadata_memory(&self) -> usize {
        // Native decoding bounds counts before allocating, but a malformed
        // short entry can still reserve a full TreeEntry slot. Account for two
        // simultaneously owned native vectors as well as wire/encoding copies.
        let items = self
            .model_limits()
            .max_items
            .min(self.max_metadata_bytes / 4);
        self.max_metadata_bytes
            .saturating_mul(8)
            .saturating_add(
                items
                    .saturating_mul(std::mem::size_of::<tinytree_model::TreeEntry>())
                    .saturating_mul(2),
            )
            .saturating_add(4096)
    }
    pub(crate) fn catalog_memory(&self) -> usize {
        self.max_catalog_bytes
            .saturating_mul(4)
            .saturating_add(self.max_catalog_objects.saturating_mul(1024))
            .saturating_add(4096)
    }
    pub(crate) fn file_memory(&self) -> usize {
        // Direct providers stage/hash files through fixed 64 KiB buffers even
        // when the transport is configured to send smaller frames.
        self.max_frame_bytes
            .max(64 * 1024)
            .saturating_mul(8)
            .saturating_add(4096)
    }
    pub(crate) fn model_limits(&self) -> tinytree_model::Limits {
        tinytree_model::Limits {
            max_object_bytes: self.max_metadata_bytes,
            ..Default::default()
        }
    }
}
