//! Configured worst-case admission charges, not measured process RSS. These
//! exclude transport framing, protobuf containers, database caches and page cache.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudResourceProfile {
    pub object_bytes: u64,
    pub index_artifact_bytes: u64,
    pub version_v1_bytes: u64,
    pub version_v2_bytes: u64,
    pub version_v3_bytes: u64,
    pub capture_read_bytes: u64,
    pub native_read_bytes: u64,
}
impl Config {
    /// Pure preflight usable before opening storage. The same calculation is
    /// exposed by Store for adapters handed an already-open instance.
    pub fn cloud_resource_profile(&self) -> Result<CloudResourceProfile> {
        let add = |terms: &[Option<u64>]| {
            terms.iter().try_fold(0u64, |sum, term| {
                sum.checked_add(term.ok_or(Error::Limit("cloud resource profile"))?)
                    .ok_or(Error::Limit("cloud resource profile"))
            })
        };
        // Unset retains the legacy cumulative-work admission exactly. The
        // opt-in policy precharges owned catalog arenas and retained pin-table
        // capacity independently; codec/closure/path/packed scratch is additive.
        let (catalog_bytes, catalog_entries) = match self.cloud.resident {
            Some(limits) => {
                limits.validate()?;
                (Some(limits.max_bytes), Some(0))
            }
            None => (
                self.max_retention_validation_bytes.checked_mul(128),
                (self.max_retention_validation_records as u64).checked_mul(4096),
            ),
        };
        let version_v1_bytes = add(&[
            catalog_bytes,
            catalog_entries,
            Some(self.cloud.max_version_decode_bytes as u64),
            Some(self.cloud.max_decode_bytes as u64),
            (self.cloud.manifest_limits.max_decode_bytes as u64).checked_mul(2),
            (self.model_limits.max_object_bytes as u64).checked_mul(16),
            (self.max_closure_objects as u64).checked_mul(1024),
            (self.max_path_contexts as u64).checked_mul(1024),
            (self.max_path_context_bytes as u64).checked_mul(4),
            Some(self.max_packed_read_memory_bytes),
            Some(1024 * 1024),
        ])?;
        let native = self
            .max_closure_objects
            .min(self.cloud.manifest_limits.max_native_roots) as u64;
        let artifacts = self.cloud.index_artifacts.max_artifacts as u64;
        let root_row = add(&[Some(74), native.checked_mul(33), artifacts.checked_mul(73)])?
            .min(CLOUD_BINARY_ROW_BYTES as u64);
        // TTROOT02 input, legacy native-prefix copy, output and descriptor
        // arrays may coexist. Per-entry allowance also covers selected closure
        // copies and runtime read-root hash entries outside the version codec.
        // The version budget itself already covers artifact graph/native proof
        // decode; standalone artifact decode is NOT added a second time here.
        let version_v2_bytes = add(&[
            Some(version_v1_bytes),
            root_row.checked_mul(4),
            add(&[
                Some(native),
                Some(artifacts),
                Some(self.cloud.manifest_limits.max_members as u64),
            ])?
            .checked_mul(1024),
        ])?;
        // Capture decoding/binding shares max_version_decode_bytes with live
        // OP/VIEW+index data. Additional conservative raw-envelope/copy slack
        // covers the inline request/immutable row while that proof is admitted.
        let capture = tinytree_cloud::CaptureDataLimits::default().max_bytes as u64;
        let version_v3_bytes = add(&[Some(version_v2_bytes), capture.checked_mul(3), Some(4096)])?;
        let capture_read_bytes =
            add(&[Some(version_v3_bytes), capture.checked_mul(2), Some(4096)])?;
        let resident_bytes = self.cloud.resident.map_or(0, |limits| limits.max_bytes);
        let object_bytes = add(&[
            Some(resident_bytes),
            Some(self.cloud.max_decode_bytes as u64),
            (self.cloud.max_object_bytes as u64).checked_mul(4),
            Some(8 * 1024 * 1024),
        ])?;
        let index_artifact_bytes = add(&[
            Some(resident_bytes),
            Some(self.cloud.index_artifacts.max_decode_bytes as u64),
            (self.cloud.index_artifacts.max_artifact_bytes as u64).checked_mul(4),
            Some(8 * 1024 * 1024),
        ])?;
        // The version guard stays live while sequential canonical proof/read
        // scratch and one packed reconstruction are owned by the same call.
        let native_read_bytes = add(&[
            Some(version_v1_bytes.max(version_v2_bytes).max(version_v3_bytes)),
            (self.model_limits.max_object_bytes as u64).checked_mul(16),
            Some(self.max_packed_read_memory_bytes),
            Some(1024 * 1024),
        ])?;
        Ok(CloudResourceProfile {
            object_bytes,
            index_artifact_bytes,
            version_v1_bytes,
            version_v2_bytes,
            version_v3_bytes,
            capture_read_bytes,
            native_read_bytes,
        })
    }
}
impl Store {
    pub fn cloud_resource_profile(&self) -> Result<CloudResourceProfile> {
        self.inner.config.cloud_resource_profile()
    }
}
