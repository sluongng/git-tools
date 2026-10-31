//! Immutable capture attachments share the authoritative version transaction.
//! The encoded policy remains client-declared data, not a live writer fence.
use super::*;
use tinytree_cloud::{CaptureDataId, CaptureDataLimits, CaptureDescriptor, ManifestV3Limits};
pub(super) const HEADER: usize = 80;
const MAGIC: &[u8; 8] = b"TTCAP01\0";

/// Additive publication API: ordinary request literals remain unchanged.
pub struct PublishCapturedCloudRequest {
    pub publication: PublishCloudRequest,
    pub capture_data: Vec<u8>,
}
pub(super) struct CaptureRecord {
    pub descriptor: CaptureDescriptor,
    pub bytes: Vec<u8>,
}
pub(super) fn encode_record(descriptor: CaptureDescriptor, bytes: &[u8]) -> Result<Vec<u8>> {
    if descriptor != CaptureDescriptor::of(bytes)
        || bytes.is_empty()
        || bytes.len() > CaptureDataLimits::default().max_bytes
    {
        return Err(Error::Invalid("cloud capture descriptor or size"));
    }
    let mut row = Vec::with_capacity(
        HEADER
            .checked_add(bytes.len())
            .ok_or(Error::Limit("cloud capture row"))?,
    );
    row.extend_from_slice(MAGIC);
    row.extend_from_slice(&descriptor.id.0);
    row.extend_from_slice(&descriptor.selected.blake3);
    row.extend_from_slice(&descriptor.selected.size_bytes.to_le_bytes());
    row.extend_from_slice(bytes);
    Ok(row)
}
pub(super) fn decode_record(row: &[u8]) -> Result<CaptureRecord> {
    if row.len() < HEADER
        || row.len() > HEADER + CaptureDataLimits::default().max_bytes
        || &row[..8] != MAGIC
    {
        return Err(Error::Corrupt("cloud capture record format"));
    }
    let descriptor = CaptureDescriptor {
        id: CaptureDataId(row[8..40].try_into().unwrap()),
        selected: BlobDigest {
            blake3: row[40..72].try_into().unwrap(),
            size_bytes: u64::from_le_bytes(row[72..80].try_into().unwrap()),
        },
    };
    if descriptor != CaptureDescriptor::of(&row[HEADER..]) || descriptor.selected.size_bytes == 0 {
        return Err(Error::Corrupt("cloud capture record identity"));
    }
    Ok(CaptureRecord {
        descriptor,
        bytes: row[HEADER..].to_vec(),
    })
}
/// Minimum durable format for independently validated capture semantics. This
/// classifies untrusted historical data; it never creates landing authority.
pub(super) fn minimum_store_feature(data: &tinytree_cloud::CaptureData) -> u8 {
    if data.version == 2 {
        if data.remote.as_ref().is_some_and(|remote| {
            !remote.archived_landings.is_empty()
                || remote
                    .landing
                    .as_ref()
                    .and_then(|landing| landing.observed.as_ref())
                    .is_some_and(|job| {
                        job.phase == tinytree_cloud::HistoricalLandingPhase::PublicationAbandoned
                    })
        }) {
            return 13;
        }
        if data
            .remote
            .as_ref()
            .and_then(|remote| remote.landing.as_ref())
            .and_then(|landing| landing.observed.as_ref())
            .is_some_and(|job| {
                job.phase == tinytree_cloud::HistoricalLandingPhase::StaleTarget
                    && job.candidate.is_none()
            })
        {
            10
        } else {
            9
        }
    } else {
        5
    }
}
impl Inner {
    pub(super) fn capture_record(
        &self,
        key: &[u8],
        expected: Option<CaptureDescriptor>,
    ) -> Result<Option<CaptureRecord>> {
        let row = self.db.read_tx().get(&self.spaces.cloud.captures, key)?;
        match (expected, row) {
            (None, None) => Ok(None),
            (Some(expected), Some(row)) => {
                if row
                    .len()
                    .checked_mul(3)
                    .is_none_or(|n| n > self.config.cloud.max_version_decode_bytes)
                {
                    return Err(Error::Limit("cloud capture input memory"));
                }
                let record = decode_record(&row)?;
                if record.descriptor != expected {
                    return Err(Error::Corrupt("cloud version capture selection"));
                }
                Ok(Some(record))
            }
            (Some(_), None) => Err(Error::Corrupt("cloud version capture missing")),
            (None, Some(_)) => Err(Error::Corrupt("unexpected cloud version capture")),
        }
    }
    pub(super) fn v3_limits(
        &self,
        manifest: tinytree_cloud::ManifestLimits,
        remaining: usize,
    ) -> Result<ManifestV3Limits> {
        let mut limits = ManifestV3Limits::default();
        limits.history = self.v2_limits(manifest)?;
        limits.capture.max_decode_bytes = limits.capture.max_decode_bytes.min(remaining);
        limits.capture.max_operations = limits.capture.max_operations.min(
            self.config
                .cloud
                .manifest_limits
                .max_members
                .saturating_add(1),
        );
        limits.capture.max_roots = limits
            .capture
            .max_roots
            .min(self.config.cloud.manifest_limits.max_native_roots);
        limits.capture.max_blobs = limits.capture.max_blobs.min(
            self.config
                .cloud
                .index_artifacts
                .max_artifacts
                .saturating_add(self.config.cloud.manifest_limits.max_members),
        );
        limits.binding.max_bytes = remaining;
        limits.binding.max_work = limits
            .binding
            .max_work
            .min(self.config.cloud.index_artifacts.max_graph_steps as u64);
        limits
            .validate()
            .map_err(|_| Error::Limit("cloud capture validation budget"))?;
        Ok(limits)
    }
}
/// Capsule bytes are destroyed before the actual retained-version reader pin.
pub struct CloudCaptureReadLease {
    bytes: io::Cursor<Vec<u8>>,
    descriptor: CaptureDescriptor,
    version: CloudVersionReadGuard,
}
impl CloudCaptureReadLease {
    pub fn descriptor(&self) -> CaptureDescriptor {
        self.descriptor
    }
    pub fn version(&self) -> &CloudVersion {
        self.version.version()
    }
}
impl Read for CloudCaptureReadLease {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.bytes.read(bytes)
    }
}
impl Store {
    pub fn publish_captured_cloud_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        request: &PublishCapturedCloudRequest,
        now: i64,
        limits: &CloudPublicationLimits,
    ) -> Result<CloudPublication> {
        self.publish_cloud_version_attachment(
            repo,
            owner,
            &request.publication,
            Some(&request.capture_data),
            now,
            limits,
        )
    }
    pub fn read_cloud_capture(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: CloudVersionSelector,
        expected: CaptureDataId,
    ) -> Result<CloudCaptureReadLease> {
        let mut version = self.read_cloud_version(
            repo,
            owner,
            selector.workspace,
            selector.generation,
            self.inner.config.cloud.manifest_limits,
        )?;
        if version.version().manifest != selector.manifest
            || BlobDigest::of(version.manifest_bytes()) != selector.selected
        {
            return Err(Error::Invalid("cloud capture version selection"));
        }
        let capture = version
            .take_capture()
            .ok_or(Error::Invalid("cloud version has no capture attachment"))?;
        if capture.descriptor.id != expected {
            return Err(Error::Invalid("cloud capture selection"));
        }
        Ok(CloudCaptureReadLease {
            bytes: io::Cursor::new(capture.bytes),
            descriptor: capture.descriptor,
            version,
        })
    }
}
