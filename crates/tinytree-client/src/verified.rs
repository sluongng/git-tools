use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
};
pub(crate) type LifetimeGuards = Arc<Vec<Arc<dyn Any + Send + Sync>>>;
use tinytree_model::{Descriptor, Object};
use tonic::Status;

/// Canonical metadata verified against its native descriptor, not protobuf bytes.
pub struct VerifiedMetadata {
    pub(crate) descriptor: Descriptor,
    pub(crate) bytes: Vec<u8>,
    pub(crate) object: Object,
}
impl VerifiedMetadata {
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn object(&self) -> &Object {
        &self.object
    }
}
#[derive(Clone)]
pub(crate) struct Temporary {
    pub file: Arc<OwnedTemporary>,
    pub _disk: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub _guards: LifetimeGuards,
}
impl Temporary {
    fn with_guard<T: Any + Send + Sync>(mut self, guard: Arc<T>) -> Self {
        let mut guards = self._guards.as_ref().clone();
        guards.push(guard);
        self._guards = Arc::new(guards);
        self
    }
}
pub(crate) struct OwnedTemporary {
    file: tempfile::NamedTempFile,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}
impl OwnedTemporary {
    pub fn new(file: tempfile::NamedTempFile) -> Result<Self, Status> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = file
                .as_file()
                .metadata()
                .map_err(|_| Status::internal("cannot inspect native temporary file"))?;
            return Ok(Self {
                file,
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }
        #[cfg(not(unix))]
        {
            Ok(Self { file })
        }
    }
}
impl std::ops::Deref for OwnedTemporary {
    type Target = tempfile::NamedTempFile;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}
impl Drop for OwnedTemporary {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if let Ok(metadata) = std::fs::symlink_metadata(self.file.path()) {
                if metadata.dev() == self.device && metadata.ino() == self.inode {
                    let _ = std::fs::remove_file(self.file.path());
                }
            }
        }
    }
}
/// A completely verified private temporary file. It has not been installed at a
/// caller's destination. Dropping it cleans up and releases temporary-disk quota.
pub struct VerifiedFile {
    pub(crate) temporary: Temporary,
    pub(crate) descriptor: Descriptor,
}
impl VerifiedFile {
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
    }
    pub fn open(&self) -> Result<VerifiedReader, Status> {
        let file = self
            .temporary
            .file
            .reopen()
            .map_err(|_| Status::data_loss("verified native temporary file was replaced"))?;
        Ok(VerifiedReader {
            file,
            _temporary: self.temporary.clone(),
        })
    }
    /// Explicit synchronous installation, suitable for a caller-owned blocking
    /// task. Does not overwrite an existing destination. No network or retry.
    pub fn persist_noclobber(self, destination: &Path) -> Result<PathBuf, Status> {
        self.temporary
            .file
            .reopen()
            .map_err(|_| Status::data_loss("verified native temporary file was replaced"))?;
        self.temporary
            .file
            .as_file()
            .sync_all()
            .map_err(|_| Status::internal("cannot synchronize verified native file"))?;
        std::fs::hard_link(self.temporary.file.path(), destination).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Status::already_exists("native output destination exists")
            } else {
                Status::internal("cannot install verified native file")
            }
        })?;
        Ok(destination.into())
    }
}
/// Read-only capability: the underlying file handle is deliberately private.
pub struct VerifiedReader {
    file: std::fs::File,
    _temporary: Temporary,
}
impl std::io::Read for VerifiedReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.file, buffer)
    }
}
impl std::io::Seek for VerifiedReader {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.file, position)
    }
}
/// An owned private immutable snapshot, reopened from the beginning for each
/// upload attempt. A mutable caller path is never reopened between retries.
#[derive(Clone)]
pub struct UploadSource {
    pub(crate) inner: Arc<Source>,
}
pub(crate) struct Source {
    pub temporary: Temporary,
    pub descriptor: Descriptor,
}
impl UploadSource {
    /// Keep external resource ownership with this returned source and every
    /// derived retry, reader and blocking result. Previously created clones
    /// are not modified. This grants no verification or retention guarantee.
    pub fn with_lifetime_guard<T: Any + Send + Sync>(self, guard: Arc<T>) -> Self {
        Self {
            inner: Arc::new(Source {
                temporary: self.inner.temporary.clone().with_guard(guard),
                descriptor: self.inner.descriptor,
            }),
        }
    }
    pub fn descriptor(&self) -> Descriptor {
        self.inner.descriptor
    }
    pub fn from_verified_file(file: VerifiedFile) -> Self {
        Self {
            inner: Arc::new(Source {
                temporary: file.temporary,
                descriptor: file.descriptor,
            }),
        }
    }
}
pub enum MetadataOutcome {
    Object(VerifiedMetadata),
    Error {
        code: tinytree_proto::v1::item_error::Code,
    },
}
/// Server-selected summaries whose individual descriptors and parent lists were
/// checked against canonical commits. This does NOT verify set completeness,
/// reachability, ancestry-difference topology, or continued closure retention.
/// Callers needing an authoritative graph must traverse canonical commits.
pub struct CanonicalCommitSummaries {
    pub entries: Vec<tinytree_proto::v1::CommitGraphEntry>,
}
