use anyhow::Result;
use async_trait::async_trait;
use futures::io::Cursor;
use jj_lib::backend::{BackendError, BackendResult};
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, VerifiedMetadata,
};
use local_cas::{Artifact, BlobSource, CacheConfig, LocalCas};
use native_vfs::Config;
use native_workspace::{Limits, Scope};
use output_core::SourceLimits;
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tinytree_model as m;
fn error() -> BackendError {
    BackendError::Other(Box::new(std::io::Error::other("fixture unavailable")))
}
#[derive(Debug, Default)]
pub struct Provider {
    metadata: Mutex<HashMap<m::ObjectKey, (m::Descriptor, Vec<u8>)>>,
    files: Mutex<HashMap<m::FileId, (m::Descriptor, Vec<u8>)>>,
    pub file_reads: AtomicUsize,
    pub metadata_reads: AtomicUsize,
    pub metadata_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}
impl Provider {
    pub fn object(&self, object: m::Object) -> m::Descriptor {
        let e = m::encode(&object, m::Limits::default()).unwrap();
        self.metadata
            .lock()
            .unwrap()
            .insert(e.descriptor.key, (e.descriptor, e.bytes));
        e.descriptor
    }
    pub fn file(&self, bytes: &[u8]) -> m::Descriptor {
        let d = m::describe_file(bytes);
        self.files
            .lock()
            .unwrap()
            .insert(m::FileId(d.key.id.0), (d, bytes.to_vec()));
        d
    }
}
#[async_trait]
impl NativeObjectIo for Provider {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        Ok(if key.kind == m::Kind::File {
            self.files
                .lock()
                .unwrap()
                .get(&m::FileId(key.id.0))
                .map(|v| v.0)
        } else {
            self.metadata.lock().unwrap().get(&key).map(|v| v.0)
        })
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.metadata_reads.fetch_add(1, Ordering::SeqCst);
        let gate = self.metadata_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.map_err(|_| error())?.forget();
        }
        let (d, b) = self
            .metadata
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(error)?;
        VerifiedMetadata::verify(d, b, m::Limits::default())
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        self.read_metadata(key).await
    }
    async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
        Err(error())
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        _owner: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        self.read_file(id).await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.file_reads.fetch_add(1, Ordering::SeqCst);
        let (d, b) = self
            .files
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(error)?;
        FileRead::from_preverified_pinned_source(d, Box::pin(Cursor::new(b)))
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Send + Unpin),
        _: u64,
    ) -> BackendResult<m::FileId> {
        Err(error())
    }
    async fn copy_catalog_snapshot(
        &self,
        l: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        CopyCatalogSnapshot::from_verified(vec![], l)
    }
}
struct NoSource;
#[async_trait]
impl BlobSource for NoSource {
    async fn fetch(&self, _: &Artifact, _: &mut File) -> Result<()> {
        anyhow::bail!("shared cache requires an authorized per-workspace source")
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("no default source")
    }
}
pub fn cache(path: &Path) -> Arc<LocalCas> {
    LocalCas::open(
        path.join("cache"),
        Arc::new(NoSource),
        CacheConfig::default(),
    )
    .unwrap()
}
pub fn fixture(path: &Path) -> (Config, Arc<Provider>) {
    let provider = Arc::new(Provider::default());
    let tool = provider.file(b"native source\n");
    let unused = provider.file(b"untouched source\n");
    let link = provider.object(m::Object::Symlink("tool".into()));
    let entry = |name: &str, d: m::Descriptor, executable| m::TreeEntry {
        name: name.into(),
        value: m::TreeValue::File {
            id: m::FileId(d.key.id.0),
            size_bytes: d.size_bytes,
            executable,
            copy_id: None,
        },
    };
    let root = provider.object(m::Object::Tree(m::Tree {
        entries: vec![
            m::TreeEntry {
                name: "link".into(),
                value: m::TreeValue::Symlink {
                    id: m::SymlinkId(link.key.id.0),
                    target_bytes: 4,
                },
            },
            entry("tool", tool, true),
            entry("unused", unused, false),
        ],
    }));
    (
        Config {
            root: path.join("workspace"),
            state: path.join("state"),
            workspace_identity: [3; 32],
            scope: Scope::from_configured_identity("repository", b"trusted-provider-scope")
                .unwrap(),
            root_merge: m::TreeMerge {
                terms: vec![m::TreeId(root.key.id.0)],
                labels: vec![String::new()],
            },
            compiler_limits: Limits::default(),
            source_limits: SourceLimits::default(),
        },
        provider,
    )
}
