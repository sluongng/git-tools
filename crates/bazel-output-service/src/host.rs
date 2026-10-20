//! In-process lazy output mount hosting. The caller owns RPC serving and the
//! shared CAS. Shut down RPC admission before draining and unmounting this host.
use crate::{Config, OutputService};
use anyhow::Result;
use local_cas::LocalCas;
use output_core::{Projection, Tree};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Copy, Debug, Default)]
pub enum LazyStorage {
    #[default]
    Native,
    Fuse,
    Nfs,
    Projfs,
}

pub struct HostConfig {
    pub service: Config,
    pub state_dir: PathBuf,
    pub storage: LazyStorage,
    /// Exact cache origin advertised to Bazel, if it differs from the upstream.
    pub client_cache_endpoint: Option<String>,
}

pub struct OutputHost {
    service: OutputService,
    tree: Arc<Tree>,
    projection: Arc<dyn Projection>,
    cache: Arc<LocalCas>,
}

impl OutputHost {
    pub async fn start(config: HostConfig, cache: Arc<LocalCas>) -> Result<Self> {
        let endpoint = config
            .client_cache_endpoint
            .as_deref()
            .unwrap_or(&config.service.cas_endpoint)
            .to_owned();
        let storage = match config.storage {
            LazyStorage::Native => {
                #[cfg(target_os = "linux")]
                {
                    LazyStorage::Fuse
                }
                #[cfg(target_os = "macos")]
                {
                    LazyStorage::Nfs
                }
                #[cfg(windows)]
                {
                    LazyStorage::Projfs
                }
                #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
                anyhow::bail!("no native lazy output backend for this platform");
            }
            storage => storage,
        };
        #[cfg(target_os = "macos")]
        if matches!(storage, LazyStorage::Nfs) {
            output_nfs::ensure_unmounted(&config.service.root)?;
        }
        // Canonicalize paths before mounting: traversing our own NFS projection
        // from its async runtime can otherwise deadlock a filesystem request.
        std::fs::create_dir_all(&config.state_dir)?;
        std::fs::create_dir_all(&config.service.root)?;
        let state = config.state_dir.canonicalize()?;
        let root = config.service.root.canonicalize()?;
        anyhow::ensure!(
            !state.starts_with(&root) && !root.starts_with(&state),
            "state directory and output mount must not overlap"
        );
        let service = OutputService::new_with_client_endpoint(config.service, &endpoint).await?;
        let tree = Tree::open_with_cache_and_source(
            &state,
            service.backend_identity(),
            cache.clone(),
            service.blob_source(),
        )?;
        let projection: Arc<dyn Projection> = match storage {
            LazyStorage::Fuse => {
                #[cfg(target_os = "linux")]
                {
                    output_fuse::start(tree.clone(), &root).await?
                }
                #[cfg(not(target_os = "linux"))]
                anyhow::bail!("FUSE lazy output storage requires Linux");
            }
            LazyStorage::Nfs => {
                #[cfg(target_os = "macos")]
                {
                    Arc::new(output_nfs::NfsMount::start(tree.clone(), &root).await?)
                }
                #[cfg(not(target_os = "macos"))]
                anyhow::bail!("NFS lazy output storage requires native macOS");
            }
            LazyStorage::Projfs => {
                #[cfg(windows)]
                {
                    Arc::new(
                        output_projfs::start(tree.clone(), &root, &state.join("provider")).await?,
                    )
                }
                #[cfg(not(windows))]
                anyhow::bail!("ProjFS lazy output storage requires native Windows");
            }
            LazyStorage::Native => unreachable!(),
        };
        // The roots were canonicalized before mount; attachment cannot require
        // filesystem I/O. Still unwind through shutdown if validation fails.
        let service = match service.with_projection(projection.clone(), tree.clone()) {
            Ok(service) => service,
            Err(error) => {
                projection.shutdown().await?;
                return Err(error);
            }
        };
        Ok(Self {
            service,
            tree,
            projection,
            cache,
        })
    }
    pub fn service(&self) -> OutputService {
        self.service.clone()
    }
    pub fn tree(&self) -> Arc<Tree> {
        self.tree.clone()
    }
    /// Stop RPC admission before calling. A busy mount returns an error and is
    /// retained: the caller must keep the runtime alive and retry shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        self.projection.shutdown().await?;
        self.cache.flush_recency()?;
        Ok(())
    }
}
