//! Broker-hosted lazy native I/O. Canonical local objects remain authoritative;
//! the admission ledger records roots/proofs, never fabricated file residency.
mod broker;
mod closure;
mod cloud;
pub use cloud::AdmittedCloudVersion;
mod config;
mod control;
mod io;
mod ledger;
mod metadata;
mod provider;
mod query;
mod staging;
use anyhow::{Context, Result, ensure};
pub use config::{ClosureLimits, Config, QueryLimits};
pub use control::{
    AdmittedHead, AdmittedReference, ConfirmedPublication, ObservedReference, PublishRequest,
    RemoteRepositoryControl,
};
use jj_native_backend::DirectObjectIo;
use local_cas::LocalCas;
pub use provider::RemoteObjectIo;
pub use query::RemoteQueryIo;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tinytree_client::NativeClient;

pub struct RemoteRepository {
    pub io: Arc<RemoteObjectIo>,
    pub control: RemoteRepositoryControl,
    pub query: Arc<RemoteQueryIo>,
}
pub(crate) struct Inner {
    pub local: Arc<DirectObjectIo>,
    pub cache: Arc<LocalCas>,
    pub ledger: Arc<ledger::Ledger>,
    pub connector: Arc<Connector>,
    pub staging: PathBuf,
    pub staging_owner: Arc<staging::Owner>,
    pub config: Config,
    pub control: Arc<tokio::sync::Semaphore>,
    pub metadata: Arc<tokio::sync::Mutex<()>>,
    pub metadata_work: Arc<io::OwnedWork>,
    #[cfg(test)]
    pub catalog_boundary: std::sync::Mutex<Option<Arc<metadata::CatalogBoundary>>>,
    pub spool: Arc<tokio::sync::Semaphore>,
}
impl Inner {
    fn model_limits(&self) -> tinytree_model::Limits {
        tinytree_model::Limits {
            max_object_bytes: self.config.remote.limits.max_metadata_bytes,
            max_path_bytes: self.config.closure.max_path_bytes,
            ..Default::default()
        }
    }
    pub fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_millis(self.config.operation_timeout_ms)
    }
}
impl RemoteRepository {
    /// Stop producers before awaiting accepted owned metadata/control work.
    pub async fn wait_for_idle(&self) {
        self.control.drain_control().await;
        self.io.inner.metadata_work.wait_for_idle().await;
    }

    /// Paths/configuration are trusted broker inputs, never incoming RPC fields.
    /// No network connection is established while opening an offline workspace.
    pub fn open(
        local: Arc<DirectObjectIo>,
        cache: Arc<LocalCas>,
        ledger_path: &Path,
        private_staging: &Path,
        config: Config,
    ) -> Result<Self> {
        let ca_certificate = match &config.remote.tls {
            Some(tls) => Some(read_ca(&tls.ca_certificate)?),
            None => None,
        };
        Self::open_with_ca_certificate(
            local,
            cache,
            ledger_path,
            private_staging,
            config,
            ca_certificate,
        )
    }
    /// Open with the daemon's already-captured trust snapshot. Lazy connection
    /// never rereads a mutable CA path or substitutes different trust material.
    pub fn open_with_ca_certificate(
        local: Arc<DirectObjectIo>,
        cache: Arc<LocalCas>,
        ledger_path: &Path,
        private_staging: &Path,
        config: Config,
        ca_certificate: Option<Arc<[u8]>>,
    ) -> Result<Self> {
        config.validate()?;
        ensure!(
            config.remote.tls.is_some() == ca_certificate.is_some(),
            "native TLS configuration requires a captured CA snapshot"
        );
        if let Some(ca) = &ca_certificate {
            ensure!(
                !ca.is_empty() && ca.len() <= 1024 * 1024,
                "native CA snapshot exceeds configured bounds"
            );
        }
        let trust = if let Some(ca) = &ca_certificate {
            let mut hash = blake3::Hasher::new();
            hash.update(b"native-tls-scope-v2\0");
            hash.update(blake3::hash(ca).as_bytes());
            match config
                .remote
                .tls
                .as_ref()
                .and_then(|tls| tls.domain_name.as_deref())
            {
                None => {
                    hash.update(&[0]);
                }
                Some(domain) => {
                    hash.update(&[1]);
                    hash.update(&(domain.len() as u64).to_be_bytes());
                    hash.update(domain.as_bytes());
                }
            }
            hash.finalize().to_hex().to_string()
        } else {
            "native-local-plaintext-v1".into()
        };
        let online = config
            .remote
            .token_env
            .as_ref()
            .is_none_or(|name| std::env::var(name).is_ok());
        let fresh = ledger::preflight(ledger_path)?;
        let staging_plan = staging::preflight(private_staging, config.max_staging_files)?;
        if fresh {
            if let Some(name) = &config.remote.token_env {
                ensure!(
                    std::env::var(name).is_ok(),
                    "cannot initialize native remote ledger without configured credentials"
                );
            }
        }
        let staging_owner = staging::open(private_staging, staging_plan)?;
        let staging = private_staging.canonicalize()?;
        let ledger = Arc::new(ledger::Ledger::open(ledger_path, &config, &trust)?);
        let connector = Arc::new(Connector {
            config: config.remote.clone(),
            credential: ledger.binding.credential.clone(),
            client: tokio::sync::OnceCell::new(),
            owner: staging_owner.clone(),
            ca_certificate,
            online,
        });
        let inner = Arc::new(Inner {
            local,
            cache,
            ledger,
            connector,
            staging,
            staging_owner,
            control: Arc::new(tokio::sync::Semaphore::new(1)),
            metadata: Arc::new(tokio::sync::Mutex::new(())),
            metadata_work: Arc::new(io::OwnedWork::default()),
            #[cfg(test)]
            catalog_boundary: std::sync::Mutex::new(None),
            spool: Arc::new(tokio::sync::Semaphore::new(
                config.max_upload_spool_bytes as usize,
            )),
            config,
        });
        Ok(Self {
            io: Arc::new(RemoteObjectIo {
                inner: inner.clone(),
            }),
            query: Arc::new(RemoteQueryIo::new(inner.clone())?),
            control: RemoteRepositoryControl { inner },
        })
    }
}
pub(crate) struct Connector {
    config: tinytree_client::Config,
    credential: String,
    client: tokio::sync::OnceCell<NativeClient>,
    owner: Arc<staging::Owner>,
    ca_certificate: Option<Arc<[u8]>>,
    online: bool,
}
impl Connector {
    pub async fn get(&self) -> Result<NativeClient> {
        ensure!(
            self.online,
            "native repository was opened offline; reopen with credentials before network access"
        );
        let actual = match &self.config.token_env {
            None => "anonymous".into(),
            Some(name) => {
                let value = std::env::var(name).map_err(|_| {
                    anyhow::anyhow!(
                        "native credentials unavailable; only offline local/cache access is allowed"
                    )
                })?;
                blake3::hash(value.as_bytes()).to_hex().to_string()
            }
        };
        ensure!(
            actual == self.credential,
            "native credentials changed; reopen the broker repository before network access"
        );
        let client = io::network(async {
            Ok::<_, tonic::Status>(
                self.client
                    .get_or_try_init(|| async {
                        let client = if let Some(ca) = &self.ca_certificate {
                            NativeClient::connect_with_ca_certificate(
                                self.config.clone(),
                                ca.clone(),
                            )
                            .await?
                        } else {
                            NativeClient::connect(self.config.clone()).await?
                        };
                        Ok::<_, tonic::Status>(client.with_lifetime_guard(self.owner.clone()))
                    })
                    .await?
                    .clone(),
            )
        })
        .await?;
        Ok(if let Some(scope) = io::scope() {
            client.with_lifetime_guard(scope)
        } else {
            client
        })
    }
}
pub(crate) fn backend(error: jj_lib::backend::BackendError) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}
pub(crate) fn backend_error(error: anyhow::Error) -> jj_lib::backend::BackendError {
    jj_lib::backend::BackendError::Other(error.into())
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn unhex<const N: usize>(text: &str) -> Result<[u8; N]> {
    ensure!(text.len() == N * 2, "invalid native durable identity");
    let mut bytes = [0; N];
    for (i, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let part = std::str::from_utf8(pair)?;
        bytes[i] = u8::from_str_radix(part, 16).context("invalid native durable identity")?;
    }
    Ok(bytes)
}
pub(crate) fn private_directory(path: &Path) -> Result<private_fs::PrivateDir> {
    ensure!(
        path.is_absolute() && path.as_os_str().len() <= 4096,
        "native private path must be bounded and absolute"
    );
    if std::fs::symlink_metadata(path)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        // Keep the established stricter creation policy: caller provisions a
        // private parent before a new native storage child is created.
        let parent = path.parent().context("native private path has no parent")?;
        #[cfg(unix)]
        let parent = parent.canonicalize()?;
        let parent = private_fs::PrivateDir::open_existing(parent)?;
        let name = path
            .file_name()
            .context("native private path has no filename")?;
        return Ok(private_fs::PrivateDir::create(parent.path().join(name))?);
    }
    #[cfg(unix)]
    let path = path.canonicalize()?;
    Ok(private_fs::PrivateDir::open_existing(path)?)
}

fn read_ca(path: &Path) -> Result<Arc<[u8]>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| anyhow::anyhow!("cannot capture native TLS trust"))?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("cannot capture native TLS trust"))?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= 1024 * 1024,
        "native CA snapshot exceeds configured bounds"
    );
    Ok(Arc::from(bytes))
}
