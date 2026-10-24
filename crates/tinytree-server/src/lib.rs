//! Native repository service. Git serving and transformation are separate.
//!
//! Authorization, bounded streaming and durable store publication are distinct
//! layers. The listener helpers enforce safe-local or authenticated TLS access;
//! callers assembling their own tonic transport must preserve that boundary.
mod auth;
mod body_deadline;
pub use body_deadline::DeadlineLayer;
mod cloud;
mod cloud_ingress;
mod landing;
mod landing_ingress;
pub use landing::{LandingConfig, LandingProfile};
pub use landing_ingress::LandingGrpcService;
mod config;
pub use cloud::CloudConfig;
pub use cloud_ingress::CloudGrpcService;
mod derived_index;
mod query_cursor;
mod query_eval;
mod query_graph;
mod query_history;
mod query_index;
mod query_ingress;
mod query_overlay;
mod query_prefix;
mod query_search;
mod query_service;
mod retention;
mod service;
pub use query_index::QueryConfig;
pub use query_ingress::QueryGrpcService;
pub use query_search::SearchStats;
pub use query_service::QueryService;

pub use auth::Principal;
pub use config::{
    CloudStoreConfig, Config, ConfigError, PrincipalConfig, ServiceConfig, StoreConfig, TlsConfig,
};
pub use service::RepositoryService;

use anyhow::{Context, Result, ensure};
use std::{
    future::Future,
    net::SocketAddr,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tonic::transport::{Identity, Server, ServerTlsConfig};

/// Serve an already-bound TCP listener. Shutdown drains requests/streams rather
/// than imposing a forced connection-age deadline.
pub async fn serve_tcp(
    listener: tokio::net::TcpListener,
    service: RepositoryService,
    tls: Option<Identity>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let address = listener
        .local_addr()
        .context("cannot inspect native listener")?;
    ensure!(
        address.ip().is_loopback() || (tls.is_some() && service.authentication_enabled()),
        "non-loopback listeners require TLS and authorization"
    );
    let mut server = Server::builder();
    if let Some(identity) = tls {
        server = server
            .tls_config(ServerTlsConfig::new().identity(identity))
            .map_err(|_| anyhow::anyhow!("cannot configure native TLS"))?;
    }
    service
        .start_landing_worker()
        .map_err(|_| anyhow::anyhow!("cannot start landing worker"))?;
    let drain = service.clone();
    let result = server
        .layer(service.deadline_layer())
        .concurrency_limit_per_connection(service.maximum_inflight())
        .add_service(service.grpc_service())
        .add_service(service.query_grpc_service())
        .add_service(service.cloud_grpc_service())
        .add_service(service.landing_grpc_service())
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener).map(|accepted| {
                accepted.and_then(|stream| {
                    // Tonic does not configure socket options for custom incoming streams.
                    stream.set_nodelay(true)?;
                    Ok(stream)
                })
            }),
            shutdown,
        )
        .await
        .context("native gRPC listener failed");
    let query_drain = drain.drain_query().await;
    drain.drain_cloud().await;
    drain.drain_landing().await;
    query_drain.context("native query shutdown failed")?;
    result
}

#[cfg(unix)]
pub async fn serve_unix(
    path: &Path,
    service: RepositoryService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let (listener, _guard) = bind_unix(path)?;
    run_unix(listener, service, shutdown).await
}
#[cfg(unix)]
async fn run_unix(
    listener: tokio::net::UnixListener,
    service: RepositoryService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    service
        .start_landing_worker()
        .map_err(|_| anyhow::anyhow!("cannot start landing worker"))?;
    let drain = service.clone();
    let result = Server::builder()
        .layer(service.deadline_layer())
        .concurrency_limit_per_connection(service.maximum_inflight())
        .add_service(service.grpc_service())
        .add_service(service.query_grpc_service())
        .add_service(service.cloud_grpc_service())
        .add_service(service.landing_grpc_service())
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            shutdown,
        )
        .await
        .context("native Unix gRPC listener failed");
    let query_drain = drain.drain_query().await;
    drain.drain_cloud().await;
    drain.drain_landing().await;
    query_drain.context("native query shutdown failed")?;
    result
}

/// Initialize configured native repositories and run a standalone server.
/// No Git storage, transformation, credential provisioning or upstream work.
pub async fn serve(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    config.validate()?;
    // Resolve and validate credentials and TLS before creating any persistent state.
    let setup = config.clone();
    let (principals, tls) = tokio::task::spawn_blocking(move || -> Result<_> {
        let principals = setup
            .principals
            .iter()
            .map(|p| {
                let token = std::env::var(&p.token_env)
                    .map_err(|_| ConfigError("cannot resolve native principal token"))?;
                Principal::new(p.id.clone(), &token, p.repositories.clone(), p.write).map(
                    |principal| {
                        principal
                            .with_retention(p.retain)
                            .with_cloud(p.cloud)
                            .with_landing_profiles(p.landing_profiles.clone())
                    },
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        auth::Authorization::new(principals.clone(), &setup.repositories)?;
        let tls = setup
            .tls
            .as_ref()
            .map(|tls| -> Result<_> {
                let identity = Identity::from_pem(
                    read_identity_file(&tls.certificate)?,
                    read_identity_file(&tls.private_key)?,
                );
                // from_pem stores bytes; tls_config constructs and validates the acceptor.
                Server::builder()
                    .tls_config(ServerTlsConfig::new().identity(identity.clone()))
                    .map_err(|_| anyhow::anyhow!("cannot configure native TLS"))?;
                Ok(identity)
            })
            .transpose()?;
        Ok((principals, tls))
    })
    .await
    .context("native startup worker failed")??;
    let listener = if let Some(path) = config.listen.strip_prefix("unix://") {
        #[cfg(unix)]
        {
            let (listener, guard) = bind_unix(Path::new(path))?;
            BoundListener::Unix(listener, guard)
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(anyhow::anyhow!("Unix listeners are unavailable"));
        }
    } else {
        let address: SocketAddr = config.listen.parse().context("invalid native listener")?;
        BoundListener::Tcp(
            tokio::net::TcpListener::bind(address)
                .await
                .context("cannot bind native TCP listener")?,
        )
    };
    let setup = config.clone();
    let store = tokio::task::spawn_blocking(move || -> Result<_> {
        let store = tinytree_store::Store::open(
            &setup.storage,
            setup.store.materialize(setup.service.max_metadata_bytes),
        )
        .context("cannot open native storage")?;
        for repo in &setup.repositories {
            store
                .create_repository(repo)
                .context("cannot initialize native repository")?;
        }
        Ok(store)
    })
    .await
    .context("native startup worker failed")??;
    let index_storage = config.storage.clone();
    let query_config = config.service.query.clone();
    let index =
        tokio::task::spawn_blocking(move || derived_index::open(&index_storage, &query_config))
            .await;
    let mut service = RepositoryService::new(
        store.clone(),
        config.service,
        config.repositories.clone(),
        principals,
    )?;
    // Derived state is rebuildable. Its failure must not disable authoritative
    // object reads, durable uploads, or reference publication. Never repair by
    // silently deleting a corrupt index; queries remain explicitly unavailable.
    match index {
        Ok(Ok(index)) => match service.clone().with_query_index(index) {
            Ok(ready) => service = ready,
            Err(_) => eprintln!(
                "derived query host unavailable; canonical object service remains enabled"
            ),
        },
        _ => eprintln!("derived query index unavailable; canonical object service remains enabled"),
    }
    let (stop_sender, stop_receiver) = watch::channel(false);
    let gc_store = store.clone();
    let repositories = config.repositories;
    let gc_interval = config.gc_interval_seconds;
    let gc = tokio::spawn(async move {
        let mut stop = stop_receiver;
        if gc_interval == 0 {
            let _ = stop.changed().await;
            return;
        }
        let period = Duration::from_secs(gc_interval);
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                _=ticks.tick()=>{
                    for repo in &repositories {
                        let store=gc_store.clone();let repo=repo.clone();
                        let result=tokio::task::spawn_blocking(move||{
                            let now=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_|())?.as_secs();
                            let now=i64::try_from(now).map_err(|_|())?;
                            store.gc(&repo,now).map_err(|_|())
                        }).await;
                        if !matches!(result,Ok(Ok(_))){eprintln!("native repository GC failed; retained state requires inspection");}
                    }
                }
            }
        }
    });
    let result = match listener {
        BoundListener::Tcp(listener) => serve_tcp(listener, service, tls, shutdown).await,
        #[cfg(unix)]
        BoundListener::Unix(listener, _guard) => run_unix(listener, service, shutdown).await,
    };
    let _ = stop_sender.send(true);
    let _ = gc.await;
    result
}

enum BoundListener {
    Tcp(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener, UnixSocketGuard),
}

#[cfg(unix)]
fn bind_unix(path: &Path) -> Result<(tokio::net::UnixListener, UnixSocketGuard)> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::Component;
    ensure!(
        path.is_absolute(),
        "native Unix socket path must be absolute"
    );
    let parent = path
        .parent()
        .context("native Unix socket parent required")?;
    let uid = unsafe { libc::geteuid() };
    let mut ancestor = std::path::PathBuf::new();
    for component in parent.components() {
        ensure!(
            matches!(component, Component::RootDir | Component::Normal(_)),
            "native Unix socket path must be normalized"
        );
        ancestor.push(component.as_os_str());
        let metadata =
            std::fs::symlink_metadata(&ancestor).context("cannot inspect native socket parent")?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "native socket ancestors must be real directories"
        );
        let mode = metadata.permissions().mode();
        ensure!(
            (metadata.uid() == 0 || metadata.uid() == uid)
                && (mode & 0o022 == 0 || (metadata.uid() == 0 && mode & 0o1000 != 0)),
            "native socket ancestor is not trusted"
        );
    }
    let metadata =
        std::fs::symlink_metadata(parent).context("cannot inspect native socket parent")?;
    ensure!(
        metadata.uid() == uid && metadata.permissions().mode() & 0o077 == 0,
        "native socket parent must be owner-only"
    );
    ensure!(
        path.file_name().is_some()
            && !path
                .try_exists()
                .context("cannot inspect native socket path")?,
        "native socket endpoint already exists"
    );
    // An owner-private directory closes the pre-chmod connection/replacement window.
    let listener =
        tokio::net::UnixListener::bind(path).context("cannot bind native Unix socket")?;
    let guard = UnixSocketGuard::new(path)?;
    Ok((listener, guard))
}

fn read_identity_file(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path)
        .map_err(|_| anyhow::anyhow!("cannot read native TLS identity"))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("cannot read native TLS identity"))?;
    ensure!(
        bytes.len() <= 1024 * 1024,
        "native TLS identity exceeds byte limit"
    );
    Ok(bytes)
}

#[cfg(unix)]
struct UnixSocketGuard {
    path: std::path::PathBuf,
    device: u64,
    inode: u64,
}
#[cfg(unix)]
impl UnixSocketGuard {
    fn new(path: &Path) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::symlink_metadata(path).context("cannot inspect native socket")?;
        let guard = Self {
            path: path.into(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .context("cannot secure native socket")?;
        Ok(guard)
    }
}
#[cfg(unix)]
impl Drop for UnixSocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.dev() == self.device && metadata.ino() == self.inode {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_rejects_unsafe_transport_and_unbounded_limits() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());
        config.listen = "0.0.0.0:8093".into();
        assert!(config.validate().is_err());
        config.principals.push(PrincipalConfig {
            id: "writer".into(),
            token_env: "TINYTREE_TEST_TOKEN".into(),
            repositories: vec!["default".into()],
            write: true,
            retain: false,
            cloud: false,
            landing_profiles: Vec::new(),
        });
        assert!(config.validate().is_err());
        config.tls = Some(TlsConfig {
            certificate: "certificate.pem".into(),
            private_key: "key.pem".into(),
        });
        assert!(config.validate().is_ok());
        config.service.query.index_rebuild_generation = 1_000_000_001;
        assert!(config.validate().is_err());
        config.service.query.index_rebuild_generation = 0;
        config.service.max_frame_bytes = usize::MAX;
        assert!(config.validate().is_err());
    }
    #[test]
    fn cloud_profile_preflight_accepts_nonaligned_pool_but_rejects_insufficient_work_budget() {
        let mut config: Config = toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
        // An odd wire bound makes the worst method's byte charge non-aligned.
        // Find the public configuration admission boundary without depending on
        // the implementation's reservation arithmetic or private state.
        config.service.cloud.max_manifest_bytes += 1;
        let (mut low, mut high) = (1024 * 1024, config.service.cloud.max_buffered_bytes);
        while low < high {
            let midpoint = low + (high - low) / 2;
            config.service.cloud.max_buffered_bytes = midpoint;
            if config.validate().is_ok() {
                high = midpoint;
            } else {
                low = midpoint + 1;
            }
        }
        assert_eq!(low % 1024, 0, "the entire rounded-up reservation must fit");
        config.service.cloud.max_buffered_bytes = low - 1;
        assert!(config.validate().is_err());
        config.service.cloud.max_buffered_bytes = low + 1;
        assert!(config.validate().is_ok());
        // Opting into indexed versions must fit their larger Store-derived
        // profile, rather than silently increasing the shared pool.
        config.service.cloud.max_buffered_bytes = low + 1;
        config.service.cloud.indexed_versions = true;
        assert!(config.validate().is_err());
        config.service.cloud.max_buffered_bytes = 1024 * 1024 * 1024;
        assert!(config.validate().is_ok());
        // Cloud OP/VIEW limits do not bound native canonical metadata. A wider
        // native model requires its own proof/read reservation under this pool.
        config.service.max_metadata_bytes = 16 * 1024 * 1024;
        assert!(config.validate().is_err());
        config.service.max_metadata_bytes = 4 * 1024 * 1024;
        assert!(config.validate().is_ok());
        let absent = tempfile::tempdir().unwrap().path().join("not-created");
        config.storage = absent.clone();
        config.service.cloud.max_buffered_bytes = 1024 * 1024;
        assert!(config.validate().is_err());
        assert!(
            !absent.exists(),
            "profile preflight does not create storage"
        );
        config.service.cloud.enabled = false;
        assert!(
            config.validate().is_ok(),
            "disabled cloud does not shrink native Store policy"
        );
    }
    #[test]
    fn config_parse_errors_never_echo_file_contents() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("server.toml");
        std::fs::write(&path, "unknown_secret_field = 'not-an-actual-token'").unwrap();
        let message = Config::load(&path).err().unwrap().to_string();
        assert!(!message.contains("not-an-actual-token"));
        assert!(!message.contains("unknown_secret_field"));
    }
}
