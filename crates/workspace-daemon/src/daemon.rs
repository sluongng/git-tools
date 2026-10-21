use crate::{
    config::Config,
    control::{Request, Response},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use local_cas::{CacheConfig, LocalCas};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};

#[derive(Serialize, Deserialize)]
struct Record {
    request: agent_session::MountRequest,
    state: String,
    #[serde(default)]
    identity: Option<crate::recovery::Identity>,
}
#[derive(Default, Serialize, Deserialize)]
struct Registry {
    mounts: BTreeMap<PathBuf, Record>,
}
#[cfg(target_os = "linux")]
type Mount = workspace_fuse::host::SourceMount;
#[cfg(not(target_os = "linux"))]
struct Mount;
struct State {
    config: Config,
    identity: crate::identity::Identity,
    cache: Arc<LocalCas>,
    proxy: cache_proxy::CacheProxy,
    bes: Option<bes_proxy::BesProxy>,
    registry: Registry,
    mounts: BTreeMap<PathBuf, Mount>,
    #[cfg(target_os = "linux")]
    source_cas: Arc<workspace_core::source_cas::SourceCas>,
}

pub fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !path.exists() {
        let mut builder = std::fs::DirBuilder::new();
        use std::os::unix::fs::DirBuilderExt;
        builder.recursive(true).mode(0o700).create(path)?;
    }
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "daemon state must be a real directory"
    );
    // Lock, token and Unix socket are only safe in a private directory.
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() } && metadata.permissions().mode() & 0o077 == 0,
        "daemon state directory must be owned by current user and mode 0700"
    );
    Ok(())
}
fn persist(config: &Config, registry: &Registry) -> Result<()> {
    let target = config.state_dir.join("mounts.json");
    let temp = config.state_dir.join("mounts.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    use std::io::Write;
    file.write_all(&serde_json::to_vec_pretty(registry)?)?;
    file.sync_all()?;
    std::fs::rename(temp, target)?;
    File::open(&config.state_dir)?.sync_all()?;
    Ok(())
}
fn token(config: &Config) -> Result<String> {
    let path = config.state_dir.join("cache.token");
    if path.exists() {
        let token = std::fs::read_to_string(path)?;
        ensure!(
            token.len() == 64 && token.bytes().all(|x| x.is_ascii_hexdigit()),
            "invalid cache token file"
        );
        return Ok(token);
    }
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    Ok(token)
}
impl State {
    fn status(&self) -> serde_json::Value {
        let metrics = self.cache.metrics();
        let proxy = self.proxy.metrics();
        json!({"pid":std::process::id(),"rbe_endpoint":self.config.rbe_endpoint(),"cache_endpoint":self.config.cache_endpoint(),"bytestream_uri_prefix":self.config.bytestream_uri_prefix(),"instance_name":self.config.cache.instance_name,"bes_endpoint":self.bes.as_ref().map(|_|self.config.bes_endpoint()),"bes":self.bes.as_ref().map(|bes|bes.metrics()),"bos_endpoint":if self.config.bos.enabled {Some(self.config.rbe_endpoint())}else{None},"output_path_prefix":self.config.state_dir.join("outputs"),"mounts":self.registry.mounts.iter().map(|(path,record)|json!({"session":path,"state":record.state})).collect::<Vec<_>>(),"proxy":{"action_cache_hits":proxy.action_cache_hits,"action_cache_upstream":proxy.action_cache_upstream,"blob_reads":proxy.blob_reads,"asset_cache_hits":proxy.asset_cache_hits,"asset_upstream_requests":proxy.asset_upstream_requests,"execution_requests":proxy.execution_requests,"execution_waits":proxy.execution_waits,"retention":{"action_refreshes":proxy.retention.action_refreshes,"output_refreshes":proxy.retention.output_refreshes,"blob_refreshes":proxy.retention.blob_refreshes,"blob_repairs":proxy.retention.blob_repairs,"blob_repair_failures":proxy.retention.blob_repair_failures,"failures":proxy.retention.failures,"missing_blobs":proxy.retention.missing_blobs,"dropped":proxy.retention.dropped}},"cache":{"hits":metrics.hits,"fetches":metrics.fetches,"resident_bytes":metrics.resident_bytes,"reserved_bytes":metrics.reserved_bytes,"evictions":metrics.evictions,"evicted_bytes":metrics.evicted_bytes}})
    }
    fn mounted(&self, path: &Path) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.mounts
                .get(path)
                .is_some_and(|mount| !mount.is_finished())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            false
        }
    }
    fn mount(&mut self, request: agent_session::MountRequest) -> Result<()> {
        ensure!(
            request.session.is_absolute() && request.session.canonicalize()? == request.session,
            "session must be an existing canonical absolute directory"
        );
        ensure!(
            request.repo == request.session.join("repo.git")
                && request.overlay == request.session.join("overlay")
                && request.mountpoint == request.session.join("workspace"),
            "mount paths must belong to the private session"
        );
        private_dir(&request.session)?;
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(request.session.join("session.json"))?)?;
        ensure!(
            manifest["daemon_managed"] == true
                && manifest["session_root"].as_str() == request.session.to_str()
                && manifest["settings"]["revision"].as_str() == Some(request.revision.as_str()),
            "mount request does not match the daemon-managed session manifest"
        );
        for path in [&request.repo, &request.mountpoint, &request.overlay] {
            if path.exists() {
                ensure!(
                    !std::fs::symlink_metadata(path)?.file_type().is_symlink(),
                    "session mount paths must not be symlinks"
                );
                ensure!(
                    path.canonicalize()?.starts_with(&request.session),
                    "canonical mount path escaped private session"
                );
            }
        }
        ensure!(
            !request.session.starts_with(&self.config.state_dir)
                && !self.config.state_dir.starts_with(&request.session),
            "session and disposable daemon cache/state roots must not overlap"
        );
        ensure!(
            !self.mounts.contains_key(&request.session),
            "session already owned; stop before resume"
        );
        self.registry.mounts.insert(
            request.session.clone(),
            Record {
                request: request.clone(),
                state: "mounting".into(),
                identity: None,
            },
        );
        persist(&self.config, &self.registry)?;
        let result = self.mount_inner(&request);
        match result {
            Ok(mount) => {
                self.mounts.insert(request.session.clone(), mount);
                self.registry
                    .mounts
                    .get_mut(&request.session)
                    .unwrap()
                    .state = "mounted".into();
                self.registry
                    .mounts
                    .get_mut(&request.session)
                    .unwrap()
                    .identity = crate::recovery::identity(&request.mountpoint)?;
                persist(&self.config, &self.registry)?;
                Ok(())
            }
            Err(error) => {
                self.registry
                    .mounts
                    .get_mut(&request.session)
                    .unwrap()
                    .state = "recovery_required".into();
                persist(&self.config, &self.registry)?;
                Err(error)
            }
        }
    }
    #[cfg(target_os = "linux")]
    fn mount_inner(&self, request: &agent_session::MountRequest) -> Result<Mount> {
        use workspace_fuse::filesystem::Snapshot;
        use workspace_fuse::{
            backend::{ContentConfig, GitRepo, SizeFallback, SizeWarmOptions},
            host::{MountConfig, SourceMount},
            prefetch::BazelPrefetchPolicy,
        };
        let started = std::time::Instant::now();
        ensure!(
            (1..=86400).contains(&request.timeout_seconds),
            "mount timeout must be 1..86400 seconds"
        );
        let deadline = Duration::from_secs(request.timeout_seconds);
        let mut repo = GitRepo::open(
            &request.repo,
            request.git.clone(),
            "origin",
            SizeFallback::Error,
        )
        .context("opening source repository")?;
        repo.set_shared_cas(self.source_cas.clone());
        let snapshot =
            Snapshot::load(&repo, &request.revision).context("loading pinned source snapshot")?;
        repo.warm_blake3_snapshot(&snapshot, &request.proxy_url, &request.proxy_repo)
            .context("importing source BLAKE3 mappings")?;
        ensure!(
            started.elapsed() < deadline,
            "mount preparation timed out; retry resume after cache warming"
        );
        repo.warm_snapshot_sizes(&snapshot, &SizeWarmOptions::default())
            .context("warming source blob sizes")?;
        ensure!(
            started.elapsed() < deadline,
            "mount preparation timed out; retry resume after cache warming"
        );
        SourceMount::start(
            repo,
            snapshot,
            MountConfig {
                mountpoint: request.mountpoint.clone(),
                read_only: false,
                overlay: Some(request.overlay.clone()),
                prefetch: Some(BazelPrefetchPolicy::new(&[])?),
                blake3: true,
                content: ContentConfig {
                    large_blob_spool_bytes: self.config.source.large_blob_spool_bytes,
                    ..ContentConfig::default()
                },
                git_vfs_executable: std::env::current_exe()?,
            },
        )
        .context("starting source mount host")
    }
    #[cfg(not(target_os = "linux"))]
    fn mount_inner(&self, _: &agent_session::MountRequest) -> Result<Mount> {
        anyhow::bail!("source mount host not yet supported on this platform")
    }
    fn recover(&mut self, path: &Path) -> Result<serde_json::Value> {
        ensure!(
            !self.mounts.contains_key(path),
            "session has a live owned mount; use workspace stop, never recover"
        );
        let record = self
            .registry
            .mounts
            .get(path)
            .context("session is not in this daemon's persisted registry")?;
        ensure!(
            matches!(record.state.as_str(), "recovery_required" | "stopped"),
            "session is not marked for crash recovery; use workspace stop"
        );
        validate_recovery(&self.config, path, record)?;
        let detached = crate::recovery::detach(
            &record.request.mountpoint,
            record.identity.as_ref(),
            "git-vfs",
        )?;
        self.registry.mounts.get_mut(path).unwrap().state = "stopped".into();
        persist(&self.config, &self.registry)?;
        Ok(
            json!({"session":path,"detached":detached,"state":"stopped","next":"workspace resume SESSION"}),
        )
    }
    fn unmount(&mut self, path: &Path) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(mount) = self.mounts.get_mut(path) {
            mount.shutdown()?;
        }
        self.mounts.remove(path);
        if let Some(record) = self.registry.mounts.get_mut(path) {
            record.state = "stopped".into();
            persist(&self.config, &self.registry)?;
        }
        Ok(())
    }
}

fn validate_recovery(config: &Config, path: &Path, record: &Record) -> Result<()> {
    let request = &record.request;
    ensure!(
        path.is_absolute() && path.canonicalize()? == path && request.session == path,
        "recovery requires the original canonical session path"
    );
    private_dir(path)?;
    ensure!(
        request.repo == path.join("repo.git")
            && request.overlay == path.join("overlay")
            && request.mountpoint == path.join("workspace"),
        "persisted mount paths do not belong to the session"
    );
    ensure!(
        !path.starts_with(&config.state_dir) && !config.state_dir.starts_with(path),
        "session and daemon state must not overlap"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("session.json"))?)?;
    ensure!(
        manifest["daemon_managed"] == true
            && manifest["session_root"].as_str() == path.to_str()
            && manifest["settings"]["revision"].as_str() == Some(request.revision.as_str()),
        "session manifest no longer matches the persisted mount request"
    );
    for child in [&request.repo, &request.overlay] {
        ensure!(
            !std::fs::symlink_metadata(child)?.file_type().is_symlink()
                && child.canonicalize()?.starts_with(path),
            "authoritative session paths must not be replaced by symlinks"
        );
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct BosRecord {
    mountpoint: PathBuf,
    identity: crate::recovery::Identity,
}
fn persist_bos(config: &Config, identity: crate::recovery::Identity) -> Result<()> {
    let path = config.state_dir.join("bos-mount.json");
    let temp = config.state_dir.join("bos-mount.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    use std::io::Write;
    file.write_all(&serde_json::to_vec(&BosRecord {
        mountpoint: config.state_dir.join("outputs"),
        identity,
    })?)?;
    file.sync_all()?;
    std::fs::rename(temp, path)?;
    File::open(&config.state_dir)?.sync_all()?;
    Ok(())
}
/// Offline repair never races a running daemon: its exclusive lock is mandatory.
pub fn recover_offline(mut config: Config, session: Option<&Path>) -> Result<serde_json::Value> {
    config.validate()?;
    private_dir(&config.state_dir)?;
    config.state_dir = config.state_dir.canonicalize()?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(config.state_dir.join("daemon.lock"))?;
    lock.try_lock_exclusive().context(
        "daemon is still alive or starting; stop it normally, do not recover live mounts",
    )?;
    let mut registry: Registry =
        serde_json::from_slice(&std::fs::read(config.state_dir.join("mounts.json"))?)?;
    if let Some(session) = session {
        let record = registry
            .mounts
            .get(session)
            .context("session is not in this daemon's persisted registry")?;
        validate_recovery(&config, session, record)?;
    }
    let output = config.state_dir.join("outputs");
    let bos_detached = if crate::recovery::identity(&output)?.is_some() {
        let saved: BosRecord = serde_json::from_slice(
            &std::fs::read(config.state_dir.join("bos-mount.json"))
                .context("no persisted BOS identity; inspect and unmount manually")?,
        )?;
        ensure!(
            saved.mountpoint == output,
            "persisted BOS mountpoint does not match private state"
        );
        crate::recovery::detach(&output, Some(&saved.identity), "git-tools-outputs")?
    } else {
        false
    };
    let detached = if let Some(session) = session {
        let record = registry.mounts.get(session).unwrap();
        let detached = crate::recovery::detach(
            &record.request.mountpoint,
            record.identity.as_ref(),
            "git-vfs",
        )?;
        registry.mounts.get_mut(session).unwrap().state = "stopped".into();
        detached
    } else {
        false
    };
    persist(&config, &registry)?;
    let _ = std::fs::remove_file(config.state_dir.join("bos-mount.json"));
    drop(lock);
    Ok(
        json!({"session":session,"detached":detached,"bos_detached":bos_detached,"state":"stopped","next":"workspace resume SESSION"}),
    )
}

// Metadata clients keep the capability prefix in-process; native Git uses a
// plain origin plus an extraHeader stored in private config/environment, so
// remote-http helper arguments cannot disclose the capability to `ps`.
fn protected_git_routes(router: axum::Router, token: &str) -> axum::Router {
    use axum::{
        extract::{Request, State},
        http::{StatusCode, header},
        middleware::{self, Next},
        response::{IntoResponse, Response},
    };
    async fn authorize(
        State(expected): State<Arc<String>>,
        request: Request,
        next: Next,
    ) -> Response {
        let valid = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                if value.len() != expected.len() {
                    return false;
                }
                value
                    .bytes()
                    .zip(expected.bytes())
                    .fold(0u8, |different, (left, right)| different | (left ^ right))
                    == 0
            });
        if !valid {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        next.run(request).await
    }
    let authorized = router.clone().layer(middleware::from_fn_with_state(
        Arc::new(format!("Bearer {token}")),
        authorize,
    ));
    axum::Router::new()
        .nest(&format!("/{token}"), router)
        .merge(authorized)
}

pub async fn serve(mut config: Config) -> Result<()> {
    config.validate()?;
    private_dir(&config.state_dir)?;
    config.state_dir = config.state_dir.canonicalize()?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(config.state_dir.join("daemon.lock"))?;
    lock.try_lock_exclusive()
        .context("another workspace daemon owns this state directory")?;
    let credentials = crate::identity::Credentials::load(&config)?;
    let identity = crate::identity::Identity::new(&config, &credentials)?;
    let token = token(&config)?;
    let mut registry: Registry = match std::fs::read(config.state_dir.join("mounts.json")) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).context("invalid mount registry; state retained")?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Registry::default(),
        Err(error) => return Err(error.into()),
    };
    for record in registry.mounts.values_mut() {
        if record.state != "stopped" {
            record.state = "recovery_required".into();
        }
    }
    persist(&config, &registry)?;
    let socket = config.socket();
    if socket.exists() {
        let metadata = std::fs::symlink_metadata(&socket)?;
        use std::os::unix::fs::FileTypeExt;
        ensure!(
            metadata.file_type().is_socket(),
            "control path is not a socket"
        );
        ensure!(
            std::os::unix::net::UnixStream::connect(&socket).is_err(),
            "control socket is live"
        );
        std::fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let api_key = credentials.cache.clone();
    let service_config = || bazel_output_service::Config {
        root: config.state_dir.join("outputs"),
        cas_endpoint: config.cache.upstream.clone(),
        instance_name: config.cache.instance_name.clone(),
        api_key: api_key.clone(),
        max_blob_bytes: config.cache.max_blob_bytes,
        policy: config.reapi.clone(),
    };
    let identity_service = bazel_output_service::OutputService::new(service_config()).await?;
    let backend_identity = identity_service.backend_identity();
    drop(identity_service);
    let upstream_headers = api_key
        .as_ref()
        .map(|key| vec![("x-buildbuddy-api-key".into(), key.clone())])
        .unwrap_or_default();
    let upstream = cache_proxy::Upstream::connect(cache_proxy::ProxyConfig {
        upstream: config.cache.upstream.clone(),
        backend_identity: backend_identity.clone(),
        instance_name: config.cache.instance_name.clone(),
        upstream_headers,
        policy: config.reapi.clone(),
        max_blob_bytes: config.cache.max_blob_bytes,
        max_batch_bytes: 4 * 1024 * 1024,
        action_cache_ttl: Duration::from_secs(config.cache.action_cache_ttl_seconds),
        max_action_cache_entries: config.cache.max_action_cache_entries,
        upstream_retention: Duration::from_secs(config.cache.upstream_retention_seconds),
        retention_refresh_interval: Duration::from_secs(config.cache.retention_refresh_seconds),
        retention_max_entries: config.cache.retention_max_entries,
        asset_cache_ttl: Duration::from_secs(config.cache.asset_cache_ttl_seconds),
        max_asset_cache_entries: config.cache.max_asset_cache_entries,
        max_asset_cache_bytes: config.cache.max_asset_cache_bytes,
        max_execution_streams: config.cache.max_execution_streams,
        execution_timeout: (config.cache.execution_timeout_ms > 0)
            .then(|| Duration::from_millis(config.cache.execution_timeout_ms)),
        max_concurrent_rpcs_per_connection: config.cache.max_concurrent_rpcs_per_connection,
        local_bearer_token: None,
    })
    .await?;
    let cache = LocalCas::open(
        config.state_dir.join("cas"),
        upstream.clone(),
        CacheConfig {
            max_bytes: config.cache.max_bytes,
            max_concurrent_fetches: config.cache.max_concurrent_fetches,
            max_inflight_bytes: config.cache.max_inflight_bytes,
            operation_timeout: Duration::from_millis(config.reapi.operation_timeout_ms),
            fetch_timeout: Duration::from_millis(config.reapi.operation_timeout_ms),
            ..Default::default()
        },
    )?;
    let proxy = cache_proxy::CacheProxy::new(upstream, cache.clone());
    let mut rbe_router = proxy.clone().unix_router();
    let bes = if config.bes.enabled {
        let settings = &config.bes;
        let bes_key = credentials.bes.clone();
        let spool = config.state_dir.join("bes");
        private_dir(&spool)?;
        let bes = bes_proxy::BesProxy::open(
            &spool,
            bes_proxy::Config {
                upstream: if settings.upstream.is_empty() {
                    config.cache.upstream.clone()
                } else {
                    settings.upstream.clone()
                },
                api_key: bes_key,
                max_pending_bytes: settings.max_pending_bytes,
                max_event_bytes: settings.max_event_bytes,
                max_streams: settings.max_streams,
                max_concurrent_forwards: settings.max_concurrent_forwards,
                connect_timeout_ms: settings.connect_timeout_ms,
                lifecycle_timeout_ms: settings.lifecycle_timeout_ms,
                post_half_close_timeout_ms: settings.post_half_close_timeout_ms,
                retry_initial_delay_ms: settings.retry_initial_delay_ms,
                retry_max_attempts: settings.retry_max_attempts,
                tombstone_ttl_seconds: settings.tombstone_ttl_seconds,
                max_completed_streams: settings.max_completed_streams,
                batch_max_events: settings.batch_max_events,
                batch_max_bytes: settings.batch_max_bytes,
                batch_max_delay_ms: settings.batch_max_delay_ms,
            },
        )
        .await?;
        rbe_router = rbe_router.add_service(
            reapi::bes::publish_build_event_server::PublishBuildEventServer::new(bes.clone())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(settings.max_event_bytes)
                .max_encoding_message_size(settings.max_event_bytes),
        );
        Some(bes)
    } else {
        None
    };
    let mut git_task = None;
    let mut git_shutdown = None;
    let mut git_cas_shutdown = None;
    if !config.git_proxy.repositories.is_empty() {
        let cas_sync = if config.git_proxy.sync_to_cas {
            Some(git_proxy::CasSyncConfig {
                endpoint: config.cache.upstream.clone(),
                instance_name: config.cache.instance_name.clone(),
                header_env: if api_key.is_some() {
                    vec![(
                        "x-buildbuddy-api-key".into(),
                        config.cache.api_key_env.clone(),
                    )]
                } else {
                    vec![]
                },
                policy: config.reapi.clone(),
                staging_limit_bytes: 64 * 1024 * 1024,
                cache_max_bytes: config.cache.max_bytes,
            })
        } else {
            None
        };
        let (router, cas_shutdown) = git_proxy::router_with_shared_cache(
            git_proxy::Config {
                cache_dir: config.state_dir.join("git"),
                git: config.session.git.clone(),
                repositories: config
                    .git_proxy
                    .repositories
                    .iter()
                    .map(|(a, b)| (a.clone(), b.clone()))
                    .collect(),
                refresh_ttl: Duration::from_secs(60),
                enable_blake3: true,
                pack_cache: Default::default(),
                cas_sync,
            },
            cache.clone(),
            backend_identity.clone(),
        )?;
        let router = protected_git_routes(router, &token);
        let listener = tokio::net::TcpListener::bind(config.git_proxy.listen).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        git_task = Some(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
        }));
        git_shutdown = Some(tx);
        git_cas_shutdown = cas_shutdown;
    }
    let mut output_host = None;
    if config.bos.enabled {
        use bazel_output_service::{
            host::{HostConfig, LazyStorage, OutputHost},
            proto::bazel_output_service_server::BazelOutputServiceServer,
        };
        let host = OutputHost::start(
            HostConfig {
                service: service_config(),
                state_dir: config.state_dir.join("output-state"),
                storage: LazyStorage::Native,
                client_cache_endpoint: Some(config.cache_endpoint()),
            },
            cache.clone(),
        )
        .await?;
        #[cfg(target_os = "linux")]
        persist_bos(
            &config,
            crate::recovery::identity(&config.state_dir.join("outputs"))?
                .context("BOS mount identity missing")?,
        )?;
        rbe_router = rbe_router.add_service(
            BazelOutputServiceServer::new(host.service())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip),
        );
        output_host = Some(host);
    }
    let cache_socket = config.state_dir.join("rbe.sock");
    if cache_socket.exists() {
        use std::os::unix::fs::FileTypeExt;
        ensure!(
            std::fs::symlink_metadata(&cache_socket)?
                .file_type()
                .is_socket(),
            "cache transport path is not a socket"
        );
        std::fs::remove_file(&cache_socket)?;
    }
    let cache_listener = UnixListener::bind(&cache_socket)?;
    std::fs::set_permissions(&cache_socket, std::fs::Permissions::from_mode(0o600))?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut proxy_task = tokio::spawn(async move {
        rbe_router
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(cache_listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .map_err(anyhow::Error::from)
    });
    #[cfg(target_os = "linux")]
    let source_cas = workspace_core::source_cas::SourceCas::new(
        cache.clone(),
        backend_identity,
        config.cache.instance_name.clone(),
    )?;
    let state = State {
        config,
        identity,
        cache,
        proxy: proxy.clone(),
        bes,
        registry,
        mounts: BTreeMap::new(),
        #[cfg(target_os = "linux")]
        source_cas,
    };

    let mut state_slot = Some(state);
    let mut proxy_completed = false;
    let run: Result<()> = async {
        loop {
            let stream = tokio::select! {
                result = listener.accept() => result?.0,
                _ = tokio::signal::ctrl_c() => {
                    if !state_slot.as_ref().unwrap().mounts.is_empty(){eprintln!("stop active sessions before daemon shutdown");continue;}
                    if let Some(host)=output_host.as_ref(){if let Err(error)=host.shutdown().await{eprintln!("output mount is busy; daemon remains running: {error}");continue;}}
                    output_host=None;break;
                },
                result = &mut proxy_task => { proxy_completed=true; result??; anyhow::bail!("cache proxy stopped unexpectedly") }

            };
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut bytes = Vec::new();
            use tokio::io::AsyncReadExt;
            let read_result = tokio::time::timeout(Duration::from_secs(10), (&mut reader).take(65537).read_until(b'\n', &mut bytes)).await;
            let request = match read_result {
                Ok(Ok(_)) if bytes.len() <= 65536 => serde_json::from_slice::<Request>(&bytes).map_err(anyhow::Error::from),
                _ => Err(anyhow::anyhow!("invalid or oversized control request")),
            };
            // Validate and execute against this same owned state. A client that
            // connected to a replacement daemon cannot reuse an old proof.
            let request = match request {
                Ok(Request::Scoped { version, digest, request }) => {
                    state_slot.as_ref().unwrap().identity.validate(version, &digest)
                        .map(|()| *request)
                }
                other => other,
            };
            let mut shutdown = false;
            // Native Git, reqwest::blocking metadata and FUSE lifecycle work
            // must never execute inside the daemon's async runtime. The state
            // is owned by exactly one blocking operation, retaining handles on
            // client disconnect rather than dropping a live mount operation.
            let result = match request {
                Ok(Request::Mount { request }) => {
                    let mut state=state_slot.take().expect("owned daemon state");
                    let (returned, result) = tokio::task::spawn_blocking(move || {
                        let session=request.session.clone();
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.mount(request)))
                            .unwrap_or_else(|_| {if let Some(record)=state.registry.mounts.get_mut(&session){record.state="recovery_required".into();}let _=persist(&state.config,&state.registry);Err(anyhow::anyhow!("mount host operation panicked; other sessions retained"))});
                        (state, result)
                    }).await.context("mount host worker panicked")?;
                    state_slot = Some(returned);
                    result.map(|()| json!(true))
                }
                Ok(Request::Unmount { session }) => {
                    let mut state=state_slot.take().expect("owned daemon state");
                    let (returned, result) = tokio::task::spawn_blocking(move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.unmount(&session)))
                            .unwrap_or_else(|_| Err(anyhow::anyhow!("unmount host operation panicked; ownership retained")));
                        (state, result)
                    }).await.context("unmount host worker panicked")?;
                    state_slot = Some(returned);
                    result.map(|()| json!(true))
                }
                Ok(Request::Recover{session})=>{
                    let mut state=state_slot.take().expect("owned daemon state");
                    let(returned,result)=tokio::task::spawn_blocking(move||{let result=match session{Some(session)=>state.recover(&session),None=>Err(anyhow::anyhow!("daemon is live; use workspace shutdown, not recovery, for its output mount"))};(state,result)}).await.context("recovery worker panicked")?;
                    state_slot=Some(returned);result
                }
                Ok(Request::IdentityChallenge) => serde_json::to_value(state_slot.as_ref().unwrap().identity.challenge()).map_err(Into::into),
                Ok(Request::Scoped { .. }) => Err(anyhow::anyhow!("nested scoped request is invalid")),
                Ok(Request::Status) => Ok(state_slot.as_ref().unwrap().status()),
                Ok(Request::RetryBes) => match state_slot.as_ref().unwrap().bes.as_ref() {
                    Some(bes) => bes.retry_blocked().await.map(|count| json!({"streams_retried":count})),
                    None => Err(anyhow::anyhow!("BES proxy is disabled")),
                },
                Ok(Request::Endpoints) => {let state=state_slot.as_ref().unwrap();Ok(json!({"git_proxy_url": if state.config.git_proxy.repositories.is_empty() {None} else {Some(format!("http://{}/{token}", state.config.git_proxy.listen))}}))},
                Ok(Request::Mounted { session }) => Ok(json!(state_slot.as_ref().unwrap().mounted(&session))),
                Ok(Request::Shutdown) => {
                    if !state_slot.as_ref().unwrap().mounts.is_empty() {
                        Err(anyhow::anyhow!("stop active sessions before daemon shutdown"))
                    } else if let Some(host) = output_host.as_ref() {
                        match host.shutdown().await {
                            Ok(()) => {output_host = None; shutdown = true; Ok(json!(true))}
                            Err(error) => Err(error.context("output mount is busy; daemon remains running")),
                        }
                    } else {
                        shutdown = true;
                        Ok(json!(true))
                    }
                }
                Err(error) => Err(error),
            };
            let response = match result {
                Ok(result) => Response {ok:true,result,error:None},
                Err(error) => Response {ok:false,result:serde_json::Value::Null,error:Some(error.to_string().replace(&token,"[redacted]"))},
            };
            let mut bytes=serde_json::to_vec(&response)?; bytes.push(b'\n');
            let _=write.write_all(&bytes).await;
            if shutdown {break;}
        }
        Ok(())
    }.await;
    let mut state =
        state_slot.context("mount worker lost daemon state; registry retained for recovery")?;
    for path in state.mounts.keys().cloned().collect::<Vec<_>>() {
        if let Err(error) = state.unmount(&path) {
            eprintln!("mount shutdown failed for {}: {error}", path.display());
        }
    }
    if let Some(tx) = git_shutdown {
        let _ = tx.send(());
    }
    if let Some(task) = git_task {
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    }
    if let Some(host) = output_host {
        host.shutdown().await?;
    }
    state.proxy.stop_forwarding();
    state.proxy.wait_for_maintenance_shutdown().await;
    let _ = shutdown_tx.send(());
    if let Some(bes) = &state.bes {
        bes.shutdown().await;
    }
    if !proxy_completed
        && tokio::time::timeout(Duration::from_secs(10), &mut proxy_task)
            .await
            .is_err()
    {
        proxy_task.abort();
        let _ = proxy_task.await;
    }
    if let Some(shutdown) = git_cas_shutdown {
        shutdown.shutdown().await;
    }
    if crate::recovery::identity(&state.config.state_dir.join("outputs"))?.is_none() {
        let _ = std::fs::remove_file(state.config.state_dir.join("bos-mount.json"));
    }
    state.cache.wait_for_idle().await?;
    state.cache.flush_recency()?;
    let _ = std::fs::remove_file(socket);
    let _ = std::fs::remove_file(cache_socket);
    drop(lock);
    run
}
