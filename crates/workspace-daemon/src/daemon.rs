use crate::{
    config::Config,
    control::{Request, Response},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use local_cas::{CacheConfig, LocalCas};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::os::unix::fs::OpenOptionsExt;
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
    startup_limits: crate::runtime::StartupLimits,
    control_accept: reapi::server_io::AcceptState,
    identity: crate::identity::Identity,
    cache: Arc<LocalCas>,
    proxy: cache_proxy::CacheProxy,
    bes: Option<bes_proxy::BesProxy>,
    native: Option<native_broker::BrokerService>,
    native_workspaces: Option<crate::native_workspaces::Manager>,
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
        json!({"pid":std::process::id(),"runtime":{"open_files":self.startup_limits,"control_accept_resource_exhaustions":self.control_accept.resource_exhaustions()},"rbe_endpoint":self.config.rbe_endpoint(),"cache_endpoint":self.config.cache_endpoint(),"bytestream_uri_prefix":self.config.bytestream_uri_prefix(),"instance_name":self.config.cache.instance_name,"bes_endpoint":self.bes.as_ref().map(|_|self.config.bes_endpoint()),"bes":self.bes.as_ref().map(|bes|bes.metrics()),"native":{"endpoint":self.native.as_ref().map(|_|self.config.rbe_endpoint()),"repositories":self.config.native.repositories.keys().collect::<Vec<_>>(),"workspaces":self.native_workspaces.as_ref().map(|manager|manager.status()),"commands":self.native_workspaces.as_ref().map(|manager|manager.command_status())},"bos_endpoint":if self.config.bos.enabled {Some(self.config.rbe_endpoint())}else{None},"output_path_prefix":self.config.state_dir.join("outputs"),"mounts":self.registry.mounts.iter().map(|(path,record)|json!({"session":path,"state":record.state})).collect::<Vec<_>>(),"proxy":{"download_failures":proxy.download_failures,"serving_read_failures":proxy.serving_read_failures,"accept_resource_exhaustions":proxy.accept_resource_exhaustions,"action_cache_hits":proxy.action_cache_hits,"action_cache_upstream":proxy.action_cache_upstream,"blob_reads":proxy.blob_reads,"read_slots_in_use":proxy.read_slots_in_use,"read_slots_queued":proxy.read_slots_queued,"asset_cache_hits":proxy.asset_cache_hits,"asset_upstream_requests":proxy.asset_upstream_requests,"execution_requests":proxy.execution_requests,"execution_waits":proxy.execution_waits,"retention":{"action_refreshes":proxy.retention.action_refreshes,"output_refreshes":proxy.retention.output_refreshes,"blob_refreshes":proxy.retention.blob_refreshes,"blob_repairs":proxy.retention.blob_repairs,"blob_repair_failures":proxy.retention.blob_repair_failures,"failures":proxy.retention.failures,"missing_blobs":proxy.retention.missing_blobs,"dropped":proxy.retention.dropped}},"cache":metrics})
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
#[cfg(target_os = "linux")]
fn existing_daemon_owner_lock(directory: &private_fs::PrivateDir) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let path = directory.path().join("daemon.lock");
    let owner = std::fs::metadata(directory.path())?.uid();
    let valid = |metadata: &std::fs::Metadata| {
        metadata.is_file()
            && metadata.uid() == owner
            && metadata.mode() & 0o022 == 0
            && metadata.nlink() == 1
    };
    let before = std::fs::symlink_metadata(&path)?;
    ensure!(valid(&before), "untrusted existing daemon owner lock");
    // Legacy daemons created a 0644 lock under their 0700 state directory.
    // Linux flock works on this readonly descriptor; never chmod or replace it.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&path)?;
    let opened = file.metadata()?;
    let after = std::fs::symlink_metadata(path)?;
    ensure!(
        valid(&opened)
            && valid(&after)
            && before.dev() == opened.dev()
            && before.ino() == opened.ino()
            && opened.dev() == after.dev()
            && opened.ino() == after.ino(),
        "daemon owner lock changed while opening"
    );
    Ok(file)
}

/// Native recovery uses a private opaque ID and never selects through a dead
/// FUSE pathname. Offline execution must hold the same daemon owner lock as
/// normal startup, and cannot initialize missing state or contact providers.
pub fn recover_native_offline(mut config: Config, workspace_id: &str) -> Result<serde_json::Value> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (config, workspace_id);
        anyhow::bail!("native disconnected-mount recovery currently requires Linux")
    }
    #[cfg(target_os = "linux")]
    {
        config.validate()?;
        let directory = private_fs::PrivateDir::open_existing(&config.state_dir)?;
        config.state_dir = directory.path().canonicalize()?;
        let lock = existing_daemon_owner_lock(&directory)?;
        lock.try_lock_exclusive().context(
            "daemon is still alive or starting; use its private recovery command, never offline recovery")?;
        crate::native_workspaces::recover_offline(&config, workspace_id)
    }
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
        .mode(0o600)
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

// The daemon accepts gRPC aliases, while the Git CAS client deliberately takes
// HTTP(S) origins. Translate only the scheme at this integration boundary.
fn git_cas_endpoint(upstream: &str) -> Result<String> {
    let endpoint = if let Some(authority) = upstream.strip_prefix("grpcs://") {
        format!("https://{authority}")
    } else if let Some(authority) = upstream.strip_prefix("grpc://") {
        format!("http://{authority}")
    } else if upstream.starts_with("https://") || upstream.starts_with("http://") {
        upstream.to_owned()
    } else {
        anyhow::bail!("Git CAS upstream requires an explicit gRPC or HTTP(S) origin");
    };
    let uri: axum::http::Uri = endpoint
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Git CAS upstream origin"))?;
    ensure!(
        uri.authority().is_some_and(|a| !a.as_str().contains('@'))
            && uri.path() == "/"
            && uri.query().is_none(),
        "Git CAS upstream must be an origin without credentials, path or query"
    );
    Ok(endpoint)
}

/// Open each configured native store exactly once under daemon ownership.
/// A partial/unrecognized existing directory is never guessed or reinitialized.
#[cfg(test)]
fn open_native_repositories(
    config: &Config,
    cache: &Arc<LocalCas>,
    native_ca: &BTreeMap<String, Arc<[u8]>>,
) -> Result<Option<native_broker::BrokerService>> {
    Ok(open_native_host(config, cache, native_ca, &BTreeMap::new(), None)?.0)
}
fn open_native_host(
    config: &Config,
    cache: &Arc<LocalCas>,
    native_ca: &BTreeMap<String, Arc<[u8]>>,
    native_credentials: &BTreeMap<String, Option<String>>,
    command_host: Option<Arc<crate::native_commands::NativeCommandHost>>,
) -> Result<(
    Option<native_broker::BrokerService>,
    BTreeMap<String, crate::native_workspaces::Repository>,
)> {
    if config.native.repositories.is_empty() {
        return Ok((None, BTreeMap::new()));
    }

    let root = config.state_dir.join("native");
    private_dir(&root)?;
    // Persist directory names as well as the files subsequently acknowledged
    // by the authoritative store. Existing ancestors are not chmodded/changed.
    for ancestor in root.ancestors() {
        File::open(ancestor)?.sync_all()?;
    }
    let mut stores: std::collections::HashMap<String, Arc<dyn jj_native_backend::NativeObjectIo>> =
        std::collections::HashMap::new();
    let mut controls: std::collections::HashMap<
        String,
        Arc<dyn native_broker::NativeRepositoryControl>,
    > = std::collections::HashMap::new();

    let mut queries: std::collections::HashMap<String, Arc<dyn native_broker::NativeQueryIo>> =
        std::collections::HashMap::new();

    let mut retained = BTreeMap::new();
    for (alias, settings) in &config.native.repositories {
        let path = root.join(alias);
        private_dir(&path)?;
        File::open(&path)?.sync_all()?;
        File::open(&root)?.sync_all()?;
        let fresh = std::fs::read_dir(&path)?.next().is_none();
        let provider = if fresh {
            jj_native_backend::DirectObjectIo::init(&path, settings.materialize())
        } else {
            jj_native_backend::DirectObjectIo::load(&path, settings.materialize())
        }
        .map_err(|_| anyhow::anyhow!("cannot open configured native repository; state retained"))?;

        let provider = Arc::new(provider);
        if let Some(remote_config) = &settings.remote {
            let remote_root = config.state_dir.join("native-remote");
            private_dir(&remote_root)?;
            let remote_root = remote_root.join(alias);
            private_dir(&remote_root)?;
            File::open(remote_root.parent().unwrap())?.sync_all()?;
            File::open(&config.state_dir)?.sync_all()?;

            let remote = jj_native_remote::RemoteRepository::open_with_ca_certificate(
                provider,
                cache.clone(),
                &remote_root.join("ledger"),
                &remote_root.join("staging"),
                remote_config.clone(),
                native_ca.get(alias).cloned(),
            )
            .map_err(|_| anyhow::anyhow!("cannot open configured native remote; state retained"))?;

            queries.insert(alias.clone(), remote.query);
            stores.insert(alias.clone(), remote.io);
            controls.insert(alias.clone(), Arc::new(remote.control));
        } else {
            stores.insert(alias.clone(), provider);
        }
    }

    let mut descriptions = std::collections::HashMap::new();
    let mut scopes = BTreeMap::new();
    for (alias, settings) in &config.native.repositories {
        let store_path = root.join(alias).canonicalize()?;
        let mut fingerprint = blake3::Hasher::new();
        fingerprint.update(b"native-workspace-provider-scope-v1\0");
        // Length-delimited private material is hashed in memory only. Neither
        // credential values nor this material are recorded in public output.
        for bytes in [
            serde_json::to_vec(settings)?,
            serde_json::to_vec(&store_path)?,
            serde_json::to_vec(&config.reapi)?,
            serde_json::to_vec(&native_credentials.get(alias))?,
            native_ca
                .get(alias)
                .map_or_else(Vec::new, |bytes| bytes.to_vec()),
        ] {
            fingerprint.update(&(bytes.len() as u64).to_le_bytes());
            fingerprint.update(&bytes);
        }
        let identity = *fingerprint.finalize().as_bytes();
        let scope = native_workspace::Scope::from_configured_identity(alias, &identity)?;
        scopes.insert(alias.clone(), scope);
        descriptions.insert(
            alias.clone(),
            native_broker::ProviderDescription {
                identity,
                query_capable: queries.contains_key(alias),
                index_mode: match config.native.workspaces.index_mode {
                    crate::config::NativeIndexMode::Legacy => {
                        native_broker::ProviderIndexMode::Legacy
                    }
                    crate::config::NativeIndexMode::Federated => {
                        native_broker::ProviderIndexMode::Federated
                    }
                },
            },
        );
    }
    let identities = descriptions
        .iter()
        .map(|(alias, description)| (alias.clone(), description.identity))
        .collect::<BTreeMap<_, _>>();
    let broker =
        native_broker::BrokerService::new(stores, config.native.broker_config(&config.reapi))
            .and_then(|broker| broker.with_controls(controls))
            .and_then(|broker| broker.with_queries(queries))
            .and_then(|broker| broker.with_provider_descriptions(descriptions))
            .map_err(|status| {
                anyhow::anyhow!("cannot configure native broker: {}", status.message())
            })?;
    let broker = if let Some(commands) = command_host {
        broker
            .with_command_resolver(commands)
            .map_err(|_| anyhow::anyhow!("cannot configure native command admission"))?
    } else {
        broker
    };

    for (alias, settings) in &config.native.repositories {
        retained.insert(
            alias.clone(),
            crate::native_workspaces::Repository {
                io: broker
                    .source_io(alias)
                    .map_err(|_| anyhow::anyhow!("cannot configure native source provider"))?,
                scope: scopes.remove(alias).expect("configured provider scope"),
                remote: settings.remote.is_some(),
                identity: identities[alias],
            },
        );
    }
    Ok((Some(broker), retained))
}

pub async fn serve(mut config: Config) -> Result<()> {
    config.validate()?;
    let git_cas_endpoint = config
        .git_proxy
        .sync_to_cas
        .then(|| git_cas_endpoint(&config.cache.upstream))
        .transpose()?;
    private_dir(&config.state_dir)?;
    config.state_dir = config.state_dir.canonicalize()?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(config.state_dir.join("daemon.lock"))?;
    lock.try_lock_exclusive()
        .context("another workspace daemon owns this state directory")?;
    let startup_limits = crate::runtime::StartupLimits::apply(config.runtime.target_open_files);
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
    let control_accept = reapi::server_io::AcceptState::default();
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
        max_concurrent_reads: config.cache.max_concurrent_reads,
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
    let native_config = config.clone();
    let native_cache = cache.clone();
    let native_ca = credentials.native_ca.clone();
    let native_credentials = credentials.native.clone();
    let native_commands = if config.native.repositories.is_empty() {
        None
    } else {
        Some(Arc::new(
            crate::native_commands::NativeCommandHost::open(
                &config.state_dir.join("native-commands"),
            )
            .await?,
        ))
    };
    let startup_commands = native_commands.clone();
    let (native, native_repositories) = tokio::task::spawn_blocking(move || {
        open_native_host(
            &native_config,
            &native_cache,
            &native_ca,
            &native_credentials,
            startup_commands,
        )
    })
    .await
    .context("native repository startup worker failed")??;

    if let Some(broker) = &native {
        // This wrapper carries pre-decode admission and independent body
        // deadlines; never embed the raw generated ObjectIo service instead.
        rbe_router = rbe_router
            .add_service(broker.rpc_service())
            .add_service(broker.control_rpc_service())
            .add_service(broker.query_rpc_service());
    }

    let mut native_workspaces = if native_repositories.is_empty() {
        None
    } else {
        Some(
            crate::native_workspaces::Manager::open(
                config.clone(),
                cache.clone(),
                native_repositories,
            )?
            .with_commands(
                native_commands
                    .clone()
                    .expect("configured native command host"),
            )?,
        )
    };
    if let Some(manager) = &native_workspaces {
        rbe_router = rbe_router.add_service(manager.rpc());
    }

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
        let cas_sync = if let Some(endpoint) = git_cas_endpoint {
            Some(git_proxy::CasSyncConfig {
                endpoint,
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
    let rbe_accept = proxy.accept_state();
    let mut proxy_task = tokio::spawn(async move {
        rbe_router
            .serve_with_incoming_shutdown(
                reapi::server_io::unix_incoming(cache_listener, rbe_accept),
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
    if let Some(manager) = &mut native_workspaces {
        manager.restore_ready().await;
    }
    let state = State {
        config,
        startup_limits,
        control_accept: control_accept.clone(),
        identity,
        cache,
        proxy: proxy.clone(),
        bes,
        native,
        native_workspaces,
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
                result = reapi::server_io::accept_unix(&listener, &control_accept) => result.context("accepting control connection")?,
                _ = tokio::signal::ctrl_c() => {
                    if !state_slot.as_ref().unwrap().mounts.is_empty() || state_slot.as_ref().unwrap().native_workspaces.as_ref().is_some_and(|manager| manager.active()){eprintln!("stop active sessions before daemon shutdown");continue;}
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
                Ok(Request::NativeMount { root, store, commit, workspace_id }) => {
                    match state_slot.as_mut().unwrap().native_workspaces.as_mut() {
                        Some(manager) => manager.create(root, store, commit, workspace_id).await.and_then(|value| serde_json::to_value(value).map_err(Into::into)),
                        None => Err(anyhow::anyhow!("native repositories are not configured")),
                    }
                }
                Ok(Request::NativeUnmount { workspace_id }) => {
                    match state_slot.as_mut().unwrap().native_workspaces.as_mut() {
                        Some(manager) => manager.stop(&workspace_id).await.map(|()| json!(true)),
                        None => Err(anyhow::anyhow!("native repositories are not configured")),
                    }
                }
                Ok(Request::NativeRecover { workspace_id }) => {
                    let mut state = state_slot.take().expect("owned daemon state");
                    let (returned, result) = tokio::task::spawn_blocking(move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            match state.native_workspaces.as_mut() {
                                Some(manager) => manager.recover(&workspace_id),
                                None => Err(anyhow::anyhow!("native repositories are not configured")),
                            }
                        })).unwrap_or_else(|_| Err(anyhow::anyhow!(
                            "native recovery worker panicked; owned providers retained")));
                        (state, result)
                    }).await.context("native recovery worker panicked")?;
                    state_slot = Some(returned);
                    result
                }
                Ok(Request::NativeMounted { workspace_id }) => {
                    match state_slot.as_ref().unwrap().native_workspaces.as_ref() {
                        Some(manager) => manager.mounted(&workspace_id).map(|value| json!(value)),
                        None => Ok(json!(false)),
                    }
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
                    if !state_slot.as_ref().unwrap().mounts.is_empty() || state_slot.as_ref().unwrap().native_workspaces.as_ref().is_some_and(|manager| manager.active()) {
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
    // Native provider ownership must be settled before any later fallible
    // cleanup can return from serve() and drop the daemon state.
    cleanup_native_workspaces(&mut state, &listener, &token).await;
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
    if !proxy_completed {
        if state.native.is_some() {
            // Native I/O owns authoritative data. Stop accepting first, then
            // drain HTTP bodies and provider jobs without a forced grace cap.
            (&mut proxy_task)
                .await
                .context("unified transport task failed")??;
        } else if tokio::time::timeout(Duration::from_secs(10), &mut proxy_task)
            .await
            .is_err()
        {
            proxy_task.abort();
            let _ = proxy_task.await;
        }
    }
    if let Some(native) = &state.native {
        native
            .drain()
            .await
            .context("native broker did not drain")?;
    }
    if let Some(shutdown) = git_cas_shutdown {
        shutdown.shutdown().await;
    }
    if crate::recovery::identity(&state.config.state_dir.join("outputs"))?.is_none() {
        let _ = std::fs::remove_file(state.config.state_dir.join("bos-mount.json"));
    }
    state.proxy.wait_for_reads_idle().await?;
    #[cfg(target_os = "linux")]
    {
        let source = state.source_cas.clone();
        tokio::task::spawn_blocking(move || {
            let result = source.shutdown();
            drop(source);
            result
        })
        .await
        .context("source cache shutdown worker failed")??;
    }
    state.cache.wait_for_idle().await?;
    state.cache.flush_recency()?;
    let _ = std::fs::remove_file(socket);
    let _ = std::fs::remove_file(cache_socket);
    drop(state);
    drop(proxy);
    drop(lock);
    run
}

/// An unexpected transport failure is not permission to drop a busy native
/// provider. Keep the owner lock, cache, and same-user control listener alive
/// until shutdown succeeds. No new work is accepted in this recovery-only loop.
async fn cleanup_native_workspaces(state: &mut State, listener: &UnixListener, token: &str) {
    if state.native_workspaces.is_none() {
        return;
    }
    let _ = state.native_workspaces.as_mut().unwrap().stop_all().await;
    let mut retry = tokio::time::interval(Duration::from_secs(1));
    while state.native_workspaces.as_ref().unwrap().active() {
        let stream = tokio::select! {
            accepted = reapi::server_io::accept_unix(listener, &state.control_accept) => match accepted {
                Ok(stream) => stream,
                Err(_) => { retry.tick().await; let _ = state.native_workspaces.as_mut().unwrap().stop_all().await; continue; }
            },
            _ = retry.tick() => { let _ = state.native_workspaces.as_mut().unwrap().stop_all().await; continue; },
            _ = tokio::signal::ctrl_c() => { eprintln!("native providers remain owned until recovery completes"); continue; },
        };
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut bytes = Vec::new();
        use tokio::io::AsyncReadExt;
        let request = match tokio::time::timeout(
            Duration::from_secs(10),
            (&mut reader).take(65537).read_until(b'\n', &mut bytes),
        )
        .await
        {
            Ok(Ok(_)) if bytes.len() <= 65536 => {
                serde_json::from_slice::<Request>(&bytes).map_err(anyhow::Error::from)
            }
            _ => Err(anyhow::anyhow!("invalid or oversized control request")),
        };
        let request = match request {
            Ok(Request::Scoped {
                version,
                digest,
                request,
            }) => state.identity.validate(version, &digest).map(|()| *request),
            other => other,
        };
        let result = match request {
            Ok(Request::NativeUnmount { workspace_id }) => state
                .native_workspaces
                .as_mut()
                .unwrap()
                .stop(&workspace_id)
                .await
                .map(|()| json!(true)),
            Ok(Request::NativeRecover { workspace_id }) => state
                .native_workspaces
                .as_mut()
                .unwrap()
                .recover(&workspace_id),
            Ok(Request::NativeMounted { workspace_id }) => state
                .native_workspaces
                .as_ref()
                .unwrap()
                .mounted(&workspace_id)
                .map(|mounted| json!(mounted)),
            Ok(Request::Status) => {
                let mut value = state.status();
                value["recovery_only"] = json!(true);
                Ok(value)
            }
            Ok(Request::Shutdown) => Err(anyhow::anyhow!(
                "native providers remain owned; stop them before shutdown"
            )),
            Ok(_) => Err(anyhow::anyhow!(
                "daemon is recovery-only; stop retained native providers"
            )),
            Err(error) => Err(error),
        };
        let response = match result {
            Ok(result) => Response {
                ok: true,
                result,
                error: None,
            },
            Err(error) => Response {
                ok: false,
                result: serde_json::Value::Null,
                error: Some(error.to_string().replace(token, "[redacted]")),
            },
        };
        if let Ok(mut bytes) = serde_json::to_vec(&response) {
            if bytes.len() <= 1024 * 1024 {
                bytes.push(b'\n');
                let _ = write.write_all(&bytes).await;
            }
        }
    }
    // Global RPC producers have stopped. Retain ownership through the final
    // stream/job barrier even if the transport exited unexpectedly.
    if let Some(manager) = &state.native_workspaces {
        while manager.drain().await.is_err() {
            retry.tick().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnusedNativeCacheSource;
    #[async_trait::async_trait]
    impl local_cas::BlobSource for UnusedNativeCacheSource {
        async fn fetch(&self, _: &local_cas::Artifact, _: &mut File) -> Result<()> {
            anyhow::bail!("native startup fixture must not fetch remote bytes")
        }
        async fn missing(&self, _: &[local_cas::Artifact]) -> Result<Vec<local_cas::Artifact>> {
            anyhow::bail!("native startup fixture must not query remote objects")
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_recovery_reuses_legacy_readonly_daemon_lock_without_repair() -> Result<()> {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let temporary = tempfile::tempdir()?;
        let directory = private_fs::PrivateDir::create(temporary.path().join("state"))?;
        let path = directory.path().join("daemon.lock");
        std::fs::write(&path, b"legacy lock")?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
        let first = existing_daemon_owner_lock(&directory)?;
        first.try_lock_exclusive()?;
        let second = existing_daemon_owner_lock(&directory)?;
        assert!(second.try_lock_exclusive().is_err());
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o644
        );
        assert_eq!(std::fs::read(&path)?, b"legacy lock");
        drop(first);
        second.try_lock_exclusive()?;
        drop(second);
        std::fs::rename(&path, directory.path().join("saved.lock"))?;
        symlink("saved.lock", &path)?;
        assert!(existing_daemon_owner_lock(&directory).is_err());
        std::fs::remove_file(&path)?;
        std::fs::hard_link(directory.path().join("saved.lock"), &path)?;
        assert!(existing_daemon_owner_lock(&directory).is_err());
        std::fs::remove_file(&path)?;
        assert!(existing_daemon_owner_lock(&directory).is_err());
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn native_store_initialization_reuses_one_owner_and_refuses_partial_or_unrelated_state()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let cache = LocalCas::open(
            directory.path().join("cas"),
            Arc::new(UnusedNativeCacheSource),
            CacheConfig::default(),
        )?;
        let mut config = Config::default();
        config.state_dir = directory.path().join("state");
        private_dir(&config.state_dir)?;
        assert!(open_native_repositories(&config, &cache, &BTreeMap::new())?.is_none());
        assert!(!config.state_dir.join("native").exists());
        config
            .native
            .repositories
            .insert("example".into(), Default::default());
        let broker = open_native_repositories(&config, &cache, &BTreeMap::new())?.unwrap();
        assert!(
            open_native_repositories(&config, &cache, &BTreeMap::new()).is_err(),
            "a second provider must not steal the live owner lock"
        );
        let identity = broker.describe_provider("example")?;
        assert_eq!(
            identity.index_mode,
            native_broker::ProviderIndexMode::Legacy
        );
        drop(broker);
        config.native.workspaces.index_mode = crate::config::NativeIndexMode::Federated;
        let reopened = open_native_repositories(&config, &cache, &BTreeMap::new())?.unwrap();
        let changed_policy = reopened.describe_provider("example")?;
        assert_eq!(
            changed_policy.identity, identity.identity,
            "index rollout policy is not provider security identity"
        );
        assert_eq!(
            changed_policy.index_mode,
            native_broker::ProviderIndexMode::Federated
        );
        drop(reopened);

        let (broker, providers) =
            open_native_host(&config, &cache, &BTreeMap::new(), &BTreeMap::new(), None)?;
        assert_eq!(providers.len(), 1);
        let scope = providers["example"].scope.backend().to_owned();
        drop(broker);
        assert!(
            open_native_repositories(&config, &cache, &BTreeMap::new()).is_err(),
            "retained workspace providers must share, not reopen, the same authoritative owner"
        );
        drop(providers);
        let (broker, providers) =
            open_native_host(&config, &cache, &BTreeMap::new(), &BTreeMap::new(), None)?;
        assert_eq!(providers["example"].scope.backend(), scope);
        config.native.workspaces.registry_path = Some(directory.path().join("registry"));
        let mut manager =
            crate::native_workspaces::Manager::open(config.clone(), cache.clone(), providers)?;
        assert!(!manager.active());
        assert!(
            !config
                .native
                .workspaces
                .registry_path
                .as_ref()
                .unwrap()
                .exists(),
            "broker-only startup must not initialize a per-user workspace registry"
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                assert!(
                    manager
                        .create(
                            directory.path().join("mount"),
                            "unconfigured".into(),
                            "0".repeat(64),
                            "1".repeat(64)
                        )
                        .await
                        .is_err()
                );
                assert!(
                    manager
                        .create(
                            directory.path().join("mount"),
                            "example".into(),
                            "0".repeat(64),
                            "0".repeat(64)
                        )
                        .await
                        .is_err()
                );
            });
        assert!(
            !config
                .native
                .workspaces
                .registry_path
                .as_ref()
                .unwrap()
                .exists()
        );
        drop(manager);
        drop(broker);

        for partial in [false, true] {
            let root = directory
                .path()
                .join(if partial { "partial" } else { "unrelated" });
            config.state_dir = root.clone();
            let path = root.join("native/example");
            private_dir(&path)?;
            if partial {
                std::fs::create_dir(path.join("native-objects"))?;
                std::fs::create_dir(path.join("native-file-staging"))?;
            } else {
                std::fs::write(path.join("sentinel"), b"unrelated bytes")?;
            }
            assert!(open_native_repositories(&config, &cache, &BTreeMap::new()).is_err());
            if partial {
                assert!(
                    std::fs::read_dir(path.join("native-objects"))?
                        .next()
                        .is_none()
                );
                assert!(
                    std::fs::read_dir(path.join("native-file-staging"))?
                        .next()
                        .is_none()
                );
            } else {
                assert_eq!(std::fs::read(path.join("sentinel"))?, b"unrelated bytes");
                assert_eq!(std::fs::read_dir(path)?.count(), 1);
            }
        }
        Ok(())
    }

    #[test]
    fn git_cas_endpoint_translates_only_supported_scheme_aliases() {
        for (behavior, upstream, expected) in [
            (
                "plain gRPC",
                "grpc://example.invalid:8080",
                "http://example.invalid:8080",
            ),
            (
                "TLS gRPC",
                "grpcs://example.invalid:443/",
                "https://example.invalid:443/",
            ),
            (
                "HTTP unchanged",
                "http://grpc.example.invalid:8080/",
                "http://grpc.example.invalid:8080/",
            ),
            (
                "HTTPS unchanged",
                "https://grpcs.example.invalid/",
                "https://grpcs.example.invalid/",
            ),
        ] {
            assert_eq!(git_cas_endpoint(upstream).unwrap(), expected, "{behavior}");
        }
        for upstream in [
            "ftp://example.invalid",
            "unix:/tmp/fixture.sock",
            "example.invalid",
            "http://example.invalid/grpcs://path",
            "grpc://user@example.invalid",
            "grpcs://example.invalid?query",
        ] {
            assert!(git_cas_endpoint(upstream).is_err());
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_command_dirty_fence_survives_client_timeout_and_kernel_lease_until_drain()
    -> Result<()> {
        use native_vfs_rpc::CommandHost as _;
        let directory = tempfile::tempdir()?;
        let private = private_fs::PrivateDir::create(directory.path().join("private"))?;
        let admins = private_fs::PrivateDir::create(private.path().join("admins"))?;
        let root = private.path().join("projection");
        std::fs::create_dir(&root)?;
        let admin = admins.path().join("workspace");
        let binding = native_workspace_admin::Binding {
            version: 1,
            workspace_id: [7; 32],
            root,
            repo: admin.join("repo"),
            working_copy: admin.join("working_copy"),
            admin,
            repository: "example".into(),
            endpoint: native_workspace_admin::Endpoint::UnixSocket {
                path: private.path().join("rbe.sock"),
            },
            rpc_mount_id: "workspace".into(),
        };
        let path = private.path().join("retention");
        let host = crate::native_commands::NativeCommandHost::open(&path).await?;
        host.register(&binding, [8; 32]).await?;
        assert_eq!(
            host.workspace_state("workspace").await?.state,
            native_retention::WorkspaceState::Dirty
        );
        let preparation_a = host.prepare("workspace", [1; 32]).await?;
        assert_eq!(host.prepare("workspace", [1; 32]).await?, preparation_a);
        assert!(
            host.begin("workspace", [1; 32], preparation_a.clone())
                .await
                .is_err(),
            "no local writer lease is held"
        );
        let lock_path = native_vfs_rpc::command_lock_path(&binding.admin, "workspace")?;
        let lease = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lease.try_lock_exclusive()?;
        let started = host
            .begin("workspace", [1; 32], preparation_a.clone())
            .await?;
        assert_eq!(
            host.begin("workspace", [1; 32], preparation_a.clone())
                .await?
                .token,
            started.token
        );
        let dirty = host.workspace_state("workspace").await?;
        assert_eq!(
            (dirty.state, dirty.command_epoch),
            (native_retention::WorkspaceState::Dirty, 1)
        );
        // A local writer is admitted only after the durable Dirty ACK.
        std::fs::write(
            private.path().join("operation-written-after-dirty"),
            b"operation",
        )?;
        let physical = native_broker::CommandResolver::resolve(&host, "example", started.token)?;
        assert!(
            native_broker::CommandResolver::resolve(&host, "different", started.token).is_err()
        );
        let finishing = host.clone();
        let token = started.token;
        let mut waiter =
            tokio::spawn(async move { finishing.finish_dirty("workspace", token).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut waiter)
                .await
                .is_err()
        );
        waiter.abort();
        assert!(host.active("workspace"));
        drop(lease);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            host.active("workspace"),
            "an accepted physical job still holds the fence"
        );
        drop(physical);
        tokio::time::timeout(Duration::from_secs(5), async {
            while host.active("workspace") {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        host.finish_dirty("workspace", token).await?;
        assert_eq!(
            host.workspace_state("workspace").await?.state,
            native_retention::WorkspaceState::Dirty
        );
        assert_eq!(host.status()["collection_enabled"], false);
        // Old closed A and old never-accepted preparations cannot mint a ghost
        // epoch merely because a later CLI now holds the same kernel lock.
        let unaccepted_a = host.prepare("workspace", [2; 32]).await?;
        let preparation_b = host.prepare("workspace", [3; 32]).await?;
        let lease_b = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lease_b.try_lock_exclusive()?;
        assert!(
            host.begin("workspace", [1; 32], preparation_a)
                .await
                .is_err()
        );
        assert!(
            host.begin("workspace", [2; 32], unaccepted_a)
                .await
                .is_err()
        );
        assert_eq!(host.workspace_state("workspace").await?.command_epoch, 1);
        let started_b = host.begin("workspace", [3; 32], preparation_b).await?;
        assert_eq!(started_b.epoch, 2);
        drop(lease_b);
        host.finish_dirty("workspace", started_b.token).await?;
        let before_restart = host.prepare("workspace", [4; 32]).await?;
        let old_process_lease = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)?;
        old_process_lease.try_lock_exclusive()?;
        assert!(host.active("workspace"));

        assert!(
            host.register(&binding, [9; 32]).await.is_err(),
            "changed provider identity must not be adopted"
        );
        drop(host);
        let reopened = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match crate::native_commands::NativeCommandHost::open(&path).await {
                    Ok(host) => break host,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await?;
        reopened.register(&binding, [8; 32]).await?;
        assert!(
            reopened.active("workspace"),
            "a pre-restart CLI kernel lease must still block stop/unmount"
        );
        drop(old_process_lease);
        assert!(!reopened.active("workspace"));
        let persisted = reopened.workspace_state("workspace").await?;
        assert_eq!(
            (persisted.state, persisted.command_epoch),
            (native_retention::WorkspaceState::Dirty, 2)
        );
        let after_restart = reopened.prepare("workspace", [5; 32]).await?;
        let lease_d = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)?;
        lease_d.try_lock_exclusive()?;
        assert!(
            reopened
                .begin("workspace", [4; 32], before_restart)
                .await
                .is_err()
        );
        assert_eq!(
            reopened.workspace_state("workspace").await?.command_epoch,
            2
        );
        let started_d = reopened.begin("workspace", [5; 32], after_restart).await?;
        assert_eq!(started_d.epoch, 3);
        drop(lease_d);
        reopened.finish_dirty("workspace", started_d.token).await?;
        assert!(native_broker::CommandResolver::resolve(&reopened, "example", token).is_err());
        Ok(())
    }
}
